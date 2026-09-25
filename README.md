# 本地多工人并发任务调度与中断恢复引擎（Go）

一个零第三方依赖、可嵌入应用进程的本地任务编排库。它在固定工人池内按依赖
顺序准入任务，按多维资源实施准入与背压，并把任务终态与在途统计持久化，
使**任务依赖、资源竞争与进程重启下的执行结果始终一致且可解释**。

Import path: `github.com/highcumontoa/parallel-task-scheduler-go`

---

## 1. 架构总览

| 组件 | 文件 | 职责 |
| --- | --- | ---|
| `Config / Scheduler` | `scheduler.go` | DAG 判定、工人池循环、就绪队列、终态传播、重试、恢复、统计 |
| `TaskSpec / Resources` | `task.go` | 任务输入模型（依赖、资源、超时、重试参数） |
| `DAG` | `dag.go` | 提交期校验（重复/缺失/成环/资源超限）与确定性排序 |
| `ResourcePool` | `resource.go` | Slots 与命名资源的阻塞式准入、释放与取消唤醒 |
| `Store / MemoryStore / FileStore` | `store.go` | 进度与在途统计的持久化接口、内存实现、原子文件实现 |
| 错误模型 | `errors.go` | 可区分的校验类别与五种可区分终态 |

运行期数据流：

```
Submit(specs)
   │  BuildDAG 校验 + Store.Load 恢复
   ▼
就绪队列（先判定初始就绪，再启动工人，避免启动竞态）
   │  worker 取队首 → ResourcePool.Acquire（不足则阻塞/背压）
   ▼
执行体（可取消 ctx，可选 Timeout）
   │  失败 → 线性退避 → 有限重试；成功/失败/取消/超时 → 落盘终态
   ▼
终态传播：成功解锁下游；非成功把整个下游子图标记为 skipped
```

---

## 2. 调度模型

### 2.1 依赖与就绪

- 每个任务用 `TaskSpec.Deps` 声明**前置依赖**。
- **任务只能在所有依赖都处于 `succeeded` 终态后才进入就绪队列**。
- 就绪任务按任务 ID 字典序进入 FIFO 队列，工人按序取队首；
  这保证在任意给定调度步上选谁执行是确定的，结果可复现。
- 非成功终态（failed/canceled/timed_out/skipped）**不会**让后继就绪；
  后继被级联标记为 `skipped` 并记录原因（如 `upstream X is failed`），
  因此**依赖失败不会使后继被静默标记为完成**，其执行体根本不会被调用。

### 2.2 提交期校验（可区分原因）

`Submit`（内部调用 `BuildDAG`）按固定顺序校验，并返回 `*ValidationError`，
其 `Kind` 与哨兵错误（`errors.Is`）一一对应：

| Kind | 哨兵 | 含义 |
| --- | --- | ---|
| `duplicate_task` | `ErrDuplicateTask` | 同一 ID 被重复声明 |
| `missing_dependency` | `ErrMissingDependency` | 依赖指向未声明的任务 |
| `cyclic_dependency` | `ErrCyclicDependency` | 依赖成环（含自环），`Detail` 给出环路径 |
| `invalid_spec` | `ErrInvalidSpec` | ID 为空、Run 为 nil、重复列同一条依赖边、负值等 |
| `resource_exceeds_limit` | `ErrResourceExceeded` | 任务需求超过 Slots 或某命名资源容量 |

可区分性：

```go
var ve *scheduler.ValidationError
if errors.As(err, &ve) {
    switch ve.Kind {
    case scheduler.KindCycle:        // …
    case scheduler.KindMissingDep:   // …
    }
}
```

### 2.3 五种可区分终态

| 终态 | 触发条件 | 是否执行过 |
| --- | --- | --- |
| `succeeded` | 执行体返回 nil 且 ctx 未结束 | 是 |
| `failed` | 普通错误重试至上限仍失败 | 是（≥1 次） |
| `canceled` | 父 ctx 被取消（运行中/退避中/等待资源中） | 不一定 |
| `timed_out` | 任务 `Timeout` 到期，或父 ctx  DeadlineExceeded | 不一定 |
| `skipped` | 上游非成功而未执行，或运行在其开始前被整体取消 | 否（attempts=0） |

> 判定规则：执行体返回值优先；返回 nil 时再判断 ctx 状态。引擎在主动
> `cancel()` 派生 ctx **之前**先捕获其状态，避免主动取消污染成功判定。

---

## 3. 资源模型、背压与取消语义

### 3.1 两维资源

- `Resources.Slots`：通用并发额度；默认池容量等于 `MaxConcurrency`，
  可用 `Config.TotalSlots` 覆盖（例如 8 个工人但只有 3 个外部连接额度）。
- `Resources.Named`：命名资源（如 `db`、`gpu`），容量由
  `Config.NamedResources` 声明；任务按需申请，**所有维度同时足额才准入**。
- 未声明容量的命名资源直接判 `resource_exceeds_limit`，杜绝"静默超额"。

### 3.2 背压（不丢弃、不无限堆积）

- 工人从就绪队列取出任务后调用 `ResourcePool.Acquire`；额度不足时
  **阻塞等待**，任务不会被丢弃，也不会为它建立独立的无界队列
  （等待者就是有限的就绪任务，数量受任务总数约束）。
- 任一任务 `Release` 时广播唤醒，等待者重新检查是否可准入。
- `Acquire` 快路径（立即可用）不创建任何辅助协程；只有真正阻塞时
  才启动一个 ctx 监视器，且在返回前 join，保证无协程泄漏。

### 3.3 取消与超时向下游传播、并释放资源

- 父 `ctx` 取消：唤醒就绪条件变量与资源等待者；运行中的任务通过其派生
  ctx 收到取消信号；退避中的 sleep 立即中断；等待资源者直接得到
  `canceled` 且**不占用**任何额度。
- 任务 `Timeout`：派生带 deadline 的 ctx，超时记 `timed_out`，
  与父取消（`canceled`）区分。
- 资源释放在执行体封装中以 `defer pool.Release(...)` 完成，
  无论成功、失败、取消、超时都会归还，随后唤醒背压中的任务。

---

## 4. 重试规则

- 由任务级 `MaxAttempts`（≤0 视为 1）与 `Backoff` 控制，**默认不重试**。
- 仅对"普通错误"重试；`context.Canceled` / `context.DeadlineExceeded`
  属于不可重试终态，立即落盘并传播。
- 退避为确定性线性策略：第 n 次失败后等待 `n * Backoff`；`Backoff=0`
  立即重试。退避期间响应取消，取消后不再发起后续尝试。
- **成功后立即终止，不再重试**；副作用提交应由任务体保证幂等，
  引擎保证对同一任务的执行体调用在成功后不再发生。
- 每次尝试开始即递增累计 `attempts` 并持久化在途统计。

---

## 5. 中断恢复规则

### 5.1 持久化内容

`Store` 接口持久化一个快照：每个任务的 `{ID, Attempts, State, ErrMsg}`
与全局 `StatsData{TotalAttempts}`。

- 只有 `State` 为终态的记录才被当作"已完成"。
- 只有 attempts、没有终态的记录表示"崩溃发生在执行中"，重启后**重新执行**
  该任务（其在途 attempt 计数仍保留在累计统计中，不丢失）。

### 5.2 重启行为（Submit 时 Load）

| 磁盘状态 | 重启后行为 |
| --- | --- |
| 某任务 `succeeded` | 不再执行，直接作为后继的已满足依赖（**副作用不重复**） |
| 某任务其他终态（failed/…） | 保持该终态，下游按规则 skipped |
| 某任务非终态/无记录 | 依赖满足后正常执行 |
| 全局 attempts 统计 | 以磁盘值为基线继续累计，**在途统计不丢失、不重复** |

### 5.3 FileStore 原子性

- 写入 = 序列化到同目录临时文件 → `fsync` → `os.Rename` 原子替换。
- 进程在任意时刻被杀，重启读到的要么是完整旧版、要么是完整新版，
  不会读到半写文件；目录中不残留 `.tmp`。
- `Store` 实现内部带互斥，并发保存下文件内容始终合法，恢复后状态自洽。

> 语义边界：本库保证"已**提交完成**的任务不重复、在途统计不丢"。
> 若进程在"副作用已发生但终态尚未落盘"的窗口内死亡，该任务会被重跑；
> 因此任务体应保证副作用**幂等**（如带去重键的写入）。

---

## 6. 快速开始

```go
s := scheduler.New(scheduler.Config{
    MaxConcurrency: 4,                       // 工人数量（Slots 默认上限）
    TotalSlots:     3,                       // 可选：更紧的槽位背压
    NamedResources: map[string]int{"db": 2}, // 命名资源容量
    Logger:         log.New(os.Stdout, "", log.LstdFlags),
})

err := s.Submit([]scheduler.TaskSpec{
    {ID: "a", Run: extract},
    {ID: "b", Deps: []string{"a"},
     Resources:   scheduler.Resources{Named: map[string]int{"db": 1}},
     Timeout:     2 * time.Second,
     MaxAttempts: 3, Backoff: 100 * time.Millisecond,
     Run: transform},
    {ID: "c", Deps: []string{"b"}, Run: load},
})
if err != nil { /* errors.As → *ValidationError，区分拒绝原因 */ }

results, stats, err := s.Run(ctx) // 阻塞至全部到达终态或 ctx 取消
```

文件持久化以支持重启：

```go
store, _ := scheduler.NewFileStore("./data/progress.json")
s := scheduler.New(scheduler.Config{MaxConcurrency: 4, Store: store})
// …Submit/Run…；下次进程启动用同一文件 New 一个 Scheduler 即可恢复。
```

---

## 7. 验证方法

所有测试位于包内，打印调度输入与判定依据（`t.Logf` 与可选 `Logger`），
覆盖题目要求的全部场景。

### 7.1 场景与测试对应

| 要求 | 测试 |
| --- | --- |
| 成环 / 缺失 / 重复 / 重复边 / 资源超限 | `TestValidationRejectsDistinctCauses`（`dag_test.go`） |
| 依赖失败不静默完成、独立任务不受连 | `TestDependencyFailurePropagates` |
| 有界并行与背压（不丢弃） | `TestBoundedConcurrencyAndBackpressure` |
| 命名资源准入与背压 | `TestNamedResourceAdmission`、`resource_test.go` |
| 取消传播 / 释放资源 / 资源等待中取消 | `TestCancelPropagatesToRunningTask`、`TestCancelReleasesResources`、`TestCancelWhileWaitingForResources` |
| 超时终态可区分 | `TestTaskTimeoutDistinct` |
| 重试上限 / 成功不重复副作用 / 退避中取消 | `TestRetryExhaustsAttempts`、`TestRetrySucceedsAndNoDoubleSideEffect`、`TestRetryBackoffCanceled` |
| 重启恢复（不重复、统计不丢、在途任务重跑） | `TestRecoverySkipsCompletedAndContinues`、`TestRecoveryRerunsInterruptedTask`、`TestFileStoreAtomicProgress` |
| 高并发结果确定 + 无死锁 + 无协程泄漏 | `TestHighConcurrencyDeterministic`、`TestHighConcurrencyWithFailuresDeterministic`、`TestCancellationStormNoLeak` |
| 并发存储自洽 | `TestFileStoreConcurrentSaves`、`TestPoolConcurrentAccounting` |

### 7.2 推荐命令

```bash
# 常规
go test ./...

# 竞态检测（必须通过）
go test -race ./...

# 高强度重复，验证确定可复现、无抖动失败
go test -race ./... -count=30
go test ./... -count=200

# 静态检查
go vet ./...
```

协程泄漏的判定：在 `Run` 返回后轮询 `runtime.NumGoroutine()`，要求在
1~2 秒内回落到运行前基线（容忍辅助协程的调度回收延迟）；取消风暴测试
额外用有界超时证明无死锁。

---

## 8. 设计取舍与确定性说明

- **单把调度互斥锁 + 条件变量**：所有阶段/队列/终态转移都在锁内判定，
  状态简单可证；执行体本身在锁外运行，不阻塞调度。
- **先入队、后开工**：初始就绪判定在持锁段完成后才启动工人，杜绝
  "工人在首个任务入队前误判结束"的启动竞态（该竞态曾被压测发现并修复）。
- **FIFO + ID 排序**：就绪与传播顺序确定，相同输入的终态集合完全一致；
  日志逐行给出 `ready/dispatch/attempt/terminal/retry` 的判定依据。
- **失败级联只标记未开始任务**：已在运行的任务自然结束并落盘，
  终态以执行结果为准，不会被并发改写。
