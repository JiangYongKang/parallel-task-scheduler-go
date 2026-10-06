package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config 配置调度器。
type Config struct {
	// Workers 为并发额度（同时运行的任务上限），<=0 时取 1。
	Workers int
	// Capacity 为命名资源总量，如 {"gpu": 1}。
	Capacity map[string]int64
	// Retry 为失败重试策略。
	Retry RetryPolicy
	// TaskTimeout 为单任务单次尝试的超时，0 表示不限制。
	TaskTimeout time.Duration
	// Groups 为各业务分组的额度（并发数与命名资源上限）。
	// 全局 Workers/Capacity 仍是最外层硬约束，分组额度只能在其内收紧。
	Groups map[string]GroupQuota
	// JournalPath 为进度日志路径，空串表示纯内存（不恢复）。
	JournalPath string
	// Logf 为可选判定日志，测试用它打印调度输入与判定依据。
	Logf func(format string, args ...any)
}

// Scheduler 是本地多工人并发任务调度与中断恢复引擎。
//
// 并发模型：单一调度循环（dispatch goroutine）独占状态迁移，
// 工人 goroutine 只执行用户函数并通过 channel 汇报结果，
// 因此状态迁移无数据竞争，可在 -race 下验证。
type Scheduler struct {
	cfg Config
	rm  *resourceManager
	gl  *groupLimiter
	j   *journal
	d   *dag

	mu     sync.Mutex
	closed bool
}

// taskState 是调度循环内部状态，仅由调度 goroutine 读写。
type taskState struct {
	t         *Task
	status    Status
	attempts  int
	remaining int  // 尚未成功终止的依赖数
	depFailed bool // 存在未成功终止的依赖
	enqueued  bool // 已进入就绪队列，防止重复入队
	admitted  bool // 已准入（占用分组额度），终态时需归还
}

// result 是工人汇报的一次任务终态。
type result struct {
	id       string
	status   Status
	attempts int
	err      error
}

// New 创建调度器。
func New(cfg Config) (*Scheduler, error) {
	j, err := openJournal(cfg.JournalPath)
	if err != nil {
		return nil, err
	}
	j.logf = cfg.Logf
	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	cfg.Workers = workers
	return &Scheduler{
		cfg: cfg,
		rm:  newResourceManager(workers, cfg.Capacity),
		gl:  newGroupLimiter(cfg.Groups),
		j:   j,
		d:   newDAG(),
	}, nil
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}

// Submit 注册任务。重复声明、自依赖、重复依赖在此被拒绝；
// 资源声明不合理（未知资源/超容量/非法申请量）也在此被拒绝，
// 避免永远不可准入的任务进入调度、无限等待拖住整批任务；
// 缺失依赖与成环在 Run 时统一校验（因为依赖可能后注册）。
func (s *Scheduler) Submit(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSchedulerClosed
	}
	if err := s.gl.validate(t, s.cfg.Capacity); err != nil {
		s.logf("submit rejected: task=%q reason=%v", t.ID, err)
		return err
	}
	if err := s.d.add(t); err != nil {
		s.logf("submit rejected: task=%q reason=%v", t.ID, err)
		return err
	}
	s.logf("submit accepted: task=%q group=%q deps=%v resources=%v", t.ID, t.Group, t.DependsOn, t.Resources)
	return nil
}

// Run 校验依赖图并调度执行，直到所有任务到达终态或 ctx 取消。
// 返回各任务的最终记录（含尝试次数），可用于核对恢复统计。
func (s *Scheduler) Run(ctx context.Context) (map[string]Record, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSchedulerClosed
	}
	s.closed = true
	s.mu.Unlock()
	defer s.j.close()

	if err := s.d.validate(); err != nil {
		s.logf("dag rejected: reason=%v", err)
		return nil, err
	}

	// 恢复：加载上次运行持久化的进度。
	recovered, err := s.j.load()
	if err != nil {
		return nil, fmt.Errorf("load journal: %w", err)
	}

	states := make(map[string]*taskState, len(s.d.tasks))
	dependents := make(map[string][]string)
	for id, t := range s.d.tasks {
		states[id] = &taskState{t: t, status: StatusPending, remaining: len(t.DependsOn)}
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], id)
		}
	}

	resultsCh := make(chan result, s.cfg.Workers)
	runCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	total := len(states)
	done := 0
	inflight := 0
	var ready []string

	// finish 迁移一个任务到终态并向下游传播。
	var finish func(id string, status Status, attempts int)
	finish = func(id string, status Status, attempts int) {
		st := states[id]
		st.status = status
		st.attempts = attempts
		done++
		if st.admitted {
			st.admitted = false
			s.gl.release(st.t)
		}
		s.j.append(id, status, attempts)
		s.logf("terminal: task=%q status=%s attempts=%d done=%d/%d", id, status, attempts, done, total)
		for _, next := range dependents[id] {
			ns := states[next]
			if ns.status.Terminal() {
				continue
			}
			if status == StatusSucceeded {
				ns.remaining--
				if ns.remaining == 0 && !ns.depFailed && !ns.enqueued {
					ns.enqueued = true
					ready = append(ready, next)
				}
			} else {
				// 依赖未成功：后继标记 Skipped（可区分，绝不静默完成），并级联。
				ns.depFailed = true
				s.logf("skip: task=%q reason=dependency %q ended %s", next, id, status)
				finish(next, StatusSkipped, ns.attempts)
			}
		}
	}

	admit := func(id string) {
		st := states[id]
		st.status = StatusReady
		st.admitted = true
		s.gl.admit(st.t)
		inflight++
		s.logf("admit: task=%q group=%q inflight=%d/%d recovered_attempts=%d", id, st.t.Group, inflight, s.cfg.Workers, st.attempts)
		s.j.append(id, StatusReady, st.attempts)
		go s.runTask(runCtx, st, resultsCh)
	}

	// 恢复回放：已终态任务不重复执行（避免重复副作用），
	// 通过 finish 走同一套传播逻辑，保证依赖计数与级联跳过自洽；
	// 在途（非终态）任务保留尝试计数，重新进入调度。
	for id, rec := range recovered {
		st, ok := states[id]
		if !ok {
			continue
		}
		if rec.Status.Terminal() {
			s.logf("recover: task=%q already terminal status=%s attempts=%d, skip execution", id, rec.Status, rec.Attempts)
			finish(id, rec.Status, rec.Attempts)
		} else {
			s.logf("recover: task=%q in-flight at crash attempts=%d, resume", id, rec.Attempts)
			st.attempts = rec.Attempts
		}
	}
	// 初始就绪集：无未决依赖且未被跳过、尚未入队的任务。
	// 按任务 ID 排序入队，保证调度顺序确定、结果可复现。
	var initial []string
	for id, st := range states {
		if !st.status.Terminal() && st.remaining == 0 && !st.depFailed && !st.enqueued {
			st.enqueued = true
			initial = append(initial, id)
		}
	}
	sort.Strings(initial)
	ready = append(ready, initial...)

	canceled := false
	for done < total {
		// 背压：仅当全局准入名额未满时才放行就绪任务，否则等待。
		// 分组公平：按入队顺序扫描，跳过额度已用满的分组（其任务继续排队，
		// 不丢弃、不阻塞其他分组），放行第一个满足分组额度的任务；
		// 任务集有限，被跳过的分组额度释放后必然被扫描到，不会饿死。
		for !canceled && inflight < s.cfg.Workers {
			idx := -1
			for i, id := range ready {
				if s.gl.canAdmit(states[id].t) {
					idx = i
					break
				}
			}
			if idx < 0 {
				break
			}
			id := ready[idx]
			ready = append(ready[:idx], ready[idx+1:]...)
			admit(id)
		}
		if done >= total {
			break
		}
		if canceled && inflight == 0 {
			// 取消后所有在途已回收，剩余未终态任务统一标记 Canceled。
			for id, st := range states {
				if !st.status.Terminal() {
					finish(id, StatusCanceled, st.attempts)
				}
			}
			break
		}
		select {
		case res := <-resultsCh:
			inflight--
			finish(res.id, res.status, res.attempts)
		case <-ctx.Done():
			if !canceled {
				canceled = true
				s.logf("cancel: propagate to %d in-flight tasks", inflight)
				cancelAll()
			}
		}
	}

	out := make(map[string]Record, total)
	for id, st := range states {
		out[id] = Record{ID: id, Status: st.status, Attempts: st.attempts, UpdatedAt: time.Now().UTC()}
	}
	return out, nil
}

// runTask 是工人：准入资源 -> 带超时执行 -> 失败按退避重试 -> 汇报终态。
func (s *Scheduler) runTask(ctx context.Context, st *taskState, resultsCh chan<- result) {
	id := st.t.ID
	if err := s.rm.acquire(ctx, st.t.Resources); err != nil {
		// 等待准入期间被取消：未占用任何资源，无需释放。
		resultsCh <- result{id: id, status: StatusCanceled, attempts: st.attempts, err: err}
		return
	}
	defer s.rm.release(st.t.Resources)

	attempts := st.attempts
	for {
		attempts++
		s.j.append(id, StatusRunning, attempts)
		s.logf("run: task=%q attempt=%d", id, attempts)

		attCtx := ctx
		cancel := context.CancelFunc(func() {})
		if s.cfg.TaskTimeout > 0 {
			attCtx, cancel = context.WithTimeout(ctx, s.cfg.TaskTimeout)
		}
		err := st.t.Fn(attCtx)
		cancel()

		if err == nil {
			resultsCh <- result{id: id, status: StatusSucceeded, attempts: attempts}
			return
		}
		// 区分取消与超时：父 ctx 结束为取消；仅尝试 ctx 超时为超时。
		if ctx.Err() != nil {
			resultsCh <- result{id: id, status: StatusCanceled, attempts: attempts, err: ctx.Err()}
			return
		}
		timedOut := errors.Is(err, context.DeadlineExceeded) ||
			(attCtx.Err() != nil && errors.Is(attCtx.Err(), context.DeadlineExceeded))
		if s.cfg.Retry.shouldRetry(attempts) {
			wait := s.cfg.Retry.backoff(attempts)
			s.logf("retry: task=%q attempt=%d failed err=%v backoff=%s", id, attempts, err, wait)
			select {
			case <-ctx.Done():
				resultsCh <- result{id: id, status: StatusCanceled, attempts: attempts, err: ctx.Err()}
				return
			case <-time.After(wait):
			}
			continue
		}
		status := StatusFailed
		if timedOut {
			status = StatusTimedOut
		}
		s.logf("exhausted: task=%q attempts=%d status=%s err=%v", id, attempts, status, err)
		resultsCh <- result{id: id, status: status, attempts: attempts, err: err}
		return
	}
}
