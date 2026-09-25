// Package scheduler 实现本地多工人并发任务调度与中断恢复引擎。
//
// 核心能力：
//
//   - 带依赖关系的 DAG 调度与有界并行。任务只有在所有依赖都成功后才就绪；
//     依赖成环、缺失、重复声明，或资源需求超过池容量，都会在 Submit 阶段
//     返回可区分原因的 *ValidationError。
//   - 按 Slots 与命名资源进行准入与背压。额度不足时任务在 Acquire 上阻塞等待，
//     既不被丢弃也不产生无界队列；取消/超时会唤醒等待者并释放已占资源。
//   - 有限次数 + 线性退避的失败重试；取消与超时不重试；成功后绝不重复执行。
//   - 基于 Store 的进度持久化与进程重启恢复：已完成任务不重复执行，
//     在途（attempts）统计跨进程累计；FileStore 使用临时文件 + rename 原子写入。
//
// 终态彼此可区分：succeeded / failed / canceled / timed_out / skipped。
// 调度过程通过 Config.Logger 打印"调度输入"与"判定依据"日志。
package scheduler
