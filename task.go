package scheduler

import (
	"context"
	"time"
)

// Resources 表示任务运行期间需要独占占用的多维资源额度。
type Resources struct {
	Slots int
	Named map[string]int
}

// TaskFunc 是任务执行体。返回 error 表示失败（可能触发重试）。
type TaskFunc func(ctx context.Context) error

// TaskSpec 描述一个任务及其依赖、资源与重试策略。
type TaskSpec struct {
	ID          string
	Deps        []string
	Resources   Resources
	Timeout     time.Duration // 0 表示不限时
	MaxAttempts int           // <=0 视为 1
	Backoff     time.Duration // 首次重试退避基数，0 表示不退避
	Run         TaskFunc
}

// TaskStatus 是单个任务的快照状态。
type TaskStatus struct {
	ID       string
	Attempts int
	State    TerminalState
	Err      string
}
