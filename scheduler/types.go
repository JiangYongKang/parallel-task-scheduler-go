// Package scheduler 提供本地多工人并发任务调度与中断恢复引擎。
package scheduler

import (
	"context"
	"errors"
	"time"
)

// Status 表示任务的终态或中间状态，各终态之间必须可区分。
type Status string

const (
	StatusPending   Status = "pending"   // 已注册，等待依赖就绪
	StatusReady     Status = "ready"     // 依赖就绪，等待准入
	StatusRunning   Status = "running"   // 已准入，正在执行
	StatusSucceeded Status = "succeeded" // 成功终态
	StatusFailed    Status = "failed"    // 失败终态（重试耗尽或不可重试）
	StatusCanceled  Status = "canceled"  // 被取消终态
	StatusTimedOut  Status = "timed_out" // 超时终态
	StatusSkipped   Status = "skipped"   // 因依赖失败而被跳过（不会被静默标记为完成）
)

// Terminal 报告状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusTimedOut, StatusSkipped:
		return true
	}
	return false
}

// 可区分的依赖声明错误。
var (
	ErrCyclicDependency    = errors.New("scheduler: cyclic dependency")
	ErrMissingDependency   = errors.New("scheduler: missing dependency")
	ErrDuplicateTask       = errors.New("scheduler: duplicate task declaration")
	ErrDuplicateDependency = errors.New("scheduler: duplicate dependency declaration")
	ErrSelfDependency      = errors.New("scheduler: self dependency")
	ErrSchedulerClosed     = errors.New("scheduler: closed")
	ErrQueueFull           = errors.New("scheduler: admission queue full")
)

// TaskFunc 是任务体。ctx 取消或超时必须向下游传播。
type TaskFunc func(ctx context.Context) error

// Task 描述一个待调度任务。
type Task struct {
	ID        string
	DependsOn []string
	Resources map[string]int64 // 资源占用需求
	Fn        TaskFunc
}

// RetryPolicy 描述有限次数与退避策略。
type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Record 是持久化的任务进度记录。
type Record struct {
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	UpdatedAt time.Time `json:"updated_at"`
}
