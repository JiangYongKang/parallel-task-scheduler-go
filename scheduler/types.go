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

// 可区分的资源声明错误（与依赖类错误完全分开）。
var (
	// ErrUnknownResource 表示任务引用了系统里未配置的资源。
	ErrUnknownResource = errors.New("scheduler: unknown resource")
	// ErrResourceExceedsCapacity 表示单个任务申请量超过配置的总容量
	// （或超过其分组额度上限），该任务永远不可能被准入。
	ErrResourceExceedsCapacity = errors.New("scheduler: resource request exceeds capacity")
	// ErrInvalidResourceRequest 表示资源申请量非法（负数）。
	ErrInvalidResourceRequest = errors.New("scheduler: invalid resource request")
)

// TaskFunc 是任务体。ctx 取消或超时必须向下游传播。
type TaskFunc func(ctx context.Context) error

// Task 描述一个待调度任务。
type Task struct {
	ID        string
	DependsOn []string
	// Group 为业务分组名，空串表示不归属任何分组（仅受全局约束）。
	Group     string
	Resources map[string]int64 // 资源占用需求
	Fn        TaskFunc
}

// GroupQuota 为单个业务分组的额度，只能在全局上限范围内收紧。
// 零值字段表示该维度不设分组级限制（仅受全局约束）。
type GroupQuota struct {
	// Workers 为该分组同时运行的任务数上限，<=0 表示不限制。
	Workers int
	// Capacity 为该分组可占用的命名资源上限（须不大于全局容量才有意义）。
	Capacity map[string]int64
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
