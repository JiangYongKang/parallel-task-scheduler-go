package scheduler

import (
	"errors"
	"fmt"
)

// 哨兵错误，便于 errors.Is 判定。
var (
	ErrDuplicateTask     = errors.New("duplicate task declaration")
	ErrMissingDependency = errors.New("task depends on an undeclared task")
	ErrCyclicDependency  = errors.New("cyclic dependency detected")
	ErrInvalidSpec       = errors.New("invalid task spec")
	ErrAlreadySubmitted  = errors.New("scheduler already submitted")
	ErrSchedulerClosed   = errors.New("scheduler closed")
	ErrResourceExceeded  = errors.New("task requests more resources than the pool limit")
)

// ValidationErrorKind 标识校验失败类别，便于调用方区分原因。
type ValidationErrorKind string

const (
	KindDuplicate     ValidationErrorKind = "duplicate_task"
	KindMissingDep    ValidationErrorKind = "missing_dependency"
	KindCycle         ValidationErrorKind = "cyclic_dependency"
	KindInvalidSpec   ValidationErrorKind = "invalid_spec"
	KindResourceLimit ValidationErrorKind = "resource_exceeds_limit"
)

// ValidationError 描述一次 DAG 校验失败的具体位置与原因。
type ValidationError struct {
	Kind   ValidationErrorKind
	TaskID string
	Detail string
}

func (e *ValidationError) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("%s: task=%q: %s", e.Kind, e.TaskID, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Detail)
}

// Is 支持按 Kind 归类，同时让 errors.Is 匹配对应哨兵错误。
func (e *ValidationError) Is(target error) bool {
	switch target {
	case ErrDuplicateTask:
		return e.Kind == KindDuplicate
	case ErrMissingDependency:
		return e.Kind == KindMissingDep
	case ErrCyclicDependency:
		return e.Kind == KindCycle
	case ErrInvalidSpec:
		return e.Kind == KindInvalidSpec
	case ErrResourceExceeded:
		return e.Kind == KindResourceLimit
	}
	return false
}

// TerminalState 表示任务的终态种类，彼此可区分。
type TerminalState string

const (
	StateSucceeded TerminalState = "succeeded"
	StateFailed    TerminalState = "failed"
	StateCanceled  TerminalState = "canceled"
	StateTimedOut  TerminalState = "timed_out"
	StateSkipped   TerminalState = "skipped" // 依赖失败/取消而未执行
)

// IsTerminal 报告状态是否为终态。
func (s TerminalState) IsTerminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCanceled, StateTimedOut, StateSkipped:
		return true
	}
	return false
}
