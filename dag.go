package scheduler

import (
	"fmt"
	"sort"
)

// DAG 是经过校验的任务依赖图。
type DAG struct {
	tasks map[string]*TaskSpec
	order []string            // 按 ID 排序，保证调度确定性
	deps  map[string][]string // 去重并排序后的依赖
	child map[string][]string // 反向边：依赖完成后通知谁
}

// BuildDAG 校验任务集合并返回可调度的 DAG。
// 校验顺序固定：非法规格 -> 重复声明 -> 资源超限 -> 缺失依赖 -> 成环，
// 每类失败返回可区分的 *ValidationError（Kind 不同）。
func BuildDAG(specs []TaskSpec, limits Resources) (*DAG, error) {
	d := &DAG{
		tasks: map[string]*TaskSpec{},
		order: make([]string, 0, len(specs)),
		deps:  map[string][]string{},
		child: map[string][]string{},
	}

	// 1. 非法规格 + 2. 重复声明。
	seen := map[string]bool{}
	for i := range specs {
		t := &specs[i]
		if t.ID == "" {
			return nil, &ValidationError{Kind: KindInvalidSpec, TaskID: t.ID, Detail: "task id must not be empty"}
		}
		if t.Run == nil {
			return nil, &ValidationError{Kind: KindInvalidSpec, TaskID: t.ID, Detail: "task Run must not be nil"}
		}
		if t.MaxAttempts < 0 {
			return nil, &ValidationError{Kind: KindInvalidSpec, TaskID: t.ID, Detail: "MaxAttempts must not be negative"}
		}
		if t.Timeout < 0 || t.Backoff < 0 {
			return nil, &ValidationError{Kind: KindInvalidSpec, TaskID: t.ID, Detail: "Timeout/Backoff must not be negative"}
		}
		if err := validateResources(t.ID, t.Resources); err != nil {
			return nil, err
		}
		if seen[t.ID] {
			return nil, &ValidationError{Kind: KindDuplicate, TaskID: t.ID, Detail: "task declared more than once"}
		}
		seen[t.ID] = true
		d.tasks[t.ID] = t
		d.order = append(d.order, t.ID)
	}
	sort.Strings(d.order)

	// 3. 资源超限。
	if limits.Slots < 0 {
		return nil, &ValidationError{Kind: KindInvalidSpec, Detail: "resource limit Slots must not be negative"}
	}
	if err := validateResources("", limits); err != nil {
		return nil, err
	}
	for _, id := range d.order {
		t := d.tasks[id]
		if t.Resources.Slots > limits.Slots {
			return nil, &ValidationError{
				Kind: KindResourceLimit, TaskID: id,
				Detail: fmt.Sprintf("requests %d slots but pool has %d", t.Resources.Slots, limits.Slots),
			}
		}
		for name, need := range t.Resources.Named {
			if cap, ok := limits.Named[name]; !ok || need > cap {
				return nil, &ValidationError{
					Kind: KindResourceLimit, TaskID: id,
					Detail: fmt.Sprintf("requests %d of resource %q (pool capacity %d)", need, name, cap),
				}
			}
		}
	}

	// 4. 缺失依赖 + 依赖边去重（自环在第 5 步作为成环报告）。
	for _, id := range d.order {
		t := d.tasks[id]
		depSet := map[string]bool{}
		for _, dep := range t.Deps {
			if dep == "" {
				return nil, &ValidationError{Kind: KindInvalidSpec, TaskID: id, Detail: "dependency id must not be empty"}
			}
			if depSet[dep] {
				return nil, &ValidationError{
					Kind: KindInvalidSpec, TaskID: id,
					Detail: fmt.Sprintf("dependency %q listed more than once", dep),
				}
			}
			depSet[dep] = true
			if _, ok := d.tasks[dep]; !ok {
				return nil, &ValidationError{
					Kind: KindMissingDep, TaskID: id,
					Detail: fmt.Sprintf("depends on undeclared task %q", dep),
				}
			}
		}
		deps := make([]string, 0, len(t.Deps))
		for dep := range depSet {
			deps = append(deps, dep)
		}
		sort.Strings(deps)
		d.deps[id] = deps
	}

	// 5. 成环检测（DFS 三色标记，输出环上的一条路径）。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	var cycle []string
	var dfs func(id string) bool
	dfs = func(id string) bool {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range d.deps[id] {
			if color[dep] == white {
				if dfs(dep) {
					return true
				}
			} else if color[dep] == gray {
				// 从栈中 dep 首次出现位置到栈顶构成环。
				start := 0
				for i, n := range stack {
					if n == dep {
						start = i
						break
					}
				}
				cycle = append(append([]string(nil), stack[start:]...), dep)
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return false
	}
	for _, id := range d.order {
		if color[id] == white && dfs(id) {
			return nil, &ValidationError{
				Kind:   KindCycle,
				TaskID: cycle[0],
				Detail: fmt.Sprintf("cycle: %v", cycle),
			}
		}
	}

	// 构建反向边（按依赖 ID 排序后，子节点也按序追加，再统一排序保证确定性）。
	for _, id := range d.order {
		for _, dep := range d.deps[id] {
			d.child[dep] = append(d.child[dep], id)
		}
	}
	for dep := range d.child {
		sort.Strings(d.child[dep])
	}
	return d, nil
}

func validateResources(taskID string, r Resources) error {
	if r.Slots < 0 {
		return &ValidationError{Kind: KindInvalidSpec, TaskID: taskID, Detail: "resource Slots must not be negative"}
	}
	for name, n := range r.Named {
		if n < 0 {
			return &ValidationError{Kind: KindInvalidSpec, TaskID: taskID, Detail: fmt.Sprintf("named resource %q must not be negative", name)}
		}
	}
	return nil
}

// TaskCount 返回任务数量。
func (d *DAG) TaskCount() int { return len(d.order) }

// spec 返回任务规格（调度器内部使用）。
func (d *DAG) spec(id string) *TaskSpec { return d.tasks[id] }
