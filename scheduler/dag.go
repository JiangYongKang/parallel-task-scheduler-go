package scheduler

import "fmt"

// dag 在提交前对任务集合做静态校验：
// 重复任务声明、重复依赖、自依赖、缺失依赖、依赖成环。
type dag struct {
	tasks map[string]*Task
	deps  map[string][]string
}

func newDAG() *dag {
	return &dag{tasks: make(map[string]*Task), deps: make(map[string][]string)}
}

// add 注册一个任务并做逐条校验。
func (d *dag) add(t *Task) error {
	if t.ID == "" {
		return fmt.Errorf("%w: empty task id", ErrDuplicateTask)
	}
	if _, ok := d.tasks[t.ID]; ok {
		return fmt.Errorf("%w: %q", ErrDuplicateTask, t.ID)
	}
	seen := make(map[string]struct{}, len(t.DependsOn))
	for _, dep := range t.DependsOn {
		if dep == t.ID {
			return fmt.Errorf("%w: %q", ErrSelfDependency, t.ID)
		}
		if _, ok := seen[dep]; ok {
			return fmt.Errorf("%w: %q -> %q", ErrDuplicateDependency, t.ID, dep)
		}
		seen[dep] = struct{}{}
	}
	d.tasks[t.ID] = t
	d.deps[t.ID] = append([]string(nil), t.DependsOn...)
	return nil
}

// validate 校验缺失依赖与成环，返回可区分的错误。
func (d *dag) validate() error {
	for id, deps := range d.deps {
		for _, dep := range deps {
			if _, ok := d.tasks[dep]; !ok {
				return fmt.Errorf("%w: %q depends on unknown %q", ErrMissingDependency, id, dep)
			}
		}
	}
	// 三色标记 DFS 检测环。
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈中
		black = 2 // 已完成
	)
	color := make(map[string]int, len(d.tasks))
	var visit func(id string) error
	visit = func(id string) error {
		color[id] = gray
		for _, dep := range d.deps[id] {
			switch color[dep] {
			case gray:
				return fmt.Errorf("%w: %q -> %q", ErrCyclicDependency, id, dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for id := range d.tasks {
		if color[id] == white {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}
