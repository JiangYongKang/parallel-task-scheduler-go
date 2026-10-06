package scheduler

import "fmt"

// validateResources 在提交时校验任务的资源声明，把两类永远不可满足的
// 声明明确挡在门外，避免它们进入调度后无限等待、拖住整批任务：
//   - 引用了系统未配置的命名资源（ErrUnknownResource）；
//   - 申请量超过配置总容量或所属分组额度（ErrResourceExceedsCapacity）。
//
// 校验与依赖类错误完全分开，可用 errors.Is 区分。
// 配置齐全、容量够用的任务不受影响。
func validateResources(cfg Config, t *Task) error {
	for name, n := range t.Resources {
		if n < 0 {
			return fmt.Errorf("%w: task %q resource %q amount %d", ErrInvalidResourceAmount, t.ID, name, n)
		}
		if n == 0 {
			continue // 零占用声明是 no-op，与准入行为一致，不误伤
		}
		cap_, ok := cfg.Capacity[name]
		if !ok {
			return fmt.Errorf("%w: task %q resource %q not configured", ErrUnknownResource, t.ID, name)
		}
		if n > cap_ {
			return fmt.Errorf("%w: task %q resource %q wants %d, capacity %d",
				ErrResourceExceedsCapacity, t.ID, name, n, cap_)
		}
		// 分组额度只收紧不放宽：若分组对该资源设了上限，申请超过它也永远不可满足。
		if t.Group != "" {
			if gq, ok := cfg.Groups[t.Group]; ok {
				if glimit, ok := gq.Resources[name]; ok && n > glimit {
					return fmt.Errorf("%w: task %q resource %q wants %d, group %q limit %d",
						ErrResourceExceedsCapacity, t.ID, name, n, t.Group, glimit)
				}
			}
		}
	}
	return nil
}
