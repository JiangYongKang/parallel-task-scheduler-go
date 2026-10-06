package scheduler

import "fmt"

// groupLimiter 跟踪各业务分组的在途并发与命名资源占用。
// 仅由调度循环 goroutine 访问，无需加锁。
// 全局并发/资源上限由 resourceManager 兜底（最外层硬约束），
// 分组额度只能在其范围内收紧；分组额度用满时该组任务继续排队，
// 不丢弃、也不阻塞其他分组。
type groupLimiter struct {
	quotas   map[string]GroupQuota
	inflight map[string]int
	resUsed  map[string]map[string]int64
}

func newGroupLimiter(quotas map[string]GroupQuota) *groupLimiter {
	g := &groupLimiter{
		quotas:   make(map[string]GroupQuota, len(quotas)),
		inflight: make(map[string]int, len(quotas)),
		resUsed:  make(map[string]map[string]int64, len(quotas)),
	}
	for name, q := range quotas {
		cp := GroupQuota{Workers: q.Workers, Capacity: make(map[string]int64, len(q.Capacity))}
		for k, v := range q.Capacity {
			cp.Capacity[k] = v
		}
		g.quotas[name] = cp
		g.resUsed[name] = make(map[string]int64, len(q.Capacity))
	}
	return g
}

// canAdmit 报告该任务当前是否满足其分组额度（不检查全局约束）。
// 未配置额度的分组（含空分组名）仅受全局约束，这里恒为 true。
func (g *groupLimiter) canAdmit(t *Task) bool {
	q, ok := g.quotas[t.Group]
	if !ok {
		return true
	}
	if q.Workers > 0 && g.inflight[t.Group] >= q.Workers {
		return false
	}
	for name, need := range t.Resources {
		if need <= 0 {
			continue
		}
		limit, ok := q.Capacity[name]
		if !ok {
			continue // 该资源分组级不设限
		}
		if g.resUsed[t.Group][name]+need > limit {
			return false
		}
	}
	return true
}

// admit 登记一次分组占用。
func (g *groupLimiter) admit(t *Task) {
	if _, ok := g.quotas[t.Group]; !ok {
		return
	}
	g.inflight[t.Group]++
	for name, need := range t.Resources {
		if need <= 0 {
			continue
		}
		if _, ok := g.quotas[t.Group].Capacity[name]; ok {
			g.resUsed[t.Group][name] += need
		}
	}
}

// release 归还一次分组占用。
func (g *groupLimiter) release(t *Task) {
	if _, ok := g.quotas[t.Group]; !ok {
		return
	}
	g.inflight[t.Group]--
	for name, need := range t.Resources {
		if need <= 0 {
			continue
		}
		if _, ok := g.quotas[t.Group].Capacity[name]; ok {
			g.resUsed[t.Group][name] -= need
		}
	}
}

// validate 校验任务资源声明的合法性，拒绝永远不可能被准入的任务，
// 避免其无限等待拖住整批任务。三类原因可互相区分、也与依赖类错误区分：
//   - ErrUnknownResource：引用了系统未配置的资源；
//   - ErrResourceExceedsCapacity：申请量超过全局总容量（或超过其分组额度上限）；
//   - ErrInvalidResourceRequest：申请量为负数。
func (g *groupLimiter) validate(t *Task, globalCap map[string]int64) error {
	for name, need := range t.Resources {
		if need < 0 {
			return fmt.Errorf("%w: task %q resource %q need %d < 0",
				ErrInvalidResourceRequest, t.ID, name, need)
		}
		if need == 0 {
			continue
		}
		total, ok := globalCap[name]
		if !ok {
			return fmt.Errorf("%w: task %q resource %q not configured",
				ErrUnknownResource, t.ID, name)
		}
		if need > total {
			return fmt.Errorf("%w: task %q resource %q need %d > total %d",
				ErrResourceExceedsCapacity, t.ID, name, need, total)
		}
		if q, ok := g.quotas[t.Group]; ok {
			if limit, ok := q.Capacity[name]; ok && need > limit {
				return fmt.Errorf("%w: task %q resource %q need %d > group %q limit %d",
					ErrResourceExceedsCapacity, t.ID, name, need, t.Group, limit)
			}
		}
	}
	return nil
}
