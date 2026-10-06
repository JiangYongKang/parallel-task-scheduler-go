package scheduler

// GroupQuota 描述一个业务分组的额度：并发数与命名资源占用上限。
// 分组额度只在全局上限（Config.Workers / Config.Capacity）之内收紧：
// 全局约束始终是最外层硬约束，分组额度大于全局上限时自然被全局截断。
type GroupQuota struct {
	// Workers 为该分组同时运行的任务数上限，<=0 表示不限制（仅受全局约束）。
	Workers int
	// Resources 为该分组可占用的命名资源上限，未列出的资源不受分组限制。
	Resources map[string]int64
}

// groupLimiter 跟踪各分组在途占用，判定任务是否可被准入。
// 仅由调度循环 goroutine 访问（准入与释放都在调度循环内完成），无需加锁。
type groupLimiter struct {
	quotas   map[string]GroupQuota
	inflight map[string]int
	used     map[string]map[string]int64
}

func newGroupLimiter(quotas map[string]GroupQuota) *groupLimiter {
	g := &groupLimiter{
		quotas:   make(map[string]GroupQuota, len(quotas)),
		inflight: make(map[string]int, len(quotas)),
		used:     make(map[string]map[string]int64, len(quotas)),
	}
	for name, q := range quotas {
		g.quotas[name] = q
	}
	return g
}

// limited 报告分组是否配置了额度；未配置的分组只受全局约束。
func (g *groupLimiter) limited(group string) (GroupQuota, bool) {
	if group == "" {
		return GroupQuota{}, false
	}
	q, ok := g.quotas[group]
	return q, ok
}

// admit 报告该分组当前是否还能准入一个占用 need 资源的任务。
func (g *groupLimiter) admit(group string, need map[string]int64) bool {
	q, ok := g.limited(group)
	if !ok {
		return true
	}
	if q.Workers > 0 && g.inflight[group] >= q.Workers {
		return false
	}
	for name, n := range need {
		limit, ok := q.Resources[name]
		if !ok {
			continue
		}
		if g.used[group][name]+n > limit {
			return false
		}
	}
	return true
}

// acquire 记录一次准入。
func (g *groupLimiter) acquire(group string, need map[string]int64) {
	if _, ok := g.limited(group); !ok {
		return
	}
	g.inflight[group]++
	if g.used[group] == nil {
		g.used[group] = make(map[string]int64, len(need))
	}
	for name, n := range need {
		g.used[group][name] += n
	}
}

// release 回收一次准入占用。
func (g *groupLimiter) release(group string, need map[string]int64) {
	if _, ok := g.limited(group); !ok {
		return
	}
	g.inflight[group]--
	for name, n := range need {
		g.used[group][name] -= n
		if g.used[group][name] <= 0 {
			delete(g.used[group], name)
		}
	}
}
