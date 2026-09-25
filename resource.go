package scheduler

import (
	"context"
	"sync"
)

// ResourcePool 管理有界并发额度与命名资源，支持阻塞式准入与释放。
//
// 准入规则：任务只有在 Slots 与所有 Named 资源同时足额时才被准入；
// 不足时调用方在 Acquire 上阻塞等待（背压），不会被丢弃，也不会无界排队
// （等待者就是被调度器选出的就绪任务，数量受任务总数约束）。
type ResourcePool struct {
	mu    sync.Mutex
	cond  *sync.Cond
	slots int
	used  int
	named map[string]int // 总量
	inUse map[string]int // 已占用
}

// NewResourcePool 创建资源池；named 为各命名资源的总量。
func NewResourcePool(slots int, named map[string]int) *ResourcePool {
	p := &ResourcePool{
		slots: slots,
		named: map[string]int{},
		inUse: map[string]int{},
	}
	for k, v := range named {
		if v < 0 {
			v = 0
		}
		p.named[k] = v
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// free 报告当前空闲额度是否足以满足 r（调用方须持锁）。
func (p *ResourcePool) free(r Resources) bool {
	if p.used+r.Slots > p.slots {
		return false
	}
	for name, need := range r.Named {
		cap, ok := p.named[name]
		if !ok || p.inUse[name]+need > cap {
			return false
		}
	}
	return true
}

// TryAcquire 尝试非阻塞占用资源，成功返回 true。
func (p *ResourcePool) TryAcquire(r Resources) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.free(r) {
		return false
	}
	p.consume(r)
	return true
}

// Acquire 阻塞等待直到资源可用、ctx 取消/超时。
// 返回 context.Canceled / context.DeadlineExceeded 时不占用任何资源。
func (p *ResourcePool) Acquire(ctx context.Context, r Resources) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	// 快路径：额度立即可用时直接占用，不启动任何辅助协程。
	if p.free(r) {
		p.consume(r)
		p.mu.Unlock()
		return nil
	}

	// 慢路径：需要等待。ctx 结束时广播唤醒，使等待者重新检查并退出。
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		case <-stop:
		}
	}()
	defer func() {
		close(stop)
		p.mu.Unlock()
		<-watcherDone // 确保 watcher 退出，杜绝协程泄漏
	}()

	for !p.free(r) {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.cond.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	p.consume(r)
	return nil
}

func (p *ResourcePool) consume(r Resources) {
	p.used += r.Slots
	for name, need := range r.Named {
		p.inUse[name] += need
	}
}

// Release 归还资源并唤醒一个等待者（实际由其自行重新判断额度）。
func (p *ResourcePool) Release(r Resources) {
	p.mu.Lock()
	p.used -= r.Slots
	if p.used < 0 {
		p.used = 0
	}
	for name, need := range r.Named {
		p.inUse[name] -= need
		if p.inUse[name] < 0 {
			p.inUse[name] = 0
		}
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Available 返回当前空闲额度快照。
func (p *ResourcePool) Available() (slots int, named map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	named = map[string]int{}
	for k, cap := range p.named {
		named[k] = cap - p.inUse[k]
	}
	return p.slots - p.used, named
}
