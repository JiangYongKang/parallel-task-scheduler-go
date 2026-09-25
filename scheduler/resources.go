package scheduler

import (
	"context"
	"sync"
	"time"
)

// backoffTick 是准入等待的重检节拍。
// 等待方在节拍到期或被释放通知唤醒后重新尝试准入，
// 既保证取消/超时及时传播，又避免忙等。
const backoffTick = 2 * time.Millisecond

// resourceManager 实现按资源占用的准入控制。
// 并发额度或命名资源不足时，Acquire 阻塞等待（背压），
// 直到资源被释放或 ctx 取消/超时。绝不丢弃任务；
// 等待者数量受调度器准入队列上限约束，不会无界堆积。
type resourceManager struct {
	mu        sync.Mutex
	slots     int // 总并发额度
	usedSlots int
	capacity  map[string]int64 // 各命名资源总量
	used      map[string]int64 // 各命名资源已占用
	notify    chan struct{}    // 释放通知（容量为1，聚合多次释放）
}

func newResourceManager(slots int, capacity map[string]int64) *resourceManager {
	if slots <= 0 {
		slots = 1
	}
	rm := &resourceManager{
		slots:    slots,
		capacity: make(map[string]int64, len(capacity)),
		used:     make(map[string]int64, len(capacity)),
		notify:   make(chan struct{}, 1),
	}
	for k, v := range capacity {
		rm.capacity[k] = v
	}
	return rm
}

// tryAcquire 非阻塞尝试占用资源，调用方须持有锁。
func (rm *resourceManager) tryAcquireLocked(need map[string]int64) bool {
	if rm.usedSlots >= rm.slots {
		return false
	}
	for name, n := range need {
		cap_, ok := rm.capacity[name]
		if !ok {
			// 未声明的资源视为容量 0，需求 >0 则永远不可满足。
			if n > 0 {
				return false
			}
			continue
		}
		if rm.used[name]+n > cap_ {
			return false
		}
	}
	rm.usedSlots++
	for name, n := range need {
		rm.used[name] += n
	}
	return true
}

// acquire 阻塞等待直到准入成功或 ctx 结束。
func (rm *resourceManager) acquire(ctx context.Context, need map[string]int64) error {
	for {
		rm.mu.Lock()
		if rm.tryAcquireLocked(need) {
			rm.mu.Unlock()
			return nil
		}
		rm.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rm.notify:
		case <-time.After(backoffTick):
		}
	}
}

// release 释放已占用资源并通知等待者。
func (rm *resourceManager) release(need map[string]int64) {
	rm.mu.Lock()
	rm.usedSlots--
	for name, n := range need {
		rm.used[name] -= n
		if rm.used[name] <= 0 {
			delete(rm.used, name)
		}
	}
	rm.mu.Unlock()
	select {
	case rm.notify <- struct{}{}:
	default:
	}
}
