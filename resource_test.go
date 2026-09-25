package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolTryAcquireAndRelease(t *testing.T) {
	p := NewResourcePool(2, map[string]int{"db": 1})
	if !p.TryAcquire(Resources{Slots: 1, Named: map[string]int{"db": 1}}) {
		t.Fatal("first acquire should succeed")
	}
	if p.TryAcquire(Resources{Slots: 1, Named: map[string]int{"db": 1}}) {
		t.Fatal("second acquire of db must fail (capacity 1)")
	}
	if !p.TryAcquire(Resources{Slots: 1}) {
		t.Fatal("slots-only acquire should succeed (slot 2 free)")
	}
	if p.TryAcquire(Resources{Slots: 1}) {
		t.Fatal("slots exhausted, must fail")
	}
	p.Release(Resources{Slots: 1, Named: map[string]int{"db": 1}})
	if !p.TryAcquire(Resources{Slots: 1, Named: map[string]int{"db": 1}}) {
		t.Fatal("acquire after release should succeed")
	}
	if s, n := p.Available(); s != 0 || n["db"] != 0 {
		t.Fatalf("available slots=%d db=%d want 0/0", s, n["db"])
	}
	t.Log("decision: TryAcquire/Release accounting correct for slots and named resources")
}

// Acquire 在额度不足时阻塞，释放后被唤醒；期间不丢弃请求。
func TestPoolBlockingAcquireWakesOnRelease(t *testing.T) {
	p := NewResourcePool(1, nil)
	if !p.TryAcquire(Resources{Slots: 1}) {
		t.Fatal("seed acquire")
	}
	acquired := make(chan struct{})
	go func() {
		if err := p.Acquire(context.Background(), Resources{Slots: 1}); err != nil {
			t.Errorf("acquire: %v", err)
			return
		}
		close(acquired)
	}()
	select {
	case <-acquired:
		t.Fatal("acquire returned while pool saturated")
	case <-time.After(50 * time.Millisecond):
	}
	p.Release(Resources{Slots: 1})
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked acquire was not woken on release (possible deadlock)")
	}
	t.Log("decision: blocked Acquire woke up after Release")
}

// Acquire 等待期间 ctx 取消必须立即返回错误且不占资源。
func TestPoolAcquireCanceledWhileBlocked(t *testing.T) {
	p := NewResourcePool(1, nil)
	if !p.TryAcquire(Resources{Slots: 1}) {
		t.Fatal("seed acquire")
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Acquire(ctx, Resources{Slots: 1}) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("err=%v want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled Acquire did not return (watcher wake missing)")
	}
	if s, _ := p.Available(); s != 0 {
		t.Fatalf("canceled acquire consumed a slot: free=%d", s)
	}
	p.Release(Resources{Slots: 1})
	if s, _ := p.Available(); s != 1 {
		t.Fatalf("slot accounting broken: free=%d", s)
	}
	t.Log("decision: canceled waiter exited without consuming resources")
}

// 高并发准入总量守恒：任意时刻占用不超过容量，全部完成后全额归还。
func TestPoolConcurrentAccounting(t *testing.T) {
	p := NewResourcePool(3, map[string]int{"db": 2})
	var inFlight, maxFlight int32
	var done int32
	const n = 200
	stop := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			r := Resources{Slots: 1, Named: map[string]int{"db": 1}}
			if err := p.Acquire(context.Background(), r); err != nil {
				return
			}
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&maxFlight)
				if cur <= old || atomic.CompareAndSwapInt32(&maxFlight, old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&inFlight, -1)
			p.Release(r)
			if atomic.AddInt32(&done, 1) == n {
				close(stop)
			}
		}()
	}
	select {
	case <-stop:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: not all acquires completed")
	}
	if maxFlight > 2 { // db=2 是更紧的约束
		t.Fatalf("max in-flight=%d exceeds db capacity 2", maxFlight)
	}
	if s, named := p.Available(); s != 3 || named["db"] != 2 {
		t.Fatalf("resources not fully returned: slots=%d db=%d", s, named["db"])
	}
	t.Logf("decision: %d concurrent acquires honored bounds; pool fully restored", n)
}
