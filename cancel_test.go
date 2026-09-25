package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// 父 ctx 取消：正在运行的任务收到取消、终态可区分，未启动者不静默完成。
func TestCancelPropagatesToRunningTask(t *testing.T) {
	s := New(Config{MaxConcurrency: 2, Logger: testLog(t)})
	started := make(chan struct{})
	var sawCancel int32
	err := s.Submit([]TaskSpec{
		{ID: "slow", Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			atomic.StoreInt32(&sawCancel, 1)
			return ctx.Err()
		}},
		{ID: "after", Deps: []string{"slow"}, Run: func(context.Context) error { return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res map[string]Result
	go func() {
		res, _, _ = s.Run(ctx)
		close(done)
	}()
	waitFor(t, started, 2*time.Second, "slow task to start")
	cancel()
	waitFor(t, done, 2*time.Second, "run to stop after cancel")
	if res["slow"].State != StateCanceled {
		t.Fatalf("slow = %s, want canceled", res["slow"].State)
	}
	if res["after"].State != StateSkipped {
		t.Fatalf("after = %s, want skipped (never silently completed)", res["after"].State)
	}
	if atomic.LoadInt32(&sawCancel) != 1 {
		t.Fatal("running task did not observe ctx cancellation")
	}
	t.Logf("decision: slow=%s after=%s", res["slow"].State, res["after"].State)
}

// 取消必须释放已占用资源，使被背压的任务能够准入（取消后等待者解除阻塞）。
func TestCancelReleasesResources(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, Logger: testLog(t)})
	holderStart := make(chan struct{})
	err := s.Submit([]TaskSpec{
		{ID: "holder", Resources: Resources{Slots: 1}, Run: func(ctx context.Context) error {
			close(holderStart)
			<-ctx.Done()
			return ctx.Err()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { s.Run(ctx) }()
	waitFor(t, holderStart, 2*time.Second, "holder to occupy the single slot")
	free, _ := s.pool.Available()
	if free != 0 {
		t.Fatalf("expected slot occupied, free=%d", free)
	}
	cancel()
	// 取消后槽位应被释放，新的资源池层面的准入立刻成功。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := s.pool.Available(); f == 1 {
			t.Log("decision: slot released after cancellation")
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("slot was not released after task cancellation")
}

// 任务自身超时：终态为 timed_out（区别于 canceled/failed），下游 skipped。
func TestTaskTimeoutDistinct(t *testing.T) {
	s := New(Config{MaxConcurrency: 2, Logger: testLog(t)})
	started := make(chan struct{})
	err := s.Submit([]TaskSpec{
		{ID: "slow", Timeout: 40 * time.Millisecond, Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}},
		{ID: "child", Deps: []string{"slow"}, Run: func(context.Context) error { return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var res map[string]Result
	var st Stats
	go func() { res, st, _ = s.Run(context.Background()); close(done) }()
	waitFor(t, started, 2*time.Second, "slow task start")
	<-done
	if res["slow"].State != StateTimedOut {
		t.Fatalf("slow = %s, want timed_out", res["slow"].State)
	}
	if res["child"].State != StateSkipped {
		t.Fatalf("child = %s, want skipped", res["child"].State)
	}
	if st.TimedOut != 1 || st.Skipped != 1 {
		t.Fatalf("stats=%+v", st)
	}
	t.Logf("decision: timed_out is distinct; stats=%+v", st)
}

// 在资源上等待的任务遇父取消：得到 canceled 终态且不占用资源。
// gate 先于 holder 就绪但会阻塞，保证 holder 先拿到唯一槽位后再放行 waiter。
func TestCancelWhileWaitingForResources(t *testing.T) {
	s := New(Config{MaxConcurrency: 2, TotalSlots: 1, Logger: testLog(t)})
	holderStart := make(chan struct{})
	gate := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err := s.Submit([]TaskSpec{
		{ID: "gate", Run: func(ctx context.Context) error {
			select {
			case <-gate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
		{ID: "holder", Resources: Resources{Slots: 1}, Run: func(ctx context.Context) error {
			close(holderStart)
			<-ctx.Done()
			return ctx.Err()
		}},
		{ID: "waiter", Deps: []string{"gate"}, Resources: Resources{Slots: 1}, Run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var res map[string]Result
	go func() { res, _, _ = s.Run(ctx); close(done) }()
	waitFor(t, holderStart, 2*time.Second, "holder takes slot")
	close(gate) // 此时 holder 独占唯一槽位；waiter 就绪但 Acquire 必然阻塞。
	// 给 waiter 时间走到 Acquire 等待，然后取消。
	time.Sleep(60 * time.Millisecond)
	cancel()
	waitFor(t, done, 2*time.Second, "run ends")
	if res["waiter"].State != StateCanceled {
		t.Fatalf("waiter = %s, want canceled", res["waiter"].State)
	}
	if f, _ := s.pool.Available(); f != 1 {
		t.Fatalf("slot leaked: free=%d want 1", f)
	}
	t.Logf("decision: waiter=%s while blocked on Acquire; slot intact", res["waiter"].State)
}
