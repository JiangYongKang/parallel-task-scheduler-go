package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDependencyOrderAndSkipOnFailure(t *testing.T) {
	s, _ := New(Config{Workers: 4, Logf: testLogger(t)})
	var mu sync.Mutex
	var order []string
	rec := func(id string, fail bool) TaskFunc {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			if fail {
				return errors.New("boom")
			}
			return nil
		}
	}
	t.Logf("input: a -> b(fail) -> c, d depends on c; expect c,d skipped not silently done")
	s.Submit(&Task{ID: "a", Fn: rec("a", false)})
	s.Submit(&Task{ID: "b", DependsOn: []string{"a"}, Fn: rec("b", true)})
	s.Submit(&Task{ID: "c", DependsOn: []string{"b"}, Fn: rec("c", false)})
	s.Submit(&Task{ID: "d", DependsOn: []string{"c"}, Fn: rec("d", false)})

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: a=%s b=%s c=%s d=%s", out["a"].Status, out["b"].Status, out["c"].Status, out["d"].Status)
	if out["a"].Status != StatusSucceeded || out["b"].Status != StatusFailed {
		t.Fatalf("unexpected a/b status: %v %v", out["a"].Status, out["b"].Status)
	}
	// 依赖失败不得使后继被静默标记为完成：必须可区分为 Skipped。
	if out["c"].Status != StatusSkipped || out["d"].Status != StatusSkipped {
		t.Fatalf("c/d must be Skipped, got %v %v", out["c"].Status, out["d"].Status)
	}
	for _, id := range order {
		if id == "c" || id == "d" {
			t.Fatalf("skipped task %q must never execute", id)
		}
	}
}

func TestRetryLimitAndBackoff(t *testing.T) {
	s, _ := New(Config{
		Workers: 1,
		Retry:   RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond},
		Logf:    testLogger(t),
	})
	var calls atomic.Int32
	s.Submit(&Task{ID: "flaky", Fn: func(context.Context) error {
		calls.Add(1)
		return errors.New("transient")
	}})
	t.Logf("input: always-failing task, MaxAttempts=3")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: status=%s attempts=%d calls=%d", out["flaky"].Status, out["flaky"].Attempts, calls.Load())
	if out["flaky"].Status != StatusFailed {
		t.Fatalf("want failed, got %s", out["flaky"].Status)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("retry limit violated: got %d calls, want 3", got)
	}
	if out["flaky"].Attempts != 3 {
		t.Fatalf("attempts=%d, want 3", out["flaky"].Attempts)
	}
}

func TestRetrySucceedsBeforeLimit(t *testing.T) {
	s, _ := New(Config{
		Workers: 2,
		Retry:   RetryPolicy{MaxAttempts: 5, BaseBackoff: time.Millisecond},
		Logf:    testLogger(t),
	})
	var calls atomic.Int32
	s.Submit(&Task{ID: "flaky", Fn: func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}})
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: status=%s attempts=%d", out["flaky"].Status, out["flaky"].Attempts)
	if out["flaky"].Status != StatusSucceeded || out["flaky"].Attempts != 3 {
		t.Fatalf("want succeeded at attempt 3, got %s/%d", out["flaky"].Status, out["flaky"].Attempts)
	}
}

func TestCancelPropagatesAndReleases(t *testing.T) {
	s, _ := New(Config{Workers: 1, Logf: testLogger(t)})
	started := make(chan struct{})
	s.Submit(&Task{ID: "long", Fn: func(ctx context.Context) error {
		close(started)
		<-ctx.Done() // 取消必须传播到任务体
		return ctx.Err()
	}})
	s.Submit(&Task{ID: "queued", DependsOn: []string{"long"}, Fn: noop})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()
	t.Logf("input: workers=1, long blocks until cancel, queued waits behind it")
	out, err := s.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: long=%s queued=%s", out["long"].Status, out["queued"].Status)
	if out["long"].Status != StatusCanceled {
		t.Fatalf("want canceled, got %s", out["long"].Status)
	}
	// 依赖被取消 -> 后继 Skipped（可区分，非静默完成）。
	if out["queued"].Status != StatusSkipped {
		t.Fatalf("want skipped, got %s", out["queued"].Status)
	}
}

func TestTimeoutDistinctFromCancel(t *testing.T) {
	s, _ := New(Config{Workers: 1, TaskTimeout: 20 * time.Millisecond, Logf: testLogger(t)})
	s.Submit(&Task{ID: "slow", Fn: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	t.Logf("input: task blocks forever, TaskTimeout=20ms")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: slow=%s", out["slow"].Status)
	if out["slow"].Status != StatusTimedOut {
		t.Fatalf("want timed_out, got %s", out["slow"].Status)
	}
}

func TestResourceBackpressure(t *testing.T) {
	s, _ := New(Config{
		Workers:  4,
		Capacity: map[string]int64{"gpu": 1},
		Logf:     testLogger(t),
	})
	var gpuCur, gpuMax atomic.Int32
	job := func(context.Context) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	gpuJob := func(ctx context.Context) error {
		c := gpuCur.Add(1)
		for {
			m := gpuMax.Load()
			if c <= m || gpuMax.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		gpuCur.Add(-1)
		return job(ctx)
	}
	t.Logf("input: 3 gpu tasks + 2 cpu-only tasks, gpu capacity=1, workers=4")
	for _, id := range []string{"g1", "g2", "g3"} {
		s.Submit(&Task{ID: id, Resources: map[string]int64{"gpu": 1}, Fn: gpuJob})
	}
	s.Submit(&Task{ID: "c1", Fn: job})
	s.Submit(&Task{ID: "c2", Fn: job})
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s not succeeded: %s", id, rec.Status)
		}
	}
	t.Logf("verdict: max concurrent gpu-holders=%d (capacity=1)", gpuMax.Load())
	if gpuMax.Load() > 1 {
		t.Fatalf("gpu capacity violated: max concurrent holders=%d", gpuMax.Load())
	}
}
