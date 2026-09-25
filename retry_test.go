package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 瞬时失败按 MaxAttempts 有限重试，达到上限后 failed；attempts 计数准确。
func TestRetryExhaustsAttempts(t *testing.T) {
	var calls int32
	s := New(Config{MaxConcurrency: 1, Logger: testLog(t)})
	err := s.Submit([]TaskSpec{
		{
			ID: "flaky", MaxAttempts: 3, Backoff: time.Millisecond,
			Run: func(ctx context.Context) error {
				n := atomic.AddInt32(&calls, 1)
				t.Logf("input: flaky invocation #%d (all fail)", n)
				return errors.New("transient failure")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, st, _ := s.Run(context.Background())
	if calls != 3 {
		t.Fatalf("invocations=%d, want 3", calls)
	}
	if res["flaky"].State != StateFailed {
		t.Fatalf("state=%s want failed", res["flaky"].State)
	}
	if res["flaky"].Attempts != 3 {
		t.Fatalf("attempts=%d want 3", res["flaky"].Attempts)
	}
	if st.TotalAttempts != 3 {
		t.Fatalf("stats.TotalAttempts=%d want 3", st.TotalAttempts)
	}
	t.Logf("decision: failed after bounded attempts; stats=%+v", st)
}

// 中途成功后不再重试：成功不产生重复副作用。
func TestRetrySucceedsAndNoDoubleSideEffect(t *testing.T) {
	var calls int32
	var sideEffectCommits int32
	s := New(Config{MaxConcurrency: 1, Logger: testLog(t)})
	err := s.Submit([]TaskSpec{
		{
			ID: "flaky", MaxAttempts: 5, Backoff: time.Millisecond,
			Run: func(ctx context.Context) error {
				n := atomic.AddInt32(&calls, 1)
				if n < 3 {
					t.Logf("input: invocation #%d fails (transient)", n)
					return errors.New("transient")
				}
				// 仅第 3 次调用提交副作用。
				atomic.AddInt32(&sideEffectCommits, 1)
				t.Logf("input: invocation #%d commits side effect and succeeds", n)
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, st, _ := s.Run(context.Background())
	if calls != 3 {
		t.Fatalf("invocations=%d want exactly 3 (no retry after success)", calls)
	}
	if got := atomic.LoadInt32(&sideEffectCommits); got != 1 {
		t.Fatalf("side effect committed %d times, want exactly 1", got)
	}
	if res["flaky"].State != StateSucceeded || res["flaky"].Attempts != 3 {
		t.Fatalf("result=%+v", res["flaky"])
	}
	if st.Succeeded != 1 {
		t.Fatalf("stats=%+v", st)
	}
	t.Logf("decision: succeeded on attempt 3; side effects=%d (idempotent single commit)", sideEffectCommits)
}

// 退避期间取消：不再发起后续尝试，终态 canceled，attempts 停在已执行次数。
func TestRetryBackoffCanceled(t *testing.T) {
	var calls int32
	s := New(Config{MaxConcurrency: 1, Logger: testLog(t)})
	err := s.Submit([]TaskSpec{
		{
			ID: "flaky", MaxAttempts: 100, Backoff: 5 * time.Second,
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&calls, 1)
				return errors.New("transient")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res map[string]Result
	go func() { res, _, _ = s.Run(ctx); close(done) }()
	time.Sleep(80 * time.Millisecond) // 等第 1 次失败进入退避
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not stop promptly after cancel during backoff")
	}
	if calls != 1 {
		t.Fatalf("invocations=%d want 1 (cancel during first backoff)", calls)
	}
	if res["flaky"].State != StateCanceled {
		t.Fatalf("state=%s want canceled", res["flaky"].State)
	}
	t.Logf("decision: canceled in backoff after %d attempt(s)", calls)
}

// MaxAttempts=1 的失败任务不重试，直接 failed。
func TestNoRetryByDefault(t *testing.T) {
	var calls int32
	s := New(Config{MaxConcurrency: 1})
	_ = s.Submit([]TaskSpec{{ID: "x", Run: func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("nope")
	}}})
	res, _, _ := s.Run(context.Background())
	if calls != 1 || res["x"].State != StateFailed {
		t.Fatalf("calls=%d state=%s", calls, res["x"].State)
	}
}
