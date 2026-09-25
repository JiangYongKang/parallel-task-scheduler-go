package scheduler

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLog(t *testing.T) Logger {
	return log.New(os.Stdout, "  [test] ", log.LstdFlags|log.Lmicroseconds)
}

func waitFor(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting: %s", msg)
	}
}

// 依赖失败：后继任务必须是 skipped 而非静默成功/完成。
func TestDependencyFailurePropagates(t *testing.T) {
	boom := errors.New("boom")
	s := New(Config{MaxConcurrency: 4, Logger: testLog(t)})
	var childRan int32
	err := s.Submit([]TaskSpec{
		{ID: "a", Run: func(context.Context) error { return boom }},
		{ID: "b", Deps: []string{"a"}, Run: func(context.Context) error {
			atomic.AddInt32(&childRan, 1)
			return nil
		}},
		{ID: "c", Deps: []string{"b"}, Run: func(context.Context) error {
			atomic.AddInt32(&childRan, 1)
			return nil
		}},
		{ID: "indep", Run: func(context.Context) error { return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("input: a fails; b->a, c->b must be skipped; indep must succeed")
	res, st, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res["a"].State != StateFailed {
		t.Fatalf("a = %s", res["a"].State)
	}
	if res["b"].State != StateSkipped || res["c"].State != StateSkipped {
		t.Fatalf("b=%s c=%s, want both skipped", res["b"].State, res["c"].State)
	}
	if atomic.LoadInt32(&childRan) != 0 {
		t.Fatalf("downstream tasks ran %d times, want 0 (no silent completion)", childRan)
	}
	if res["indep"].State != StateSucceeded {
		t.Fatalf("indep = %s, want succeeded", res["indep"].State)
	}
	if st.Failed != 1 || st.Skipped != 2 || st.Succeeded != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if res["a"].Err == nil {
		t.Fatal("failed task should carry an explanatory error")
	}
	_ = boom
	t.Logf("decision: stats=%+v", st)
}

// 有界并行：并发额度受 MaxConcurrency 约束；资源不足时等待而非丢弃。
func TestBoundedConcurrencyAndBackpressure(t *testing.T) {
	const workers = 2
	const tasksN = 6
	var inFlight, maxFlight int32
	var mu sync.Mutex
	s := New(Config{MaxConcurrency: workers, Logger: testLog(t)})
	release := make(chan struct{})
	started := make(chan struct{}, tasksN)
	specs := make([]TaskSpec, tasksN)
	for i := 0; i < tasksN; i++ {
		specs[i] = TaskSpec{
			ID: idFor(i),
			Run: func(ctx context.Context) error {
				cur := atomic.AddInt32(&inFlight, 1)
				mu.Lock()
				if cur > maxFlight {
					maxFlight = cur
				}
				mu.Unlock()
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					atomic.AddInt32(&inFlight, -1)
					return ctx.Err()
				}
				atomic.AddInt32(&inFlight, -1)
				return nil
			},
		}
	}
	if err := s.Submit(specs); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: %d independent tasks, %d workers; expect max in-flight = %d", tasksN, workers, workers)
	done := make(chan struct{})
	var results map[string]Result
	var st Stats
	go func() {
		results, st, _ = s.Run(context.Background())
		close(done)
	}()
	waitFor(t, started, 2*time.Second, "first two tasks to start")
	waitFor(t, started, 2*time.Second, "second task to start")
	select {
	case <-started:
		t.Fatal("third task started while slots saturated: no backpressure")
	case <-time.After(50 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&maxFlight); got != workers {
		t.Fatalf("max in-flight = %d, want %d", got, workers)
	}
	close(release) // 释放资源，剩余任务应排队准入、全部完成，无丢弃。
	waitFor(t, done, 2*time.Second, "run to finish")
	if st.Succeeded != tasksN {
		t.Fatalf("stats=%+v results=%v", st, states(results))
	}
	t.Logf("decision: maxFlight=%d stats=%+v", maxFlight, st)
}

// 命名资源准入：占 db=2 的 big 运行期间，db=1 的小任务必须阻塞等待（背压）。
// 通过 prep 门控任务确定性地保证 big 先完成准入，再解锁小任务的依赖。
func TestNamedResourceAdmission(t *testing.T) {
	s := New(Config{MaxConcurrency: 4, NamedResources: map[string]int{"db": 2}, Logger: testLog(t)})
	bigStarted := make(chan struct{})
	bigRelease := make(chan struct{})
	smallDone := make(chan struct{}, 2)
	specs := []TaskSpec{
		{ID: "big", Resources: Resources{Named: map[string]int{"db": 2}}, Run: func(ctx context.Context) error {
			close(bigStarted) // big 已获得 db=2
			<-bigRelease
			return nil
		}},
		{ID: "prep", Run: func(ctx context.Context) error {
			// 等 big 完成准入后才放行其下游的小任务。
			<-bigStarted
			return nil
		}},
		{ID: "s1", Deps: []string{"prep"}, Resources: Resources{Named: map[string]int{"db": 1}}, Run: func(ctx context.Context) error {
			select {
			case smallDone <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}},
		{ID: "s2", Deps: []string{"prep"}, Resources: Resources{Named: map[string]int{"db": 1}}, Run: func(ctx context.Context) error {
			select {
			case smallDone <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}},
	}
	if err := s.Submit(specs); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _, _ = s.Run(context.Background()); close(done) }()
	waitFor(t, bigStarted, 2*time.Second, "big task (db=2) to acquire")
	// prep 已完成、s1/s2 已就绪，但 db 被 big 占满：必须等待而非执行。
	select {
	case <-smallDone:
		t.Fatal("small task ran while db fully occupied by big task")
	case <-time.After(80 * time.Millisecond):
	}
	close(bigRelease) // big 释放 db，两个小任务准入并完成。
	waitFor(t, smallDone, 2*time.Second, "s1")
	waitFor(t, smallDone, 2*time.Second, "s2")
	waitFor(t, done, 2*time.Second, "finish")
	t.Log("decision: named resource backpressure observed, then drained")
}

func idFor(i int) string {
	return "t" + string(rune('a'+i))
}

func states(m map[string]Result) map[string]TerminalState {
	out := map[string]TerminalState{}
	for k, v := range m {
		out[k] = v.State
	}
	return out
}
