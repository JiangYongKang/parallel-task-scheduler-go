package scheduler

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestHighConcurrencyDeterministic 在竞态检测下验证：
// 大量任务、复杂依赖下结果确定，且 Run 返回后无协程泄漏。
func TestHighConcurrencyDeterministic(t *testing.T) {
	before := runtime.NumGoroutine()

	const layers = 5
	const width = 20
	var executed atomic.Int32
	s, _ := New(Config{
		Workers:  8,
		Capacity: map[string]int64{"io": 4},
		Retry:    RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond},
		Logf:     testLogger(t),
	})
	// 分层全连接 DAG：第 i 层每个任务依赖第 i-1 层全部任务。
	prev := []string{}
	for l := 0; l < layers; l++ {
		var cur []string
		for w := 0; w < width; w++ {
			id := fmt.Sprintf("L%d-W%d", l, w)
			deps := append([]string(nil), prev...)
			s.Submit(&Task{
				ID:        id,
				DependsOn: deps,
				Resources: map[string]int64{"io": 1},
				Fn: func(context.Context) error {
					executed.Add(1)
					return nil
				},
			})
			cur = append(cur, id)
		}
		prev = cur
	}
	t.Logf("input: %d layers x %d width fully-connected DAG, workers=8, io=4", layers, width)

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := executed.Load(); got != layers*width {
		t.Fatalf("executed=%d, want %d", got, layers*width)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
		if rec.Attempts != 1 {
			t.Fatalf("task %s attempts=%d, want 1", id, rec.Attempts)
		}
	}
	t.Logf("verdict: all %d tasks succeeded exactly once", layers*width)

	// 协程泄漏检查：Run 返回后工人协程必须全部回收。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutine leak: before=%d after=%d", before, n)
	}
}

// TestConcurrentSubmitAndCancel 并发提交与取消交叉，验证无死锁。
func TestConcurrentSubmitAndCancel(t *testing.T) {
	for round := 0; round < 10; round++ {
		s, _ := New(Config{Workers: 4})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			for i := 0; i < 50; i++ {
				s.Submit(&Task{ID: fmt.Sprintf("t%d", i), Fn: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Millisecond):
						return nil
					}
				}})
			}
			close(done)
		}()
		<-done
		cancel() // 提交完成后立即取消
		out, err := s.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for id, rec := range out {
			if !rec.Status.Terminal() {
				t.Fatalf("round %d: task %s in non-terminal %s", round, id, rec.Status)
			}
		}
	}
	t.Logf("verdict: 10 rounds of submit+cancel completed, no deadlock, all terminal")
}
