package scheduler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 高并发 DAG：多层宽图 + 有限资源，结果确定可复现，执行计数精确。
func TestHighConcurrencyDeterministic(t *testing.T) {
	const layers = 6
	const width = 40 // 共 240 个任务
	s := New(Config{MaxConcurrency: 8, TotalSlots: 6,
		NamedResources: map[string]int{"db": 3}, Logger: testLog(t)})

	var specIDs []string
	var execCount int32
	var inFlight, maxFlight int32
	var fmu sync.Mutex
	specs := make([]TaskSpec, 0, layers*width)
	for l := 0; l < layers; l++ {
		for w := 0; w < width; w++ {
			id := fmt.Sprintf("L%d-W%d", l, w)
			specIDs = append(specIDs, id)
			var deps []string
			if l > 0 {
				// 每个任务依赖上一层两个任务，构成宽 DAG。
				deps = []string{
					fmt.Sprintf("L%d-W%d", l-1, w%width),
					fmt.Sprintf("L%d-W%d", l-1, (w+1)%width),
				}
			}
			res := Resources{Slots: 1, Named: map[string]int{}}
			if w%3 == 0 {
				res.Named["db"] = 1
			}
			specs = append(specs, TaskSpec{
				ID: id, Deps: deps, Resources: res,
				Run: func(ctx context.Context) error {
					cur := atomic.AddInt32(&inFlight, 1)
					fmu.Lock()
					if cur > maxFlight {
						maxFlight = cur
					}
					fmu.Unlock()
					atomic.AddInt32(&execCount, 1)
					atomic.AddInt32(&inFlight, -1)
					return nil
				},
			})
		}
	}
	t.Logf("input: tasks=%d layers=%d width=%d workers=8 slots=6 db=3",
		len(specs), layers, width)
	if err := s.Submit(specs); err != nil {
		t.Fatal(err)
	}

	before := runtime.NumGoroutine()
	res, st, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if int(execCount) != len(specs) {
		t.Fatalf("executions=%d want %d (each task exactly once)", execCount, len(specs))
	}
	if st.Succeeded != len(specs) {
		t.Fatalf("stats=%+v", st)
	}
	for _, id := range specIDs {
		if res[id].State != StateSucceeded {
			t.Fatalf("%s = %s", id, res[id].State)
		}
	}
	if maxFlight > 6 { // 受 slots=6 约束（worker=8）
		t.Fatalf("max in-flight=%d exceeds slot limit 6", maxFlight)
	}
	if !waitGoroutines(before, time.Second) {
		t.Fatalf("goroutine leak: base=%d now=%d", before, runtime.NumGoroutine())
	}
	t.Logf("decision: all %d tasks succeeded once; maxFlight=%d (<=6); goroutines settled to %d",
		execCount, maxFlight, runtime.NumGoroutine())
}

// 高并发 + 混合失败：每个失败子树的后继精确 skipped，状态集合可复现。
func TestHighConcurrencyWithFailuresDeterministic(t *testing.T) {
	const n = 200
	s := New(Config{MaxConcurrency: 16, Logger: testLog(t)})
	specs := make([]TaskSpec, n)
	executed := make(chan string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%03d", i)
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("t%03d", i-1)} // 线性链
		}
		failAt := i == 50
		specs[i] = TaskSpec{ID: id, Deps: deps, Run: func(context.Context) error {
			executed <- id
			if failAt {
				return errors.New("planned failure")
			}
			return nil
		}}
	}
	if err := s.Submit(specs); err != nil {
		t.Fatal(err)
	}
	res, st, _ := s.Run(context.Background())
	// 前 51 个（0..50）执行；50 failed；51..199 全部 skipped。
	if st.Succeeded != 50 || st.Failed != 1 || st.Skipped != n-51 {
		t.Fatalf("stats=%+v want succeeded=50 failed=1 skipped=%d", st, n-51)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%03d", i)
		want := StateSucceeded
		switch {
		case i == 50:
			want = StateFailed
		case i > 50:
			want = StateSkipped
		}
		if res[id].State != want {
			t.Fatalf("%s=%s want %s", id, res[id].State, want)
		}
	}
	close(executed)
	seen := map[string]int{}
	for id := range executed {
		seen[id]++
	}
	if len(seen) != 51 {
		t.Fatalf("executed tasks=%d want 51", len(seen))
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s executed %d times", id, c)
		}
	}
	t.Logf("decision: chain failed at t050; succeeded=%d failed=%d skipped=%d, each executed at most once",
		st.Succeeded, st.Failed, st.Skipped)
}

// 取消风暴：多调度器并行运行并随机取消，全部应在有界时间内退出且无协程泄漏/死锁。
// waitGoroutines 轮询等待 goroutine 数回落到基线附近，容忍辅助协程的回收延迟。
func waitGoroutines(base int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= base+2 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestCancellationStormNoLeak(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for iter := 0; iter < 8; iter++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			s := New(Config{MaxConcurrency: 4, TotalSlots: 3, NamedResources: map[string]int{"db": 2}})
			var specs []TaskSpec
			for i := 0; i < 60; i++ {
				id := fmt.Sprintf("i%d-t%d", seed, i)
				var deps []string
				if i >= 2 {
					deps = []string{fmt.Sprintf("i%d-t%d", seed, i-2)}
				}
				specs = append(specs, TaskSpec{
					ID: id, Deps: deps,
					Resources: Resources{Slots: 1, Named: map[string]int{"db": 1}},
					Run: func(ctx context.Context) error {
						select {
						case <-time.After(20 * time.Millisecond):
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					},
				})
			}
			if err := s.Submit(specs); err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(5+seed%20)*time.Millisecond)
			defer cancel()
			done := make(chan struct{})
			go func() { _, _, _ = s.Run(ctx); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Errorf("scheduler %d deadlocked after cancel", seed)
			}
		}(iter)
	}
	wg.Wait()

	// 等待系统 goroutine 回收后检查泄漏。
	if !waitGoroutines(base, 2*time.Second) {
		t.Fatalf("goroutine leak: base=%d now=%d", base, runtime.NumGoroutine())
	}
}
