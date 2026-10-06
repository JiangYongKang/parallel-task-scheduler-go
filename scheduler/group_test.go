package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// maxTracker 统计某分组的最大并发占用。
type maxTracker struct {
	cur atomic.Int32
	max atomic.Int32
}

func (m *maxTracker) enter() {
	c := m.cur.Add(1)
	for {
		old := m.max.Load()
		if c <= old || m.max.CompareAndSwap(old, c) {
			break
		}
	}
}

func (m *maxTracker) exit() { m.cur.Add(-1) }

// TestGroupConcurrencyQuota 验证分组并发额度生效且全局上限仍是最外层硬约束。
func TestGroupConcurrencyQuota(t *testing.T) {
	s, _ := New(Config{
		Workers: 4,
		Groups: map[string]GroupQuota{
			"etl": {Workers: 1}, // 收紧到 1
			"bi":  {Workers: 2},
		},
		Logf: testLogger(t),
	})
	track := map[string]*maxTracker{"etl": {}, "bi": {}}
	var global maxTracker
	mk := func(group string, n int) {
		for i := 0; i < n; i++ {
			g := group
			s.Submit(&Task{ID: fmt.Sprintf("%s-%d", g, i), Group: g, Fn: func(context.Context) error {
				track[g].enter()
				global.enter()
				time.Sleep(2 * time.Millisecond)
				global.exit()
				track[g].exit()
				return nil
			}})
		}
	}
	mk("etl", 4)
	mk("bi", 4)
	t.Logf("input: groups etl(workers=1) x4, bi(workers=2) x4, global workers=4")

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
	}
	t.Logf("verdict: etl max inflight=%d (quota 1), bi max inflight=%d (quota 2), global max=%d (quota 4)",
		track["etl"].max.Load(), track["bi"].max.Load(), global.max.Load())
	if got := track["etl"].max.Load(); got > 1 {
		t.Fatalf("etl group quota violated: max inflight=%d", got)
	}
	if got := track["bi"].max.Load(); got > 2 {
		t.Fatalf("bi group quota violated: max inflight=%d", got)
	}
	if got := global.max.Load(); got > 4 {
		t.Fatalf("global hard limit violated: max inflight=%d", got)
	}
}

// TestSaturatedGroupDoesNotBlockOthers 验证额度用满的分组排队等待，
// 不丢弃、也不拖住其他分组的任务。
func TestSaturatedGroupDoesNotBlockOthers(t *testing.T) {
	s, _ := New(Config{
		Workers: 2,
		Groups: map[string]GroupQuota{
			"slow": {Workers: 1},
		},
		Logf: testLogger(t),
	})
	release := make(chan struct{})
	var fastDone atomic.Int32
	// slow 分组先占满自己的额度（任务阻塞到测试末尾），其后续任务排队；
	// 无分组任务应照常完成，不被拖住。
	s.Submit(&Task{ID: "slow-0", Group: "slow", Fn: func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	s.Submit(&Task{ID: "slow-1", Group: "slow", Fn: noop})
	for i := 0; i < 3; i++ {
		s.Submit(&Task{ID: fmt.Sprintf("free-%d", i), Fn: func(context.Context) error {
			fastDone.Add(1)
			return nil
		}})
	}
	t.Logf("input: group slow quota=1 with slow-0 blocking, slow-1 queued; 3 ungrouped tasks; global workers=2")

	runDone := make(chan map[string]Record)
	go func() {
		out, err := s.Run(context.Background())
		if err != nil {
			t.Errorf("run: %v", err)
		}
		runDone <- out
	}()
	// 等待无分组任务完成：若 slow 分组拖住全局，这里会超时。
	deadline := time.After(5 * time.Second)
	for fastDone.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("saturated group blocked ungrouped tasks")
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.Logf("verdict: 3 ungrouped tasks finished while slow group saturated; releasing slow-0")
	close(release)
	out := <-runDone
	// 排队中的 slow-1 最终也必须完成（不丢弃、不饿死）。
	if out["slow-1"].Status != StatusSucceeded {
		t.Fatalf("queued group task must eventually run, got %s", out["slow-1"].Status)
	}
	t.Logf("verdict: slow-1 eventually %s, no task discarded", out["slow-1"].Status)
}

// TestGroupResourceQuota 验证分组命名资源额度，以及持续负载下各分组不被饿死、结果确定可复现。
func TestGroupResourceQuota(t *testing.T) {
	runOnce := func() map[string]Record {
		s, _ := New(Config{
			Workers:  4,
			Capacity: map[string]int64{"gpu": 2},
			Groups: map[string]GroupQuota{
				"train": {Resources: map[string]int64{"gpu": 1}}, // 分组只能用 1 张卡
			},
			Logf: testLogger(t),
		})
		var trainGPU, inferGPU maxTracker
		for i := 0; i < 4; i++ {
			s.Submit(&Task{ID: fmt.Sprintf("train-%d", i), Group: "train",
				Resources: map[string]int64{"gpu": 1},
				Fn: func(context.Context) error {
					trainGPU.enter()
					time.Sleep(2 * time.Millisecond)
					trainGPU.exit()
					return nil
				}})
			s.Submit(&Task{ID: fmt.Sprintf("infer-%d", i),
				Resources: map[string]int64{"gpu": 1},
				Fn: func(context.Context) error {
					inferGPU.enter()
					time.Sleep(2 * time.Millisecond)
					inferGPU.exit()
					return nil
				}})
		}
		out, err := s.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("verdict: train max gpu=%d (group quota 1), infer max gpu=%d (global 2)",
			trainGPU.max.Load(), inferGPU.max.Load())
		if got := trainGPU.max.Load(); got > 1 {
			t.Fatalf("train group gpu quota violated: %d", got)
		}
		if got := inferGPU.max.Load(); got > 2 {
			t.Fatalf("global gpu capacity violated: %d", got)
		}
		for id, rec := range out {
			if rec.Status != StatusSucceeded {
				t.Fatalf("task %s = %s under continuous load, want succeeded (starvation?)", id, rec.Status)
			}
		}
		return out
	}
	t.Logf("input: train(group gpu=1) x4 + infer(ungrouped) x4, global gpu=2, workers=4; run twice for reproducibility")
	first := runOnce()
	second := runOnce()
	// 结果确定且可复现：两次运行的最终状态与尝试次数一致（时间戳除外）。
	for id, rec := range first {
		got := second[id]
		if got.Status != rec.Status || got.Attempts != rec.Attempts {
			t.Fatalf("non-reproducible result for %s: %s/%d vs %s/%d",
				id, rec.Status, rec.Attempts, got.Status, got.Attempts)
		}
	}
	t.Logf("verdict: two runs produced identical final records (%d tasks)", len(first))
}

// TestGroupQuotaFairnessAcrossRounds 多轮打乱顺序的负载下，每个有任务等待的分组都能推进。
func TestGroupQuotaFairnessAcrossRounds(t *testing.T) {
	var mu sync.Mutex
	completed := map[string]int{}
	s, _ := New(Config{
		Workers: 3,
		Groups: map[string]GroupQuota{
			"a": {Workers: 1},
			"b": {Workers: 1},
			"c": {Workers: 1},
		},
		Logf: testLogger(t),
	})
	for _, g := range []string{"a", "b", "c"} {
		for i := 0; i < 5; i++ {
			g := g
			s.Submit(&Task{ID: fmt.Sprintf("%s-%d", g, i), Group: g, Fn: func(context.Context) error {
				time.Sleep(time.Millisecond)
				mu.Lock()
				completed[g]++
				mu.Unlock()
				return nil
			}})
		}
	}
	t.Logf("input: 3 groups x5 tasks, each group quota=1, global workers=3")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("verdict: completed per group=%v", completed)
	for _, g := range []string{"a", "b", "c"} {
		if completed[g] != 5 {
			t.Fatalf("group %s starved: completed %d/5", g, completed[g])
		}
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
	}
}
