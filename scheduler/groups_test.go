package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// concTracker 统计某分组任务的实时并发峰值。
type concTracker struct{ cur, max atomic.Int32 }

func (c *concTracker) enter() {
	n := c.cur.Add(1)
	for {
		m := c.max.Load()
		if n <= m || c.max.CompareAndSwap(m, n) {
			break
		}
	}
}
func (c *concTracker) exit() { c.cur.Add(-1) }

func TestGroupConcurrencyQuotaEnforced(t *testing.T) {
	var groupA, ungrouped concTracker
	s, _ := New(Config{
		Workers: 4,
		Groups:  map[string]GroupQuota{"a": {Workers: 1}},
		Logf:    testLogger(t),
	})
	job := func(tr *concTracker) TaskFunc {
		return func(context.Context) error {
			tr.enter()
			time.Sleep(5 * time.Millisecond)
			tr.exit()
			return nil
		}
	}
	t.Logf("input: 3 tasks in group a (quota workers=1) + 2 ungrouped, global workers=4")
	for _, id := range []string{"a1", "a2", "a3"} {
		s.Submit(&Task{ID: id, Group: "a", Fn: job(&groupA)})
	}
	s.Submit(&Task{ID: "u1", Fn: job(&ungrouped)})
	s.Submit(&Task{ID: "u2", Fn: job(&ungrouped)})

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded (quota must queue, never drop)", id, rec.Status)
		}
	}
	t.Logf("verdict: group-a max concurrent=%d (quota 1), ungrouped max=%d, all succeeded",
		groupA.max.Load(), ungrouped.max.Load())
	if groupA.max.Load() > 1 {
		t.Fatalf("group quota violated: max concurrent in group a = %d", groupA.max.Load())
	}
	// 未分组任务不受分组额度影响，应能并行。
	if ungrouped.max.Load() < 2 {
		t.Fatalf("ungrouped tasks should overlap, max=%d", ungrouped.max.Load())
	}
}

func TestSaturatedGroupDoesNotBlockOthers(t *testing.T) {
	var groupB concTracker
	a2Started := make(chan struct{})
	s, _ := New(Config{
		Workers: 4,
		Groups:  map[string]GroupQuota{"a": {Workers: 1}, "b": {Workers: 2}},
		Logf:    testLogger(t),
	})
	// a1 长时间占用分组 a 的唯一额度；a2 排队；b 组任务必须照常推进。
	s.Submit(&Task{ID: "a1", Group: "a", Fn: func(context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}})
	s.Submit(&Task{ID: "a2", Group: "a", Fn: func(context.Context) error {
		close(a2Started)
		return nil
	}})
	for _, id := range []string{"b1", "b2"} {
		s.Submit(&Task{ID: id, Group: "b", Fn: func(context.Context) error {
			groupB.enter()
			time.Sleep(10 * time.Millisecond)
			groupB.exit()
			return nil
		}})
	}
	t.Logf("input: group a quota=1 with a1 slow + a2 queued; group b quota=2 with b1,b2")

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
	}
	t.Logf("verdict: group-b max concurrent=%d while group a saturated; all succeeded", groupB.max.Load())
	if groupB.max.Load() < 2 {
		t.Fatalf("saturated group a blocked group b: b max concurrent=%d", groupB.max.Load())
	}
}

func TestGroupResourceQuotaEnforced(t *testing.T) {
	var groupA, global concTracker
	s, _ := New(Config{
		Workers:  4,
		Capacity: map[string]int64{"gpu": 2},
		Groups:   map[string]GroupQuota{"a": {Capacity: map[string]int64{"gpu": 1}}},
		Logf:     testLogger(t),
	})
	gpuJob := func(tr *concTracker) TaskFunc {
		return func(context.Context) error {
			tr.enter()
			global.enter()
			time.Sleep(5 * time.Millisecond)
			global.exit()
			tr.exit()
			return nil
		}
	}
	t.Logf("input: 2 gpu tasks in group a (group gpu limit=1) + 2 ungrouped gpu tasks, global gpu=2")
	s.Submit(&Task{ID: "ga1", Group: "a", Resources: map[string]int64{"gpu": 1}, Fn: gpuJob(&groupA)})
	s.Submit(&Task{ID: "ga2", Group: "a", Resources: map[string]int64{"gpu": 1}, Fn: gpuJob(&groupA)})
	s.Submit(&Task{ID: "gu1", Resources: map[string]int64{"gpu": 1}, Fn: gpuJob(&concTracker{})})
	s.Submit(&Task{ID: "gu2", Resources: map[string]int64{"gpu": 1}, Fn: gpuJob(&concTracker{})})

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
	}
	t.Logf("verdict: group-a gpu max=%d (limit 1), global gpu max=%d (capacity 2)",
		groupA.max.Load(), global.max.Load())
	if groupA.max.Load() > 1 {
		t.Fatalf("group resource quota violated: %d", groupA.max.Load())
	}
	if global.max.Load() > 2 {
		t.Fatalf("global resource capacity violated: %d", global.max.Load())
	}
}

func TestGroupFairnessNoStarvation(t *testing.T) {
	const perGroup = 5
	var aDone, bDone atomic.Int32
	var bFirstDone, aLastDone atomic.Int64
	s, _ := New(Config{
		Workers: 2,
		Groups: map[string]GroupQuota{
			"a": {Workers: 1},
			"b": {Workers: 1},
		},
		Logf: testLogger(t),
	})
	mk := func(group string, done *atomic.Int32, ts *atomic.Int64, pickMax bool) TaskFunc {
		return func(context.Context) error {
			time.Sleep(2 * time.Millisecond)
			now := time.Now().UnixNano()
			if pickMax {
				for {
					old := ts.Load()
					if now <= old || ts.CompareAndSwap(old, now) {
						break
					}
				}
			} else {
				ts.CompareAndSwap(0, now)
			}
			done.Add(1)
			return nil
		}
	}
	t.Logf("input: groups a,b each quota=1, %d tasks per group, global workers=2", perGroup)
	for i := 0; i < perGroup; i++ {
		s.Submit(&Task{ID: fmt.Sprintf("a%d", i), Group: "a", Fn: mk("a", &aDone, &aLastDone, true)})
		s.Submit(&Task{ID: fmt.Sprintf("b%d", i), Group: "b", Fn: mk("b", &bDone, &bFirstDone, false)})
	}
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s, want succeeded", id, rec.Status)
		}
	}
	t.Logf("verdict: a done=%d b done=%d, b first done before a last done=%v",
		aDone.Load(), bDone.Load(), bFirstDone.Load() < aLastDone.Load())
	if aDone.Load() != perGroup || bDone.Load() != perGroup {
		t.Fatalf("starvation or drop: a=%d b=%d, want %d each", aDone.Load(), bDone.Load(), perGroup)
	}
	// 持续负载下两组交替推进：b 的首次完成必早于 a 的全部完成。
	if bFirstDone.Load() >= aLastDone.Load() {
		t.Fatalf("group b starved behind group a")
	}
}

func TestUnknownGroupFallsBackToGlobalLimits(t *testing.T) {
	var tr concTracker
	s, _ := New(Config{Workers: 2, Logf: testLogger(t)})
	t.Logf("input: 2 tasks in unconfigured group 'ghost', global workers=2")
	for _, id := range []string{"g1", "g2"} {
		s.Submit(&Task{ID: id, Group: "ghost", Fn: func(context.Context) error {
			tr.enter()
			time.Sleep(5 * time.Millisecond)
			tr.exit()
			return nil
		}})
	}
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s = %s", id, rec.Status)
		}
	}
	t.Logf("verdict: unconfigured group max concurrent=%d (only global limit applies)", tr.max.Load())
	if tr.max.Load() < 2 {
		t.Fatalf("unconfigured group should only be limited globally, max=%d", tr.max.Load())
	}
}

func TestGroupSchedulingDeterministic(t *testing.T) {
	runOnce := func() map[string]Record {
		s, _ := New(Config{
			Workers: 3,
			Groups: map[string]GroupQuota{
				"a": {Workers: 1},
				"b": {Workers: 2},
			},
		})
		for i := 0; i < 4; i++ {
			for _, g := range []string{"a", "b"} {
				s.Submit(&Task{ID: fmt.Sprintf("%s%d", g, i), Group: g, Fn: func(context.Context) error {
					time.Sleep(time.Millisecond)
					return nil
				}})
			}
		}
		out, err := s.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	t.Logf("input: same workload (groups a=1,b=2 quota, 8 tasks) run twice")
	r1, r2 := runOnce(), runOnce()
	if len(r1) != len(r2) {
		t.Fatalf("record count differs: %d vs %d", len(r1), len(r2))
	}
	for id, rec1 := range r1 {
		rec2 := r2[id]
		if rec1.Status != rec2.Status || rec1.Attempts != rec2.Attempts {
			t.Fatalf("non-deterministic result for %s: %s/%d vs %s/%d",
				id, rec1.Status, rec1.Attempts, rec2.Status, rec2.Attempts)
		}
	}
	t.Logf("verdict: two runs produced identical status/attempts for all %d tasks", len(r1))
}
