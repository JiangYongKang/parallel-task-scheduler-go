package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestUnknownResourceRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "x", Resources: map[string]int64{"tpu": 1}, Fn: noop})
	t.Logf("input: task requests 'tpu' but only 'gpu' configured; verdict: %v", err)
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("want ErrUnknownResource, got %v", err)
	}
	// 与依赖类错误、超容量错误严格区分。
	if errors.Is(err, ErrResourceExceedsCapacity) || errors.Is(err, ErrMissingDependency) ||
		errors.Is(err, ErrDuplicateTask) || errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("error kinds must be distinguishable: %v", err)
	}
}

func TestExceedsTotalCapacityRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 2}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "x", Resources: map[string]int64{"gpu": 3}, Fn: noop})
	t.Logf("input: task needs gpu=3 but total capacity=2; verdict: %v", err)
	if !errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("want ErrResourceExceedsCapacity, got %v", err)
	}
	if errors.Is(err, ErrUnknownResource) || errors.Is(err, ErrMissingDependency) {
		t.Fatalf("error kinds must be distinguishable: %v", err)
	}
}

func TestExceedsGroupQuotaRejected(t *testing.T) {
	s, _ := New(Config{
		Capacity: map[string]int64{"gpu": 4},
		Groups:   map[string]GroupQuota{"a": {Capacity: map[string]int64{"gpu": 1}}},
		Logf:     testLogger(t),
	})
	err := s.Submit(&Task{ID: "x", Group: "a", Resources: map[string]int64{"gpu": 2}, Fn: noop})
	t.Logf("input: task needs gpu=2, group a limit=1 (global=4); verdict: %v", err)
	if !errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("want ErrResourceExceedsCapacity, got %v", err)
	}
}

func TestNegativeResourceRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "x", Resources: map[string]int64{"gpu": -1}, Fn: noop})
	t.Logf("input: task needs gpu=-1; verdict: %v", err)
	if !errors.Is(err, ErrInvalidResourceRequest) {
		t.Fatalf("want ErrInvalidResourceRequest, got %v", err)
	}
}

func TestInvalidDeclarationDoesNotStallBatch(t *testing.T) {
	s, _ := New(Config{Workers: 2, Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	// 非法任务在 Submit 即被拒绝，根本不会进入调度队列。
	if err := s.Submit(&Task{ID: "bad", Resources: map[string]int64{"gpu": 99}, Fn: noop}); !errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("bad task should be rejected at submit, got %v", err)
	}
	var ran atomic.Int32
	s.Submit(&Task{ID: "ok1", Resources: map[string]int64{"gpu": 1}, Fn: func(context.Context) error { ran.Add(1); return nil }})
	s.Submit(&Task{ID: "ok2", DependsOn: []string{"ok1"}, Fn: func(context.Context) error { ran.Add(1); return nil }})
	t.Logf("input: 1 rejected task (gpu=99 > capacity 1) + 2 valid tasks")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: ran=%d, ok1=%s ok2=%s", ran.Load(), out["ok1"].Status, out["ok2"].Status)
	if ran.Load() != 2 || out["ok1"].Status != StatusSucceeded || out["ok2"].Status != StatusSucceeded {
		t.Fatalf("valid tasks must not be stalled by rejected task: ran=%d out=%v", ran.Load(), out)
	}
}

func TestWellFormedDeclarationUnaffected(t *testing.T) {
	s, _ := New(Config{
		Workers:  2,
		Capacity: map[string]int64{"gpu": 2},
		Groups:   map[string]GroupQuota{"a": {Capacity: map[string]int64{"gpu": 2}}},
		Logf:     testLogger(t),
	})
	// 申请量等于总容量、等于分组上限：边界值必须放行，不被校验误伤。
	var ran atomic.Int32
	if err := s.Submit(&Task{ID: "exact", Group: "a", Resources: map[string]int64{"gpu": 2}, Fn: func(context.Context) error { ran.Add(1); return nil }}); err != nil {
		t.Fatalf("exact-fit task must be accepted, got %v", err)
	}
	if err := s.Submit(&Task{ID: "free", Fn: func(context.Context) error { ran.Add(1); return nil }}); err != nil {
		t.Fatalf("resource-free task must be accepted, got %v", err)
	}
	t.Logf("input: task needing exactly capacity (gpu=2/2) + resource-free task")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: exact=%s free=%s ran=%d", out["exact"].Status, out["free"].Status, ran.Load())
	if out["exact"].Status != StatusSucceeded || out["free"].Status != StatusSucceeded || ran.Load() != 2 {
		t.Fatalf("well-formed tasks regressed: %v ran=%d", out, ran.Load())
	}
}
