package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestUnknownResourceRejected 引用未配置资源的任务在提交时被明确拒绝，
// 且错误与依赖类问题可区分。
func TestUnknownResourceRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "bad", Resources: map[string]int64{"tpu": 1}, Fn: noop})
	t.Logf("input: task requests unconfigured resource 'tpu'; verdict: %v", err)
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("want ErrUnknownResource, got %v", err)
	}
	// 与依赖类错误严格区分。
	if errors.Is(err, ErrMissingDependency) || errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("resource error must be distinguishable from dependency errors: %v", err)
	}
	if errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("unknown-resource must differ from exceeds-capacity: %v", err)
	}
}

// TestExceedsCapacityRejected 单任务申请超过总容量时被拒绝，错误可与未知资源区分。
func TestExceedsCapacityRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 2}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "greedy", Resources: map[string]int64{"gpu": 3}, Fn: noop})
	t.Logf("input: task wants gpu=3, capacity=2; verdict: %v", err)
	if !errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("want ErrResourceExceedsCapacity, got %v", err)
	}
	if errors.Is(err, ErrUnknownResource) {
		t.Fatalf("exceeds-capacity must differ from unknown-resource: %v", err)
	}
}

// TestExceedsGroupQuotaRejected 申请超过所属分组额度同样永远不可满足，提交时拒绝。
func TestExceedsGroupQuotaRejected(t *testing.T) {
	s, _ := New(Config{
		Capacity: map[string]int64{"gpu": 4},
		Groups:   map[string]GroupQuota{"g": {Resources: map[string]int64{"gpu": 1}}},
		Logf:     testLogger(t),
	})
	err := s.Submit(&Task{ID: "big", Group: "g", Resources: map[string]int64{"gpu": 2}, Fn: noop})
	t.Logf("input: task in group g(gpu<=1) wants gpu=2, capacity=4; verdict: %v", err)
	if !errors.Is(err, ErrResourceExceedsCapacity) {
		t.Fatalf("want ErrResourceExceedsCapacity, got %v", err)
	}
}

// TestNegativeResourceAmountRejected 负数申请非法。
func TestNegativeResourceAmountRejected(t *testing.T) {
	s, _ := New(Config{Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "neg", Resources: map[string]int64{"gpu": -1}, Fn: noop})
	t.Logf("input: task wants gpu=-1; verdict: %v", err)
	if !errors.Is(err, ErrInvalidResourceAmount) {
		t.Fatalf("want ErrInvalidResourceAmount, got %v", err)
	}
}

// TestInvalidDeclarationDoesNotBlockBatch 非法声明在提交时被挡在门外，
// 其余配置齐全的任务照常完成，整批不被拖住。
func TestInvalidDeclarationDoesNotBlockBatch(t *testing.T) {
	s, _ := New(Config{Workers: 2, Capacity: map[string]int64{"gpu": 1}, Logf: testLogger(t)})
	if err := s.Submit(&Task{ID: "bad", Resources: map[string]int64{"ghost": 1}, Fn: noop}); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("bad task must be rejected at submit, got %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := s.Submit(&Task{ID: fmt.Sprintf("ok-%d", i), Resources: map[string]int64{"gpu": 1}, Fn: noop}); err != nil {
			t.Fatalf("valid task rejected: %v", err)
		}
	}
	t.Logf("input: 1 unknown-resource task (rejected) + 5 valid gpu tasks, workers=2")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 {
		t.Fatalf("rejected task leaked into batch: %d records", len(out))
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("valid task %s = %s, batch was blocked by invalid declaration", id, rec.Status)
		}
	}
	t.Logf("verdict: all 5 valid tasks succeeded, invalid declaration never entered the batch")
}

// TestValidDeclarationsUnaffected 配置齐全、容量够用的任务行为与校验引入前一致。
func TestValidDeclarationsUnaffected(t *testing.T) {
	s, _ := New(Config{
		Workers:  2,
		Capacity: map[string]int64{"gpu": 2},
		Groups:   map[string]GroupQuota{"g": {Resources: map[string]int64{"gpu": 2}}},
		Logf:     testLogger(t),
	})
	// 边界：申请量恰好等于容量 / 恰好等于分组额度，以及零占用声明，都必须放行。
	cases := []*Task{
		{ID: "exact-cap", Resources: map[string]int64{"gpu": 2}, Fn: noop},
		{ID: "exact-group", Group: "g", Resources: map[string]int64{"gpu": 2}, Fn: noop},
		{ID: "zero-amount", Resources: map[string]int64{"gpu": 0}, Fn: noop},
		{ID: "no-resources", Fn: noop},
	}
	for _, task := range cases {
		if err := s.Submit(task); err != nil {
			t.Fatalf("valid task %q falsely rejected: %v", task.ID, err)
		}
	}
	t.Logf("input: exact-capacity, exact-group-quota, zero-amount, no-resource declarations")
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range cases {
		if out[task.ID].Status != StatusSucceeded {
			t.Fatalf("valid task %q = %s, want succeeded", task.ID, out[task.ID].Status)
		}
	}
	t.Logf("verdict: all boundary-valid declarations accepted and succeeded")
}
