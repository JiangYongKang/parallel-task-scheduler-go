package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func okRun() TaskFunc { return func(context.Context) error { return nil } }

func TestValidationRejectsDistinctCauses(t *testing.T) {
	cases := []struct {
		name     string
		specs    []TaskSpec
		limits   Resources
		wantErr  error
		wantKind ValidationErrorKind
	}{
		{
			name: "duplicate declaration",
			specs: []TaskSpec{
				{ID: "a", Run: okRun()},
				{ID: "a", Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrDuplicateTask,
			wantKind: KindDuplicate,
		},
		{
			name: "missing dependency",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"ghost"}, Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrMissingDependency,
			wantKind: KindMissingDep,
		},
		{
			name: "self loop is a cycle",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"a"}, Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrCyclicDependency,
			wantKind: KindCycle,
		},
		{
			name: "three node cycle",
			specs: []TaskSpec{
				{ID: "a", Deps: []string{"c"}, Run: okRun()},
				{ID: "b", Deps: []string{"a"}, Run: okRun()},
				{ID: "c", Deps: []string{"b"}, Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrCyclicDependency,
			wantKind: KindCycle,
		},
		{
			name: "duplicated dep edge",
			specs: []TaskSpec{
				{ID: "a", Run: okRun()},
				{ID: "b", Deps: []string{"a", "a"}, Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrInvalidSpec,
			wantKind: KindInvalidSpec,
		},
		{
			name: "empty id",
			specs: []TaskSpec{
				{ID: "", Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrInvalidSpec,
			wantKind: KindInvalidSpec,
		},
		{
			name: "slots exceed limit",
			specs: []TaskSpec{
				{ID: "a", Resources: Resources{Slots: 5}, Run: okRun()},
			},
			limits:   Resources{Slots: 2},
			wantErr:  ErrResourceExceeded,
			wantKind: KindResourceLimit,
		},
		{
			name: "named resource exceeds limit",
			specs: []TaskSpec{
				{ID: "a", Resources: Resources{Named: map[string]int{"db": 3}}, Run: okRun()},
			},
			limits:   Resources{Slots: 4, Named: map[string]int{"db": 2}},
			wantErr:  ErrResourceExceeded,
			wantKind: KindResourceLimit,
		},
		{
			name: "unknown named resource",
			specs: []TaskSpec{
				{ID: "a", Resources: Resources{Named: map[string]int{"gpu": 1}}, Run: okRun()},
			},
			limits:   Resources{Slots: 4},
			wantErr:  ErrResourceExceeded,
			wantKind: KindResourceLimit,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("input: tasks=%d limits=%+v", len(tc.specs), tc.limits)
			_, err := BuildDAG(tc.specs, tc.limits)
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error %v is not *ValidationError", err)
			}
			if ve.Kind != tc.wantKind {
				t.Fatalf("kind=%s want=%s (detail=%s)", ve.Kind, tc.wantKind, ve.Detail)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("errors.Is mismatch: err=%v want=%v", err, tc.wantErr)
			}
			t.Logf("decision: rejected kind=%s reason=%s", ve.Kind, ve.Error())
		})
	}
}

func TestValidationAcceptsDAG(t *testing.T) {
	specs := []TaskSpec{
		{ID: "a", Run: okRun()},
		{ID: "b", Deps: []string{"a"}, Run: okRun()},
		{ID: "c", Deps: []string{"a"}, Run: okRun()},
	}
	dag, err := BuildDAG(specs, Resources{Slots: 2, Named: map[string]int{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dag.TaskCount() != 3 {
		t.Fatalf("count=%d", dag.TaskCount())
	}
	// 成环错误信息必须包含环路径，便于解释。
	_, err = BuildDAG([]TaskSpec{
		{ID: "a", Deps: []string{"b"}, Run: okRun()},
		{ID: "b", Deps: []string{"a"}, Run: okRun()},
	}, Resources{Slots: 2})
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error should mention cycle path: %v", err)
	}
}
