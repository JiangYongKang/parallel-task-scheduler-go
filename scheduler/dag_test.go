package scheduler

import (
	"context"
	"errors"
	"testing"
)

func noop(context.Context) error { return nil }

func testLogger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) { t.Logf(format, args...) }
}

func TestDuplicateTaskRejected(t *testing.T) {
	s, _ := New(Config{Logf: testLogger(t)})
	if err := s.Submit(&Task{ID: "a", Fn: noop}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	err := s.Submit(&Task{ID: "a", Fn: noop})
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}
	t.Logf("input: duplicate id 'a'; verdict: %v", err)
}

func TestDuplicateDependencyRejected(t *testing.T) {
	s, _ := New(Config{Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", Fn: noop})
	err := s.Submit(&Task{ID: "b", DependsOn: []string{"a", "a"}, Fn: noop})
	if !errors.Is(err, ErrDuplicateDependency) {
		t.Fatalf("want ErrDuplicateDependency, got %v", err)
	}
	t.Logf("input: b depends on [a a]; verdict: %v", err)
}

func TestSelfDependencyRejected(t *testing.T) {
	s, _ := New(Config{Logf: testLogger(t)})
	err := s.Submit(&Task{ID: "a", DependsOn: []string{"a"}, Fn: noop})
	if !errors.Is(err, ErrSelfDependency) {
		t.Fatalf("want ErrSelfDependency, got %v", err)
	}
	t.Logf("input: a depends on itself; verdict: %v", err)
}

func TestMissingDependencyRejected(t *testing.T) {
	s, _ := New(Config{Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", DependsOn: []string{"ghost"}, Fn: noop})
	_, err := s.Run(context.Background())
	if !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("want ErrMissingDependency, got %v", err)
	}
	t.Logf("input: a depends on missing 'ghost'; verdict: %v", err)
}

func TestCyclicDependencyRejected(t *testing.T) {
	s, _ := New(Config{Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", DependsOn: []string{"c"}, Fn: noop})
	s.Submit(&Task{ID: "b", DependsOn: []string{"a"}, Fn: noop})
	s.Submit(&Task{ID: "c", DependsOn: []string{"b"}, Fn: noop})
	_, err := s.Run(context.Background())
	if !errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("want ErrCyclicDependency, got %v", err)
	}
	t.Logf("input: a->c->b->a cycle; verdict: %v", err)
}
