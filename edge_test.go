package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSubmitTwiceRejected(t *testing.T) {
	s := New(Config{MaxConcurrency: 1})
	one := []TaskSpec{{ID: "a", Run: okRun()}}
	if err := s.Submit(one); err != nil {
		t.Fatal(err)
	}
	err := s.Submit(one)
	if !errors.Is(err, ErrAlreadySubmitted) {
		t.Fatalf("err=%v want ErrAlreadySubmitted", err)
	}
	t.Logf("decision: second submit rejected: %v", err)
}

func TestRunBeforeSubmitRejected(t *testing.T) {
	s := New(Config{MaxConcurrency: 1})
	_, _, err := s.Run(context.Background())
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("err=%v want ErrInvalidSpec", err)
	}
	t.Logf("decision: run-before-submit rejected: %v", err)
}

func TestNegativeAndZeroResources(t *testing.T) {
	if _, err := BuildDAG([]TaskSpec{
		{ID: "a", Resources: Resources{Slots: -1}, Run: okRun()},
	}, Resources{Slots: 1}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("negative slots: %v", err)
	}
	if _, err := BuildDAG([]TaskSpec{
		{ID: "a", Resources: Resources{Named: map[string]int{"x": -1}}, Run: okRun()},
	}, Resources{Slots: 1, Named: map[string]int{"x": 1}}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("negative named: %v", err)
	}
	if _, err := BuildDAG(nil, Resources{Slots: -1}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("negative limit: %v", err)
	}
	// 零需求资源任务可以准入。
	p := NewResourcePool(0, map[string]int{"db": 0})
	if !p.TryAcquire(Resources{}) {
		t.Fatal("zero-demand acquire should succeed on zero-capacity pool")
	}
	t.Log("decision: negative specs rejected; zero-demand acquire allowed")
}

func TestBackoffZeroRetriesImmediately(t *testing.T) {
	var calls int
	s := New(Config{MaxConcurrency: 1})
	_ = s.Submit([]TaskSpec{{
		ID: "x", MaxAttempts: 3, Backoff: 0,
		Run: func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("e")
			}
			return nil
		},
	}})
	res, _, _ := s.Run(context.Background())
	if calls != 3 || res["x"].State != StateSucceeded {
		t.Fatalf("calls=%d res=%s", calls, res["x"].State)
	}
	t.Log("decision: zero backoff retries immediately and succeeds")
}

func TestFileStoreCorruptAndErrorPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fs.Load(); err == nil {
		t.Fatal("corrupt progress file must surface a load error")
	} else {
		t.Logf("decision: corrupt file rejected: %v", err)
	}

	// 空文件视为空白进度。
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	fs2, _ := NewFileStore(empty)
	recs, stats, err := fs2.Load()
	if err != nil || len(recs) != 0 || stats.TotalAttempts != 0 {
		t.Fatalf("empty file: recs=%d stats=%+v err=%v", len(recs), stats, err)
	}

	// 空路径拒绝。
	if _, err := NewFileStore(""); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("empty path err=%v", err)
	}

	// MemoryStore 关闭后写入报错。
	ms := NewMemoryStore()
	_ = ms.Close()
	if err := ms.Save(TaskRecord{ID: "a", State: StateSucceeded}, StatsData{}); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("closed store save err=%v", err)
	}
	t.Log("decision: store error paths distinguishable")
}

// 任务返回 (nil, nil) 但 ctx 已结束（罕见但需确定）：按上下文判定为取消。
func TestNilErrorButCanceledContext(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, Logger: testLog(t)})
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	_ = s.Submit([]TaskSpec{{ID: "a", Run: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil // 故意返回 nil，但 ctx 已取消
	}}})
	done := make(chan struct{})
	var res map[string]Result
	go func() { res, _, _ = s.Run(ctx); close(done) }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish")
	}
	if res["a"].State != StateCanceled {
		t.Fatalf("state=%s want canceled (ctx state authoritative)", res["a"].State)
	}
	t.Logf("decision: nil error with canceled ctx => %s", res["a"].State)
}
