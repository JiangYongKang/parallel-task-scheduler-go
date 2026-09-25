package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// writeJournal 直接构造崩溃现场：A 已成功落盘，B 在途（第 2 次尝试中崩溃）。
func writeJournal(t *testing.T, path string, recs []Record) {
	t.Helper()
	j, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := j.append(r.ID, r.Status, r.Attempts); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverySkipsCompletedAndResumesInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	writeJournal(t, path, []Record{
		{ID: "a", Status: StatusSucceeded, Attempts: 1},
		{ID: "b", Status: StatusRunning, Attempts: 2}, // 崩溃时在途
	})
	t.Logf("input: journal has a=succeeded(1), b=running(2) at crash; restart with same journal")

	var aCalls, bCalls atomic.Int32
	s, _ := New(Config{Workers: 2, JournalPath: path, Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", Fn: func(context.Context) error { aCalls.Add(1); return nil }})
	s.Submit(&Task{ID: "b", DependsOn: []string{"a"}, Fn: func(context.Context) error { bCalls.Add(1); return nil }})

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: a=%s(attempts=%d,calls=%d) b=%s(attempts=%d,calls=%d)",
		out["a"].Status, out["a"].Attempts, aCalls.Load(),
		out["b"].Status, out["b"].Attempts, bCalls.Load())

	// 已完成任务不得重复执行（不重复副作用）。
	if aCalls.Load() != 0 {
		t.Fatalf("completed task re-executed: calls=%d", aCalls.Load())
	}
	if out["a"].Status != StatusSucceeded || out["a"].Attempts != 1 {
		t.Fatalf("recovered a = %s/%d, want succeeded/1", out["a"].Status, out["a"].Attempts)
	}
	// 在途任务重新执行且不丢失已持久化的尝试统计：2(崩溃前) + 1(恢复后) = 3。
	if bCalls.Load() != 1 {
		t.Fatalf("resumed task should run once, got %d", bCalls.Load())
	}
	if out["b"].Status != StatusSucceeded || out["b"].Attempts != 3 {
		t.Fatalf("resumed b = %s/%d, want succeeded/3", out["b"].Status, out["b"].Attempts)
	}
}

func TestRecoveryCascadeSkipFromFailedDependency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	writeJournal(t, path, []Record{
		{ID: "a", Status: StatusFailed, Attempts: 3},
	})
	t.Logf("input: journal has a=failed(3) at crash; b depends on a")
	var bCalls atomic.Int32
	s, _ := New(Config{Workers: 2, JournalPath: path, Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", Fn: noop})
	s.Submit(&Task{ID: "b", DependsOn: []string{"a"}, Fn: func(context.Context) error { bCalls.Add(1); return nil }})
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: a=%s b=%s bCalls=%d", out["a"].Status, out["b"].Status, bCalls.Load())
	if out["b"].Status != StatusSkipped || bCalls.Load() != 0 {
		t.Fatalf("dependent of recovered-failed task must be skipped, got %s calls=%d", out["b"].Status, bCalls.Load())
	}
}

func TestRecoveryStateSelfConsistentUnderConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	// 崩溃现场：一半任务已成功，一半在途。
	var recs []Record
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		if i%2 == 0 {
			recs = append(recs, Record{ID: id, Status: StatusSucceeded, Attempts: 1})
		} else {
			recs = append(recs, Record{ID: id, Status: StatusRunning, Attempts: 1})
		}
	}
	writeJournal(t, path, recs)
	t.Logf("input: 10 tasks, even=succeeded odd=running at crash, workers=4")

	var total atomic.Int32
	s, _ := New(Config{Workers: 4, JournalPath: path, Logf: testLogger(t)})
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		s.Submit(&Task{ID: id, Fn: func(context.Context) error { total.Add(1); return nil }})
	}
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for id, rec := range out {
		if rec.Status != StatusSucceeded {
			t.Fatalf("task %s not succeeded after recovery: %s", id, rec.Status)
		}
	}
	// 只有 5 个在途任务需要重新执行。
	if total.Load() != 5 {
		t.Fatalf("re-executed %d tasks, want 5", total.Load())
	}
	t.Logf("verdict: re-executed=%d, all 10 succeeded, no duplicates", total.Load())
}

func TestJournalFileActuallyPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	s, _ := New(Config{Workers: 1, JournalPath: path})
	s.Submit(&Task{ID: "x", Fn: noop})
	if _, err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Size() == 0 {
		t.Fatalf("journal not persisted: %v size=%d", err, fi.Size())
	}
}
