package scheduler

import (
	"context"
	"fmt"
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

// TestRecoveryToleratesCorruptTail 模拟写进度中途被强杀：
// 日志尾部留下半行不完整 JSON，重启必须正常恢复，不能让整次恢复失败。
func TestRecoveryToleratesCorruptTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	writeJournal(t, path, []Record{
		{ID: "a", Status: StatusSucceeded, Attempts: 1},
		{ID: "b", Status: StatusRunning, Attempts: 2},
	})
	// 在尾部拼接半行残缺记录（崩溃撕裂写）。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"id":"c","status":"running","atte`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: journal a=succeeded(1), b=running(2), plus torn half-line tail from mid-write kill")

	var aCalls, bCalls, cCalls atomic.Int32
	s, _ := New(Config{Workers: 2, JournalPath: path, Logf: testLogger(t)})
	s.Submit(&Task{ID: "a", Fn: func(context.Context) error { aCalls.Add(1); return nil }})
	s.Submit(&Task{ID: "b", Fn: func(context.Context) error { bCalls.Add(1); return nil }})
	s.Submit(&Task{ID: "c", Fn: func(context.Context) error { cCalls.Add(1); return nil }})

	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("recovery must not fail on corrupt tail: %v", err)
	}
	t.Logf("verdict: a=%s(%d,calls=%d) b=%s(%d,calls=%d) c=%s(%d,calls=%d)",
		out["a"].Status, out["a"].Attempts, aCalls.Load(),
		out["b"].Status, out["b"].Attempts, bCalls.Load(),
		out["c"].Status, out["c"].Attempts, cCalls.Load())
	// 已成功任务不重复执行。
	if aCalls.Load() != 0 || out["a"].Status != StatusSucceeded {
		t.Fatalf("completed task re-executed: calls=%d status=%s", aCalls.Load(), out["a"].Status)
	}
	// 在途任务保留尝试计数继续。
	if out["b"].Status != StatusSucceeded || out["b"].Attempts != 3 || bCalls.Load() != 1 {
		t.Fatalf("in-flight task progress lost: %s/%d calls=%d", out["b"].Status, out["b"].Attempts, bCalls.Load())
	}
	// 残缺记录被丢弃，对应任务按全新任务正常执行一次。
	if out["c"].Status != StatusSucceeded || cCalls.Load() != 1 {
		t.Fatalf("torn-record task must run exactly once: %s calls=%d", out["c"].Status, cCalls.Load())
	}
}

// TestRecoveryKilledAfterAdmission 模拟刚拿到准入（资源已分配）还没真正
// 开始执行时被杀：日志最后一条是 ready。重启后在途进度不丢、任务恰好补跑一次。
func TestRecoveryKilledAfterAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	writeJournal(t, path, []Record{
		{ID: "x", Status: StatusReady, Attempts: 0}, // 已准入未执行时崩溃
		{ID: "y", Status: StatusRunning, Attempts: 4},
	})
	t.Logf("input: journal x=ready(0) (killed after admission, before running), y=running(4)")

	var xCalls, yCalls atomic.Int32
	s, _ := New(Config{Workers: 2, JournalPath: path, Logf: testLogger(t)})
	s.Submit(&Task{ID: "x", Fn: func(context.Context) error { xCalls.Add(1); return nil }})
	s.Submit(&Task{ID: "y", Fn: func(context.Context) error { yCalls.Add(1); return nil }})
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("verdict: x=%s(%d,calls=%d) y=%s(%d,calls=%d)",
		out["x"].Status, out["x"].Attempts, xCalls.Load(),
		out["y"].Status, out["y"].Attempts, yCalls.Load())
	if xCalls.Load() != 1 || out["x"].Status != StatusSucceeded || out["x"].Attempts != 1 {
		t.Fatalf("admitted-but-never-ran task must run exactly once: %s/%d calls=%d",
			out["x"].Status, out["x"].Attempts, xCalls.Load())
	}
	// 在途尝试计数不丢：4(崩溃前) + 1(恢复后) = 5。
	if yCalls.Load() != 1 || out["y"].Attempts != 5 {
		t.Fatalf("in-flight attempts lost: %s/%d calls=%d", out["y"].Status, out["y"].Attempts, yCalls.Load())
	}
}

// TestRecoveryCorruptTailUnderConcurrency 并发恢复 + 残缺尾部：状态自洽、无重复执行。
func TestRecoveryCorruptTailUnderConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	var recs []Record
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("t%02d", i)
		if i%3 == 0 {
			recs = append(recs, Record{ID: id, Status: StatusSucceeded, Attempts: 1})
		} else {
			recs = append(recs, Record{ID: id, Status: StatusRunning, Attempts: i})
		}
	}
	writeJournal(t, path, recs)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"id":"t01","status":"run`) // 撕裂写
	f.Close()
	t.Logf("input: 12 tasks (1/3 succeeded, 2/3 in-flight) + torn tail, workers=4")

	counts := make(map[string]*atomic.Int32)
	s, _ := New(Config{Workers: 4, JournalPath: path, Logf: testLogger(t)})
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("t%02d", i)
		c := &atomic.Int32{}
		counts[id] = c
		s.Submit(&Task{ID: id, Fn: func(context.Context) error { c.Add(1); return nil }})
	}
	out, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("concurrent recovery with corrupt tail must not fail: %v", err)
	}
	reran := 0
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("t%02d", i)
		if out[id].Status != StatusSucceeded {
			t.Fatalf("task %s = %s after recovery, want succeeded", id, out[id].Status)
		}
		n := int(counts[id].Load())
		reran += n
		if i%3 == 0 && n != 0 {
			t.Fatalf("completed task %s re-executed %d times", id, n)
		}
		if i%3 != 0 && n != 1 {
			t.Fatalf("in-flight task %s executed %d times, want 1", id, n)
		}
	}
	t.Logf("verdict: re-executed=%d (want 8), all 12 succeeded, state self-consistent", reran)
	if reran != 8 {
		t.Fatalf("re-executed %d tasks, want 8", reran)
	}
}
