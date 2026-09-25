package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 进程重启（同一持久化存储上新 Scheduler）：
// 已成功任务的 Run 不会再次执行；未完成任务从中断点继续，在途 attempts 统计不丢失。
func TestRecoverySkipsCompletedAndContinues(t *testing.T) {
	store := NewMemoryStore()

	flakyCalls := int32(0)
	specs := []TaskSpec{
		{ID: "a", Run: func(context.Context) error { return nil }},
		{
			ID: "b", Deps: []string{"a"}, MaxAttempts: 3, Backoff: time.Millisecond,
			Run: func(ctx context.Context) error {
				n := atomic.AddInt32(&flakyCalls, 1)
				if n < 2 {
					return errors.New("transient: simulate crash after first attempt")
				}
				return nil
			},
		},
		{ID: "c", Deps: []string{"b"}, Run: func(context.Context) error { return nil }},
	}

	// 第一次"进程"：a 成功；b 第一次失败后，模拟中断（不再重跑，丢弃调度器）。
	s1 := New(Config{MaxConcurrency: 2, Store: store, Logger: testLog(t)})
	if err := s1.Submit([]TaskSpec{
		specs[0],
		{
			ID: "b", Deps: []string{"a"}, MaxAttempts: 1, // 只给一次机会：失败落盘后"进程崩溃"
			Run: specs[1].Run,
		},
		specs[2],
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("input (run 1): a must succeed; b fails its only attempt then process dies; c is pending")
	res1, st1, _ := s1.Run(context.Background())
	if res1["a"].State != StateSucceeded || res1["b"].State != StateFailed {
		t.Fatalf("run1 unexpected: a=%s b=%s", res1["a"].State, res1["b"].State)
	}
	if st1.TotalAttempts != 2 {
		t.Fatalf("run1 attempts=%d want 2", st1.TotalAttempts)
	}

	// 第二次"进程"：用同一 Store 与完整规格重启。a 已完成不得再执行；
	// b 按设计已 failed 保持终态（下游 c skipped），并验证统计从持久化基线继续累计。
	var aRuns int32
	s2 := New(Config{MaxConcurrency: 2, Store: store, Logger: testLog(t)})
	if err := s2.Submit([]TaskSpec{
		{ID: "a", Run: func(context.Context) error {
			atomic.AddInt32(&aRuns, 1)
			t.Error("completed task a was re-executed after restart")
			return nil
		}},
		specs[1],
		specs[2],
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("input (run 2): restart; a must be skipped as already succeeded; b stays failed; c skipped")
	res2, st2, _ := s2.Run(context.Background())
	if atomic.LoadInt32(&aRuns) != 0 {
		t.Fatal("side effect of completed task repeated")
	}
	if res2["a"].State != StateSucceeded || res2["a"].Attempts != 1 {
		t.Fatalf("a after recovery = %+v", res2["a"])
	}
	if res2["b"].State != StateFailed {
		t.Fatalf("b = %s want failed (durable terminal)", res2["b"].State)
	}
	if res2["c"].State != StateSkipped {
		t.Fatalf("c = %s want skipped", res2["c"].State)
	}
	// 在途统计：a=1 + b=1 已在磁盘，第二轮没有任何新执行，故总计仍为 2。
	if st2.TotalAttempts != 2 {
		t.Fatalf("in-flight attempts lost or duplicated: %d want 2", st2.TotalAttempts)
	}
	t.Logf("decision: resumed from durable progress; cumulative attempts=%d", st2.TotalAttempts)
}

// 崩溃发生在任务执行中（仅有非终态 attempts 记录）：重启后该任务重新执行，
// 已完成任务不重复，attempts 统计在磁盘基线之上累计。
func TestRecoveryRerunsInterruptedTask(t *testing.T) {
	store := NewMemoryStore()
	// 手工制造中断现场：a 已成功（2 次尝试），b 在第 1 次尝试中被杀（只有非终态记录）。
	if err := store.Save(TaskRecord{ID: "a", Attempts: 2, State: StateSucceeded}, StatsData{TotalAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(TaskRecord{ID: "b", Attempts: 1}, StatsData{TotalAttempts: 3}); err != nil {
		t.Fatal(err)
	}

	var bRuns int32
	s := New(Config{MaxConcurrency: 2, Store: store, Logger: testLog(t)})
	if err := s.Submit([]TaskSpec{
		{ID: "a", MaxAttempts: 3, Run: func(context.Context) error {
			t.Error("a must not run again")
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Run: func(context.Context) error {
			atomic.AddInt32(&bRuns, 1)
			return nil
		}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("input: crash-scene store: a=succeeded(2 attempts), b=interrupted(1 non-terminal attempt)")
	res, st, _ := s.Run(context.Background())
	if res["a"].State != StateSucceeded {
		t.Fatalf("a=%s", res["a"].State)
	}
	if bRuns != 1 {
		t.Fatalf("b reruns=%d want 1", bRuns)
	}
	if res["b"].State != StateSucceeded {
		t.Fatalf("b=%s want succeeded after rerun", res["b"].State)
	}
	// 基线 3（a 的 2 次 + b 的 1 次在途），b 重跑 +1 => 4。
	if st.TotalAttempts != 4 {
		t.Fatalf("cumulative attempts=%d want 4 (3 from disk + 1 rerun)", st.TotalAttempts)
	}
	t.Logf("decision: interrupted task rerun exactly once; cumulative attempts=%d", st.TotalAttempts)
}

// FileStore：原子写入保证读不到半成品；重启后状态自洽。
func TestFileStoreAtomicProgress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.json")
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.Save(TaskRecord{ID: "a", Attempts: 1, State: StateSucceeded}, StatsData{TotalAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	recs, stats, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if recs["a"].State != StateSucceeded || stats.TotalAttempts != 1 {
		t.Fatalf("load=%+v stats=%+v", recs["a"], stats)
	}
	// 目录中不应残留临时文件（原子 rename 清理）。
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	// 用同一个文件启动新调度器，模拟真实进程重启。
	var ran int32
	s := New(Config{
		MaxConcurrency: 2,
		Store:          mustFileStore(t, path),
		Logger:         testLog(t),
	})
	if err := s.Submit([]TaskSpec{
		{ID: "a", Run: func(context.Context) error { atomic.AddInt32(&ran, 1); return nil }},
		{ID: "b", Deps: []string{"a"}, Run: func(context.Context) error {
			atomic.AddInt32(&ran, 1)
			return nil
		}},
	}); err != nil {
		t.Fatal(err)
	}
	res, st, _ := s.Run(context.Background())
	if ran != 1 {
		t.Fatalf("runs after restart=%d, want only b to run", ran)
	}
	if res["a"].State != StateSucceeded || res["b"].State != StateSucceeded {
		t.Fatalf("res=%+v", res)
	}
	if st.TotalAttempts != 2 { // 磁盘上 1 + b 新执行 1
		t.Fatalf("attempts=%d want 2", st.TotalAttempts)
	}
	t.Logf("decision: file-backed restart consistent; attempts=%d", st.TotalAttempts)
}

// 并发保存下 FileStore 内容必须保持完整合法（并发恢复后状态自洽）。
func TestFileStoreConcurrentSaves(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(filepath.Join(dir, "p.json"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "t" + strconv.Itoa(i)
			for j := 0; j < 10; j++ {
				if err := fs.Save(TaskRecord{ID: id, Attempts: j + 1, State: StateSucceeded},
					StatsData{TotalAttempts: i*10 + j + 1}); err != nil {
					t.Errorf("save: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	recs, _, err := fs.Load()
	if err != nil {
		t.Fatalf("file corrupted by concurrent saves: %v", err)
	}
	if len(recs) != 32 {
		t.Fatalf("records=%d want 32 (no torn/merged writes)", len(recs))
	}
}

func mustFileStore(t *testing.T, path string) Store {
	t.Helper()
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}
