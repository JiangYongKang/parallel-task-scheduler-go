package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TaskRecord 是单个任务的持久化记录。
type TaskRecord struct {
	ID       string        `json:"id"`
	Attempts int           `json:"attempts"`
	State    TerminalState `json:"state"`
	ErrMsg   string        `json:"err_msg,omitempty"`
}

// StatsData 是需要跨进程存活的汇总统计（在途统计不丢失）。
type StatsData struct {
	TotalAttempts int `json:"total_attempts"`
}

// snapshot 是磁盘文件的完整内容。
type snapshot struct {
	Version int                   `json:"version"`
	Records map[string]TaskRecord `json:"records"`
	Stats   StatsData             `json:"stats"`
}

// Store 是进度持久化接口。
type Store interface {
	Load() (map[string]TaskRecord, StatsData, error)
	Save(record TaskRecord, stats StatsData) error
	Close() error
}

// MemoryStore 是仅用于测试/内存模式的 Store 实现。
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]TaskRecord
	stats   StatsData
	closed  bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: map[string]TaskRecord{}}
}

func (s *MemoryStore) Load() (map[string]TaskRecord, StatsData, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]TaskRecord, len(s.records))
	for k, v := range s.records {
		out[k] = v
	}
	return out, s.stats, nil
}

func (s *MemoryStore) Save(record TaskRecord, stats StatsData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSchedulerClosed
	}
	s.records[record.ID] = record
	s.stats = stats
	return nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// FileStore 将进度以 JSON 原子写入单个文件（临时文件 + rename），
// 保证进程中断后重启看到的要么是旧版本、要么是完整新版本，不会读到半写文件。
type FileStore struct {
	mu sync.Mutex
	fn string
}

// NewFileStore 创建/打开文件存储；文件不存在时视为空白进度。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty store path", ErrInvalidSpec)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &FileStore{fn: path}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := s.writeLocked(snapshot{Version: 1, Records: map[string]TaskRecord{}, Stats: StatsData{}}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *FileStore) Load() (map[string]TaskRecord, StatsData, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.fn)
	if err != nil {
		return nil, StatsData{}, err
	}
	var snap snapshot
	if len(data) == 0 {
		return map[string]TaskRecord{}, StatsData{}, nil
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, StatsData{}, fmt.Errorf("corrupt progress file: %w", err)
	}
	if snap.Records == nil {
		snap.Records = map[string]TaskRecord{}
	}
	return snap.Records, snap.Stats, nil
}

func (s *FileStore) Save(record TaskRecord, stats StatsData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{Version: 1, Records: map[string]TaskRecord{}, Stats: stats}
	// 先读回既有记录再合并，使 Store 接口实现本身在多写者下也是自洽的。
	if data, err := os.ReadFile(s.fn); err == nil && len(data) > 0 {
		var old snapshot
		if err := json.Unmarshal(data, &old); err == nil && old.Records != nil {
			snap.Records = old.Records
		}
	}
	snap.Records[record.ID] = record
	snap.Stats = stats
	return s.writeLocked(snap)
}

// writeLocked 先写临时文件再 fsync + rename，调用方须持锁。
func (s *FileStore) writeLocked(snap snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.fn)
	tmp, err := os.CreateTemp(dir, ".progress-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.fn); err != nil {
		cleanup()
		return err
	}
	return nil
}

func (s *FileStore) Close() error { return nil }
