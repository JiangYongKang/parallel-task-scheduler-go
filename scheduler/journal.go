package scheduler

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// journal 是追加式进度日志：每次任务状态迁移都先落盘再生效，
// 进程中断后重启可从中恢复，既不重复执行已完成任务，
// 也不丢失在途统计（attempts 计数随记录持久化）。
type journal struct {
	mu   sync.Mutex
	path string
	f    *os.File
	enc  *json.Encoder
	logf func(format string, args ...any) // 可选，恢复时报告不完整尾部
}

// openJournal 打开（不存在则创建）日志文件。
// path 为空时使用内存态（不落盘），便于测试与纯内存调度。
func openJournal(path string) (*journal, error) {
	j := &journal{path: path}
	if path == "" {
		return j, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	j.f = f
	j.enc = json.NewEncoder(f)
	return j, nil
}

// append 追加一条进度记录并 fsync，保证崩溃后可恢复。
func (j *journal) append(id string, st Status, attempts int) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	rec := Record{ID: id, Status: st, Attempts: attempts, UpdatedAt: time.Now().UTC()}
	if err := j.enc.Encode(&rec); err != nil {
		return err
	}
	return j.f.Sync()
}

// load 读取日志，返回每个任务的最新记录（后写覆盖先写）。
//
// 崩溃兼容：进程可能在写记录的中途被强杀，文件尾部因此留下不完整
// （截断/半行）的数据。追加式日志一旦解码失败就无法重新同步，
// 此时丢弃不完整的尾部、用已成功解码的前缀记录恢复，
// 不让整次恢复因此失败。空 ID 的残缺记录同样跳过。
func (j *journal) load() (map[string]Record, error) {
	out := make(map[string]Record)
	if j.path == "" {
		return out, nil
	}
	f, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var rec Record
		err := dec.Decode(&rec)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if j.logf != nil {
				j.logf("journal: dropping incomplete tail after crash mid-write: %v", err)
			}
			break
		}
		if rec.ID == "" {
			if j.logf != nil {
				j.logf("journal: skipping record with empty id")
			}
			continue
		}
		out[rec.ID] = rec
	}
	return out, nil
}

func (j *journal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		return j.f.Close()
	}
	return nil
}
