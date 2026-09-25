package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Logger 是调度器使用的最小日志接口（打印调度输入与判定依据）。
type Logger interface {
	Printf(format string, args ...any)
}

// Config 控制调度器的全局行为。
type Config struct {
	MaxConcurrency int // worker 数量；也是 Slots 默认上限
	TotalSlots     int // 可选：覆盖资源池槽位上限（可小于 worker 数以制造槽位背压）
	NamedResources map[string]int
	Store          Store // 为空时使用 MemoryStore
	Logger         Logger
	Now            func() time.Time
}

// Stats 是调度完成后的汇总统计。
type Stats struct {
	Total         int
	Succeeded     int
	Failed        int
	Canceled      int
	TimedOut      int
	Skipped       int
	TotalAttempts int // 跨重启累计的执行尝试次数（在途统计不丢失）
}

// Result 是单个任务的最终结果。
type Result struct {
	ID       string
	State    TerminalState
	Attempts int
	Err      error
}

// taskPhase 是任务的内部生命周期阶段。
type taskPhase int

const (
	phasePending taskPhase = iota
	phaseReady
	phaseRunning
	phaseTerminal
)

type taskRT struct {
	spec     *TaskSpec
	phase    taskPhase
	attempts int
	state    TerminalState
	errMsg   string
}

// Scheduler 是并发任务调度引擎。
type Scheduler struct {
	cfg   Config
	log   Logger
	pool  *ResourcePool
	store Store
	now   func() time.Time

	mu            sync.Mutex
	submitted     bool
	dag           *DAG
	tasks         map[string]*taskRT
	ready         []string
	cond          *sync.Cond // 等待 ready 队列/终止变化
	wg            sync.WaitGroup
	totalAttempts int64
	closed        bool
}

// New 创建调度器。
func New(cfg Config) *Scheduler {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Scheduler{
		cfg:   cfg,
		log:   cfg.Logger,
		store: cfg.Store,
		now:   cfg.Now,
		tasks: map[string]*taskRT{},
	}
	if s.log == nil {
		s.log = nopLogger{}
	}
	slots := cfg.TotalSlots
	if slots <= 0 {
		slots = cfg.MaxConcurrency
	}
	s.cond = sync.NewCond(&s.mu)
	s.pool = NewResourcePool(slots, cfg.NamedResources)
	return s
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Submit 校验并登记任务集合，只能调用一次。
func (s *Scheduler) Submit(specs []TaskSpec) error {
	limits := Resources{Slots: s.pool.slots, Named: s.cfg.NamedResources}
	dag, err := BuildDAG(specs, limits)
	if err != nil {
		s.log.Printf("submit rejected: input=%d tasks: %v", len(specs), err)
		return err
	}

	records, stats, err := s.store.Load()
	if err != nil {
		return fmt.Errorf("load progress: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.submitted {
		return ErrAlreadySubmitted
	}
	s.submitted = true
	s.dag = dag
	atomic.StoreInt64(&s.totalAttempts, int64(stats.TotalAttempts))

	for _, id := range dag.order {
		rt := &taskRT{spec: dag.spec(id)}
		if rec, ok := records[id]; ok && rec.State.IsTerminal() {
			// 恢复：已终态任务不再执行，副作用不重复。
			rt.phase = phaseTerminal
			rt.state = rec.State
			rt.attempts = rec.Attempts
			rt.errMsg = rec.ErrMsg
		}
		s.tasks[id] = rt
	}
	s.log.Printf("submit accepted: tasks=%d recovered=%d priorAttempts=%d deps=%v",
		dag.TaskCount(), len(records), stats.TotalAttempts, describeDeps(dag))
	return nil
}

// Run 执行全部任务直到全部到达终态或 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) (map[string]Result, Stats, error) {
	s.mu.Lock()
	if !s.submitted {
		s.mu.Unlock()
		return nil, Stats{}, ErrInvalidSpec
	}
	s.mu.Unlock()

	// 根 ctx 取消时广播，唤醒等待 ready/资源的 worker。
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	n := s.cfg.MaxConcurrency
	s.log.Printf("run start: workers=%d resources(slots=%d,named=%v)", n, s.pool.slots, s.cfg.NamedResources)

	// 先在持锁状态下完成初始就绪判定并启动 worker，
	// 保证任何 worker 都不会在就绪任务入队前因"无事可做"而提前退出。
	s.mu.Lock()
	for _, id := range s.dag.order {
		rt := s.tasks[id]
		if rt.phase == phaseTerminal {
			s.log.Printf("decision: task=%s recovered-state=%s => skip execution", id, rt.state)
			continue
		}
		if s.depsMet(id) {
			s.enqueueLocked(id, "all dependencies succeeded")
		}
	}
	for i := 0; i < n; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i)
	}
	s.mu.Unlock()

	s.wg.Wait()

	// 兜底：ctx 取消后仍 pending/ready 的任务给出可区分终态。
	s.mu.Lock()
	for _, id := range s.dag.order {
		rt := s.tasks[id]
		if rt.phase != phaseTerminal {
			state := StateSkipped
			detail := "never scheduled: run canceled before start"
			if ctx.Err() == nil {
				state = StateSkipped
				detail = "never scheduled: upstream did not succeed"
			}
			s.markTerminalLocked(rt, state, detail)
			s.propagateLocked(rt, state)
		}
	}
	s.mu.Unlock()

	return s.resultsLocked(ctx)
}

// depsMet 报告依赖是否全部处于成功终态（调用方持锁）。
func (s *Scheduler) depsMet(id string) bool {
	for _, dep := range s.dag.deps[id] {
		if s.tasks[dep].state != StateSucceeded {
			return false
		}
	}
	return true
}

func (s *Scheduler) enqueueLocked(id, reason string) {
	rt := s.tasks[id]
	if rt.phase != phasePending {
		return
	}
	rt.phase = phaseReady
	s.ready = append(s.ready, id)
	s.log.Printf("decision: task=%s ready (%s)", id, reason)
	s.cond.Broadcast()
}

func (s *Scheduler) worker(ctx context.Context, wid int) {
	defer s.wg.Done()
	for {
		var rt *taskRT
		s.mu.Lock()
		for {
			for len(s.ready) == 0 {
				if s.allSettledLocked() {
					s.mu.Unlock()
					return
				}
				s.cond.Wait()
			}
			// 取队首：保持就绪顺序，确定可复现。
			id := s.ready[0]
			s.ready = s.ready[1:]
			cand := s.tasks[id]
			if cand.phase == phaseReady {
				// 排队期间可能已被失败传播标记为终态，那种任务跳过不执行。
				cand.phase = phaseRunning
				rt = cand
				break
			}
			// 丢弃已终态的陈旧条目，继续取下一个。
			if s.allSettledLocked() {
				s.mu.Unlock()
				return
			}
		}
		id := rt.spec.ID
		s.mu.Unlock()

		s.log.Printf("dispatch: worker=%d task=%s resources=%+v", wid, id, rt.spec.Resources)
		s.execute(ctx, rt)
	}
}

// allSettledLocked：没有可运行任务且没有在途任务时，调度结束。
func (s *Scheduler) allSettledLocked() bool {
	for _, rt := range s.tasks {
		if rt.phase == phaseRunning {
			return false
		}
	}
	return len(s.ready) == 0
}

// execute 执行单个任务：资源准入（背压）、重试退避、超时与取消传播。
func (s *Scheduler) execute(parent context.Context, rt *taskRT) {
	spec := rt.spec

	// 资源准入：额度不足时阻塞等待，不丢弃、不忽略取消。
	if err := s.pool.Acquire(parent, spec.Resources); err != nil {
		s.mu.Lock()
		state := StateCanceled
		if parent.Err() == context.DeadlineExceeded {
			state = StateTimedOut
		}
		s.finishLocked(rt, state, "canceled while waiting for resources: "+err.Error())
		s.mu.Unlock()
		return
	}
	defer s.pool.Release(spec.Resources)

	maxAttempts := spec.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 父 ctx 已取消则不再发起新尝试。
		if err := parent.Err(); err != nil {
			s.mu.Lock()
			state := StateCanceled
			if err == context.DeadlineExceeded {
				state = StateTimedOut
			}
			s.finishLocked(rt, state, "canceled before attempt: "+err.Error())
			s.mu.Unlock()
			return
		}

		rt.attempts = attempt
		atomic.AddInt64(&s.totalAttempts, 1)
		s.persistAttempt(rt)

		taskCtx, cancelTask := context.WithCancel(parent)
		if spec.Timeout > 0 {
			taskCtx, cancelTask = context.WithTimeout(parent, spec.Timeout)
		}

		start := s.now()
		s.log.Printf("attempt: task=%s n=%d/%d started_at=%s", spec.ID, attempt, maxAttempts, start.Format(time.RFC3339Nano))
		err := spec.Run(taskCtx)
		// 必须在 cancelTask 之前捕获上下文状态，否则主动 cancel 会污染判定。
		ctxErr := taskCtx.Err()
		parentErr := parent.Err()
		cancelTask()
		elapsed := s.now().Sub(start)

		// 判定依据：任务自身错误优先；否则区分父取消与任务超时。
		switch {
		case err == nil && ctxErr == nil:
			s.mu.Lock()
			s.finishLocked(rt, StateSucceeded, "")
			s.mu.Unlock()
			s.log.Printf("attempt: task=%s n=%d SUCCEEDED elapsed=%s", spec.ID, attempt, elapsed)
			return
		default:
			lastErr = classifyErr(err, ctxErr, parentErr)
			s.log.Printf("attempt: task=%s n=%d failed elapsed=%s cause=%v", spec.ID, attempt, elapsed, lastErr)
		}

		// 取消/超时属于不可重试终态，直接向下游传播。
		if isCancellation(lastErr) {
			s.mu.Lock()
			state := StateCanceled
			if parentErr == context.DeadlineExceeded || ctxErr == context.DeadlineExceeded {
				state = StateTimedOut
			}
			s.finishLocked(rt, state, lastErr.Error())
			s.mu.Unlock()
			return
		}

		// 普通瞬时失败：有剩余次数则退避后重试。
		if attempt < maxAttempts {
			d := backoff(spec.Backoff, attempt)
			s.log.Printf("retry: task=%s backing_off=%s then attempt=%d", spec.ID, d, attempt+1)
			if !sleep(parent, d) {
				s.mu.Lock()
				state := StateCanceled
				if parent.Err() == context.DeadlineExceeded {
					state = StateTimedOut
				}
				s.finishLocked(rt, state, "canceled during backoff: "+parent.Err().Error())
				s.mu.Unlock()
				return
			}
		}
	}

	s.mu.Lock()
	s.finishLocked(rt, StateFailed, fmt.Sprintf("exhausted %d attempts: %v", maxAttempts, lastErr))
	s.mu.Unlock()
}

// finishLocked 将任务置为终态、持久化并传播到下游（调用方持 s.mu）。
func (s *Scheduler) finishLocked(rt *taskRT, state TerminalState, msg string) {
	if rt.phase == phaseTerminal {
		return
	}
	s.markTerminalLocked(rt, state, msg)
	s.propagateLocked(rt, state)
}

// markTerminalLocked 只负责写状态与持久化，不做传播。
func (s *Scheduler) markTerminalLocked(rt *taskRT, state TerminalState, msg string) {
	rt.phase = phaseTerminal
	rt.state = state
	rt.errMsg = msg
	rec := TaskRecord{ID: rt.spec.ID, Attempts: rt.attempts, State: state, ErrMsg: msg}
	stats := StatsData{TotalAttempts: int(atomic.LoadInt64(&s.totalAttempts))}
	if err := s.store.Save(rec, stats); err != nil {
		s.log.Printf("warn: persist task=%s failed: %v", rt.spec.ID, err)
	}
	s.log.Printf("terminal: task=%s state=%s attempts=%d detail=%q", rt.spec.ID, state, rt.attempts, msg)
	s.cond.Broadcast()
}

// propagateLocked：某任务非成功终态后，把后继任务标记为 skipped（带原因），
// 并级联；已在运行/已终态的不受影响。成功终态则尝试解锁后继就绪。
func (s *Scheduler) propagateLocked(rt *taskRT, state TerminalState) {
	id := rt.spec.ID
	if state == StateSucceeded {
		for _, ch := range s.dag.child[id] {
			child := s.tasks[ch]
			if child.phase == phasePending && s.depsMet(ch) {
				s.enqueueLocked(ch, fmt.Sprintf("dependency %s succeeded", id))
			}
		}
		return
	}
	reason := fmt.Sprintf("upstream %s is %s", id, state)
	var walk func(string)
	walk = func(dep string) {
		for _, ch := range s.dag.child[dep] {
			child := s.tasks[ch]
			if child.phase == phasePending || child.phase == phaseReady {
				s.markTerminalLocked(child, StateSkipped, reason)
				walk(ch)
			}
		}
	}
	walk(id)
}

func (s *Scheduler) persistAttempt(rt *taskRT) {
	stats := StatsData{TotalAttempts: int(atomic.LoadInt64(&s.totalAttempts))}
	// 以非终态记录 attempts，仅用于在途统计；恢复时不被当作已完成。
	rec := TaskRecord{ID: rt.spec.ID, Attempts: rt.attempts}
	if err := s.store.Save(rec, stats); err != nil {
		s.log.Printf("warn: persist attempt task=%s failed: %v", rt.spec.ID, err)
	}
}

func (s *Scheduler) resultsLocked(_ context.Context) (map[string]Result, Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Result, len(s.tasks))
	var st Stats
	st.Total = len(s.tasks)
	st.TotalAttempts = int(atomic.LoadInt64(&s.totalAttempts))
	for _, id := range s.dag.order {
		rt := s.tasks[id]
		var err error
		if rt.errMsg != "" {
			err = fmt.Errorf("%s", rt.errMsg)
		}
		out[id] = Result{ID: id, State: rt.state, Attempts: rt.attempts, Err: err}
		switch rt.state {
		case StateSucceeded:
			st.Succeeded++
		case StateFailed:
			st.Failed++
		case StateCanceled:
			st.Canceled++
		case StateTimedOut:
			st.TimedOut++
		case StateSkipped:
			st.Skipped++
		}
	}
	s.log.Printf("run finished: stats=%+v", st)
	return out, st, nil
}

// ---- helpers ----

func classifyErr(runErr, ctxErr, parentErr error) error {
	if runErr != nil {
		return runErr
	}
	if ctxErr != nil {
		return ctxErr
	}
	if parentErr != nil {
		return parentErr
	}
	return fmt.Errorf("task failed without error")
}

func isCancellation(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

// backoff 采用线性退避 attempt*base（base=0 时立即重试）。
func backoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	return time.Duration(attempt) * base
}

// sleep 在等待退避时响应取消。
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func describeDeps(d *DAG) []string {
	out := make([]string, 0, len(d.order))
	for _, id := range d.order {
		if len(d.deps[id]) > 0 {
			out = append(out, fmt.Sprintf("%s<-%v", id, d.deps[id]))
		}
	}
	sort.Strings(out)
	return out
}
