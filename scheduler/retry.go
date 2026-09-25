package scheduler

import "time"

// backoff 计算第 attempt 次重试前的等待时长（指数退避，封顶 MaxBackoff）。
// attempt 从 1 开始：第 1 次重试等待 BaseBackoff。
func (p RetryPolicy) backoff(attempt int) time.Duration {
	if p.BaseBackoff <= 0 {
		return 0
	}
	d := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d > p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// shouldRetry 报告在 attempt 次失败后是否还允许重试。
// MaxAttempts 为总尝试上限（含首次），<=0 表示不重试。
func (p RetryPolicy) shouldRetry(attempt int) bool {
	if p.MaxAttempts <= 0 {
		return false
	}
	return attempt < p.MaxAttempts
}
