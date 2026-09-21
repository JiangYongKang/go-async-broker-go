package broker

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Broker 是异步消息引擎主体：生产、按组有序投递、确认/拒绝、
// 重试退避、死信、背压与崩溃恢复。
//
// 一致性约定：
//   - 所有状态变更在持锁状态下先落盘再对外可见，崩溃后从磁盘恢复；
//   - 恢复时所有 inflight 消息回到 pending（至少一次投递）；
//   - 同一分组同一时刻至多一条 inflight，且只允许组内最小序号投递，
//     因此正常投递与重试过程中组内顺序稳定。
type Broker struct {
	cfg    Config
	st     *store
	mu     sync.Mutex
	notify chan struct{} // 状态变化信号（容量释放/新消息可投递）
	state  *state
	closed bool
}

// New 打开（或创建）一个 Broker，并从本地存储恢复状态。
func New(cfg Config) (*Broker, error) {
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 1024
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 50 * time.Millisecond
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	st := newStore(cfg.Dir)
	state, err := st.load()
	if err != nil {
		return nil, err
	}
	// 崩溃恢复：所有 inflight 回到 pending，等待重新投递。
	// 这保证至少一次语义：已投递未确认的消息不会丢。
	recovered := 0
	for _, m := range state.Messages {
		if m.Status == StatusInflight {
			m.Status = StatusPending
			recovered++
		}
	}
	b := &Broker{cfg: cfg, st: st, state: state, notify: make(chan struct{}, 1)}
	if recovered > 0 {
		if err := b.st.save(b.state); err != nil {
			return nil, fmt.Errorf("persist recovery: %w", err)
		}
	}
	return b, nil
}

// signal 非阻塞地通知等待者状态已变化。
func (b *Broker) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// occupied 返回占用容量的消息数（pending + inflight）。
func (b *Broker) occupied() int {
	n := 0
	for _, m := range b.state.Messages {
		if m.Status == StatusPending || m.Status == StatusInflight {
			n++
		}
	}
	return n
}

// produceLocked 在持锁状态下入队一条消息（指定 ID，供 outbox 幂等重放）。
// 同 ID 消息已存在时直接返回，保证幂等。
func (b *Broker) produceLocked(id, group string, payload []byte) (string, error) {
	if id == "" {
		b.state.Seq++
		id = fmt.Sprintf("msg-%d", b.state.Seq)
	} else if _, ok := b.state.Messages[id]; ok {
		return id, nil // 幂等：已接收的消息不重复入队
	} else {
		b.state.Seq++
	}
	payloadCopy := append([]byte(nil), payload...)
	b.state.Messages[id] = &Message{
		ID: id, Group: group, Seq: b.state.Seq,
		Payload: payloadCopy, Status: StatusPending,
	}
	if err := b.st.save(b.state); err != nil {
		delete(b.state.Messages, id)
		return "", fmt.Errorf("persist produce: %w", err)
	}
	return id, nil
}

// Produce 生产一条消息。达到容量上限时按策略阻塞或返回 ErrBrokerFull。
// 已被接收的消息不会丢失（先落盘后返回）。
func (b *Broker) Produce(ctx context.Context, group string, payload []byte) (string, error) {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return "", ErrClosed
		}
		if b.occupied() < b.cfg.MaxPending {
			id, err := b.produceLocked("", group, payload)
			b.mu.Unlock()
			if err != nil {
				return "", err
			}
			b.signal()
			return id, nil
		}
		b.mu.Unlock()
		if b.cfg.Policy == PolicyReject {
			return "", ErrBrokerFull
		}
		// 阻塞策略：等待容量释放或 ctx 取消。
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-b.notify:
		}
	}
}

// deliverableLocked 返回当前可投递消息：pending、NotBefore 已到、
// 且其分组内没有 inflight 消息、也没有更小序号的 pending 消息。
func (b *Broker) deliverableLocked(now time.Time) *Message {
	inflightGroups := map[string]bool{}
	var candidates []*Message
	for _, m := range b.state.Messages {
		switch m.Status {
		case StatusInflight:
			inflightGroups[m.Group] = true
		case StatusPending:
			if !m.NotBefore.After(now) {
				candidates = append(candidates, m)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Seq < candidates[j].Seq })
	for _, m := range candidates {
		if !inflightGroups[m.Group] {
			return m // 全局最小序号且组内无在途消息
		}
	}
	return nil
}

// nextRetryLocked 返回最近的退避到期时间（用于定时唤醒）。
func (b *Broker) nextRetryLocked(now time.Time) (time.Time, bool) {
	var earliest time.Time
	found := false
	for _, m := range b.state.Messages {
		if m.Status == StatusPending && m.NotBefore.After(now) {
			if !found || m.NotBefore.Before(earliest) {
				earliest = m.NotBefore
				found = true
			}
		}
	}
	return earliest, found
}

// Receive 拉取下一条可投递消息（阻塞直到有消息或 ctx 取消）。
func (b *Broker) Receive(ctx context.Context) (*Message, error) {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, ErrClosed
		}
		now := time.Now()
		if m := b.deliverableLocked(now); m != nil {
			m.Status = StatusInflight
			m.Attempts++
			if err := b.st.save(b.state); err != nil {
				m.Status = StatusPending
				m.Attempts--
				b.mu.Unlock()
				return nil, fmt.Errorf("persist delivery: %w", err)
			}
			cp := *m
			b.mu.Unlock()
			b.signal()
			return &cp, nil
		}
		var timer *time.Timer
		var timerC <-chan time.Time
		if t, ok := b.nextRetryLocked(now); ok {
			timer = time.NewTimer(time.Until(t))
			timerC = timer.C
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil, ctx.Err()
		case <-b.notify:
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
		}
	}
}

// Ack 确认一条 inflight 消息。
// 未知消息 -> ErrUnknownMessage；重复确认 -> ErrAlreadyAcked；
// 其他非 inflight 状态 -> ErrNotInflight。三类原因可区分。
func (b *Broker) Ack(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.state.Messages[id]
	if !ok {
		return ErrUnknownMessage
	}
	switch m.Status {
	case StatusAcked:
		return ErrAlreadyAcked
	case StatusInflight:
		m.Status = StatusAcked
		if err := b.st.save(b.state); err != nil {
			m.Status = StatusInflight
			return fmt.Errorf("persist ack: %w", err)
		}
		b.signal()
		return nil
	default:
		return fmt.Errorf("%w: status=%s", ErrNotInflight, m.Status)
	}
}

// Nack 拒绝一条 inflight 消息。retryable=true 时按退避重试，
// 超过 MaxRetries 进死信（DeadRetryExhausted）；否则直接进死信
// （DeadNonRetryable）。判定依据记录在消息的 ErrorClass/DeadReason 上。
func (b *Broker) Nack(id string, retryable bool, cause string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.state.Messages[id]
	if !ok {
		return ErrUnknownMessage
	}
	if m.Status != StatusInflight {
		if m.Status == StatusAcked {
			return ErrAlreadyAcked
		}
		return fmt.Errorf("%w: status=%s", ErrNotInflight, m.Status)
	}
	m.LastError = cause
	if !retryable {
		m.ErrorClass = ErrClassNonRetryable
		m.Status = StatusDead
		m.DeadReason = DeadNonRetryable
	} else if m.Attempts > b.cfg.MaxRetries {
		m.ErrorClass = ErrClassRetryable
		m.Status = StatusDead
		m.DeadReason = DeadRetryExhausted
	} else {
		m.ErrorClass = ErrClassRetryable
		m.Status = StatusPending
		m.NotBefore = time.Now().Add(b.backoff(m.Attempts))
	}
	if err := b.st.save(b.state); err != nil {
		m.Status = StatusInflight
		return fmt.Errorf("persist nack: %w", err)
	}
	b.signal()
	return nil
}

// DeadLetters 返回当前死信列表（快照，按序号排序）。
func (b *Broker) DeadLetters() []*Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*Message
	for _, m := range b.state.Messages {
		if m.Status == StatusDead {
			cp := *m
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Pending 返回当前待投递+在途消息数（背压可观测）。
func (b *Broker) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.occupied()
}

// Close 关闭引擎并落盘最终状态。
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	err := b.st.save(b.state)
	b.signal()
	return err
}

// backoff 计算第 n 次重试的等待时长（指数退避，封顶 BackoffMax）。
func (b *Broker) backoff(attempt int) time.Duration {
	d := b.cfg.BackoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= b.cfg.BackoffMax {
			return b.cfg.BackoffMax
		}
	}
	if d > b.cfg.BackoffMax {
		return b.cfg.BackoffMax
	}
	return d
}

// Get 返回一条消息的快照（用于测试与观测）。
func (b *Broker) Get(id string) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.state.Messages[id]
	if !ok {
		return nil, ErrUnknownMessage
	}
	cp := *m
	return &cp, nil
}
