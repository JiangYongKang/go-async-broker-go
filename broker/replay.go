package broker

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// LifecycleEvent 记录一条消息的一次生命周期判定（进死信 / 被重放）。
// 事件只追加不修改：重放不会抹掉历史死信记录，每次判定原因都可追溯。
type LifecycleEvent struct {
	Time      time.Time  `json:"time"`
	MessageID string     `json:"message_id"`
	Kind      string     `json:"kind"` // "dead" | "replayed"
	Reason    DeadReason `json:"reason,omitempty"`
	Error     string     `json:"error,omitempty"`
	// ReplayID：kind=replayed 时为重放请求 ID；kind=dead 且处于重放生命
	// 周期时，记录导致本次死亡的那次重放，空串表示首次生命周期内死亡。
	ReplayID string `json:"replay_id,omitempty"`
	// ReplayGen：事件发生时该消息已完成的重放次数。
	ReplayGen int `json:"replay_gen,omitempty"`
}

// ReplayRecord 是一次死信重放请求的持久化记录（先落盘后生效）。
// 崩溃恢复时按记录补齐未完成的重放，保证整批结果确定。
type ReplayRecord struct {
	Seq      uint64       `json:"seq"` // 单调序号，决定记录顺序
	ID       string       `json:"id"`
	Time     time.Time    `json:"time"`
	IDs      []string     `json:"ids"`                // 请求目标（快照顺序，按序号排序）
	Group    string       `json:"group,omitempty"`    // 非空表示按分组重放
	Replayed []string     `json:"replayed,omitempty"` // 已实际转回投递的消息
	Skipped  []ReplaySkip `json:"skipped,omitempty"`  // 未执行的目标及原因
	Done     bool         `json:"done"`
}

// ReplaySkip 说明一个重放目标为什么未执行。
type ReplaySkip struct {
	ID     string `json:"id"`
	Reason string `json:"reason"` // "not_found" | "not_dead"
}

// ReplayResult 是一次重放调用的结果快照。
type ReplayResult struct {
	ReplayID string
	Replayed []string // 实际转回投递的消息 ID
	Skipped  []ReplaySkip
}

// 生命周期事件类型。
const (
	EventDead     = "dead"     // 消息进入死信
	EventReplayed = "replayed" // 消息被重放回投递链路
)

// 重放跳过原因。
const (
	SkipNotFound = "not_found" // 消息不存在
	SkipNotDead  = "not_dead"  // 消息已不在死信状态（含已被并发重放）
)

// ReplayDead 把一批死信消息重新投入正常投递链路。
//
// 语义：
//   - 只处理当前处于 dead 状态的目标；不存在的目标记 not_found，
//     已不在死信（含被并发的另一次重放抢先处理）的目标记 not_dead，
//     两者都在结果中说明，调用不因此失败。
//   - 重放是幂等的：状态在互斥锁内判定并与重放记录同一次原子落盘，
//     同一条死信被重复或并发重放只会产生一条在途消息。
//   - 重放的消息重新获得完整的重试预算（Attempts 归零），并分配新的
//     全局序号排在所属分组队尾；它只能作为组头被投递，因此不能越过
//     同组尚未处理完（含退避等待）的消息。
//   - 容量受 MaxPending 约束：PolicyReject 下容量不足立即返回
//     ErrBrokerFull 且不留下任何记录；PolicyBlock 下等待容量释放或
//     ctx 取消。已落盘的重放意图在崩溃恢复时不受容量限制地补齐，
//     与 outbox“已提交意图不丢”的策略一致。
//   - 原死信记录不被抹掉：历史 dead 事件保留，本次转换追加 replayed
//     事件；消息再次进死信时原因标记为 replay_*，可与首次失败区分。
func (b *Broker) ReplayDead(ctx context.Context, ids []string) (*ReplayResult, error) {
	return b.replay(ctx, append([]string(nil), ids...), "")
}

// ReplayDeadGroup 把指定分组当前的全部死信重新投入投递。
// 目标集合在持锁时按序号快照确定，语义与 ReplayDead 相同。
func (b *Broker) ReplayDeadGroup(ctx context.Context, group string) (*ReplayResult, error) {
	b.mu.Lock()
	var ids []string
	for _, m := range b.state.Messages {
		if m.Group == group && m.Status == StatusDead {
			ids = append(ids, m.ID)
		}
	}
	b.sortIDsBySeqLocked(ids)
	b.mu.Unlock()
	return b.replay(ctx, ids, group)
}

// sortIDsBySeqLocked 按消息序号升序排列 ID（缺失消息排到最后）。
func (b *Broker) sortIDsBySeqLocked(ids []string) {
	sort.SliceStable(ids, func(i, j int) bool {
		mi, oi := b.state.Messages[ids[i]]
		mj, oj := b.state.Messages[ids[j]]
		switch {
		case !oi:
			return false
		case !oj:
			return true
		default:
			return mi.Seq < mj.Seq
		}
	})
}

// replay 是 ReplayDead / ReplayDeadGroup 的共同实现。
// 重放记录与消息状态在同一次原子快照中落盘：记录只在第一条消息实际
// 转换（或确定没有可转换目标）时才创建，因此“记录可见”与“转换生效”
// 要么同时发生，要么都不发生。
func (b *Broker) replay(ctx context.Context, ids []string, group string) (*ReplayResult, error) {
	result := &ReplayResult{}
	processed := map[string]bool{} // 已处理目标（转换或跳过）

	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, ErrClosed
		}
		rec := b.state.Replays[result.ReplayID]
		full := false
		// 拒绝策略：记录尚未创建时做整批容量预算。本批次在持锁期间
		// 连续完成、不会有其他分配介入，因此只要 occ + 可转换目标数
		// 不超过上限，逐次转换也不会超限；放不下就在落盘任何记录前
		// 整体拒绝（不产生半批记录）。
		if rec == nil && b.cfg.Policy == PolicyReject {
			eligible := 0
			for _, id := range ids {
				if processed[id] {
					continue
				}
				if m, ok := b.state.Messages[id]; ok && m.Status == StatusDead {
					eligible++
				}
			}
			if eligible > 0 && b.occupied()+eligible > b.cfg.MaxPending {
				b.mu.Unlock()
				return nil, ErrBrokerFull
			}
		}
		for _, id := range ids {
			if processed[id] {
				continue
			}
			m, ok := b.state.Messages[id]
			if !ok {
				b.skipLocked(rec, id, SkipNotFound, &result.Skipped)
				processed[id] = true
				continue
			}
			if m.Status != StatusDead {
				// 已被并发重放或状态已变化：不产生第二条在途消息。
				b.skipLocked(rec, id, SkipNotDead, &result.Skipped)
				processed[id] = true
				continue
			}
			if b.occupied() >= b.cfg.MaxPending {
				// 拒绝策略已通过上面的整批预算，不会走到这里；
				// 阻塞策略逐条等待容量释放。
				full = true
				break
			}
			if rec == nil {
				rec = b.newReplayRecordLocked(ids, group)
				result.ReplayID = rec.ID
			}
			// 深拷贝消息作为回滚快照后执行转换。
			snap := *m
			payload := append([]byte(nil), m.Payload...)
			b.transitionReplayLocked(m, rec.ID)
			if err := b.st.save(b.state); err != nil {
				// 落盘失败：回滚内存中的全部变更，对外视为未发生。
				b.rollbackReplayLocked(&snap, payload, rec, id, result)
				b.mu.Unlock()
				return nil, fmt.Errorf("persist replay: %w", err)
			}
			rec.Replayed = append(rec.Replayed, id)
			result.Replayed = append(result.Replayed, id)
			processed[id] = true
		}
		if full {
			b.mu.Unlock()
			if b.cfg.Policy == PolicyReject {
				return nil, ErrBrokerFull
			}
			if err := b.waitCapacity(ctx); err != nil {
				return nil, err
			}
			continue // 容量释放后重新持锁，目标状态需重新判定
		}
		if len(processed) == len(ids) {
			if rec != nil {
				rec.Done = true
				if err := b.st.save(b.state); err != nil {
					rec.Done = false
					b.mu.Unlock()
					return nil, fmt.Errorf("persist replay done: %w", err)
				}
			}
			b.mu.Unlock()
			b.signal()
			return result, nil
		}
		b.mu.Unlock()
	}
}

// newReplayRecordLocked 分配重放序号并创建记录（尚未落盘）。
func (b *Broker) newReplayRecordLocked(ids []string, group string) *ReplayRecord {
	b.state.ReplaySeq++
	rec := &ReplayRecord{
		Seq:   b.state.ReplaySeq,
		ID:    fmt.Sprintf("replay-%d", b.state.ReplaySeq),
		Time:  time.Now(),
		IDs:   append([]string(nil), ids...),
		Group: group,
	}
	b.state.Replays[rec.ID] = rec
	return rec
}

// skipLocked 登记一个被跳过的目标；若记录已创建则写入记录，
// 否则只写入本次调用结果（拒绝路径下不存在记录）。
func (b *Broker) skipLocked(rec *ReplayRecord, id, reason string, skipped *[]ReplaySkip) {
	sk := ReplaySkip{ID: id, Reason: reason}
	*skipped = append(*skipped, sk)
	if rec != nil {
		rec.Skipped = append(rec.Skipped, sk)
	}
}

// transitionReplayLocked 把一条死信消息转回投递链路：
// 分配新序号排到组尾、重置重试预算与退避、保留载荷，
// 并追加一条 replayed 生命周期事件。调用方需持锁。
func (b *Broker) transitionReplayLocked(m *Message, replayID string) {
	m.ReplayGen++
	m.OriginReplayID = replayID
	b.state.Seq++
	m.Seq = b.state.Seq
	m.Status = StatusPending
	m.Attempts = 0
	m.NotBefore = time.Time{}
	m.LastError = ""
	m.ErrorClass = ""
	m.DeadReason = ""
	b.state.Events = append(b.state.Events, LifecycleEvent{
		Time:      time.Now(),
		MessageID: m.ID,
		Kind:      EventReplayed,
		ReplayID:  replayID,
		ReplayGen: m.ReplayGen,
	})
}

// rollbackReplayLocked 在落盘失败时把内存状态恢复到转换前：
// 恢复消息字段、移除事件与重放记录、撤销已登记的转换结果。
func (b *Broker) rollbackReplayLocked(snap *Message, payload []byte, rec *ReplayRecord, id string, result *ReplayResult) {
	if cur, ok := b.state.Messages[id]; ok {
		*cur = *snap
		cur.Payload = payload
	}
	if n := len(b.state.Events); n > 0 &&
		b.state.Events[n-1].Kind == EventReplayed &&
		b.state.Events[n-1].MessageID == id {
		b.state.Events = b.state.Events[:n-1]
	}
	if len(rec.Replayed) == 0 {
		delete(b.state.Replays, rec.ID)
		b.state.ReplaySeq--
		result.ReplayID = ""
	}
	for i, r := range result.Replayed {
		if r == id {
			result.Replayed = append(result.Replayed[:i], result.Replayed[i+1:]...)
			break
		}
	}
}

// waitCapacity 阻塞等待一次状态变化信号或 ctx 取消（PolicyBlock）。
func (b *Broker) waitCapacity(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.notify:
		return nil
	}
}

// recoverReplaysLocked 在启动时幂等补齐未完成的重放记录。
// 已在 Replayed 列表中的消息（即使之后又一次进死信）不会被二次重放；
// 其余目标按记录顺序判定：仍是 dead 的执行转换，其他状态记为跳过。
// 恢复中的转换不受容量限制——重放意图在崩溃前已提交，不能因容量丢失。
func (b *Broker) recoverReplaysLocked() bool {
	var recs []*ReplayRecord
	for _, rec := range b.state.Replays {
		if !rec.Done {
			recs = append(recs, rec)
		}
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Seq < recs[j].Seq })
	changed := false
	for _, rec := range recs {
		done := map[string]bool{}
		for _, id := range rec.Replayed {
			done[id] = true
		}
		for _, id := range rec.IDs {
			if done[id] {
				continue
			}
			m, ok := b.state.Messages[id]
			switch {
			case !ok:
				rec.Skipped = append(rec.Skipped, ReplaySkip{ID: id, Reason: SkipNotFound})
			case m.Status != StatusDead:
				rec.Skipped = append(rec.Skipped, ReplaySkip{ID: id, Reason: SkipNotDead})
			default:
				b.transitionReplayLocked(m, rec.ID)
				rec.Replayed = append(rec.Replayed, id)
			}
			changed = true
		}
		rec.Done = true
		changed = true
	}
	return changed
}

// Replays 返回全部重放请求记录（按创建序号排序的快照）。
func (b *Broker) Replays() []*ReplayRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*ReplayRecord, 0, len(b.state.Replays))
	for _, rec := range b.state.Replays {
		cp := *rec
		cp.IDs = append([]string(nil), rec.IDs...)
		cp.Replayed = append([]string(nil), rec.Replayed...)
		cp.Skipped = append([]ReplaySkip(nil), rec.Skipped...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// History 返回一条消息的生命周期事件（进死信/重放），按时间顺序。
// 未知消息返回 ErrUnknownMessage。
func (b *Broker) History(id string) ([]LifecycleEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.state.Messages[id]; !ok {
		return nil, ErrUnknownMessage
	}
	var out []LifecycleEvent
	for _, e := range b.state.Events {
		if e.MessageID == id {
			out = append(out, e)
		}
	}
	return out, nil
}
