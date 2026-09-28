package broker

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ReplayMessages 按消息 ID 批量重放死信。
//
// 语义（整批原子，与既有背压策略对齐）：
//   - 仅当前处于 dead 的消息会成为本次重放目标；未知 ID 报错且整批
//     不生效；目标均已不是死信（重复调用、并发调用）返回 ErrEmptyReplay，
//     不会产生第二条在途消息；
//   - 整批要么全部生效、要么全部不生效。容量不足以放下整批时：
//     PolicyReject 立即返回与 Produce 相同的 ErrBrokerFull，死信状态、
//     序号、顺序、死信原因一律不变，也没有任何后台续跑；调用方自行决定
//     稍后整批重试或降级。PolicyBlock 阻塞到容量能容纳整批后一次性完成
//     （或 ctx 取消 / Broker 关闭）；
//   - 重放把同一条消息从 dead 置回 pending、清零本轮投递计数并分配新的
//     全局序号（按旧序号顺序连续分配，重放消息整体排到队尾），随后走与
//     正常消息完全一致的投递/确认/重试/再次进死信流程；
//   - 全部状态变更在同一个原子快照内落盘：失败时整批回滚，任何情况下
//     占用都不会超过 MaxPending。
func (b *Broker) ReplayMessages(ctx context.Context, ids []string) (*ReplayResult, error) {
	return b.replay(ctx, "", ids)
}

// ReplayGroup 按分组重放该组当前全部死信（冻结时刻的快照）。
func (b *Broker) ReplayGroup(ctx context.Context, group string) (*ReplayResult, error) {
	return b.replay(ctx, group, nil)
}

// replay 是 ReplayMessages / ReplayGroup 的统一实现。
func (b *Broker) replay(ctx context.Context, group string, ids []string) (*ReplayResult, error) {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, ErrClosed
		}
		// 冻结目标：显式 ID 列表，或该分组当前全部死信；按旧序号排序，
		// 使重放后组内相对顺序与死信发生顺序一致。
		targets, skipped, err := b.freezeTargetsLocked(group, ids)
		if err != nil {
			b.mu.Unlock()
			return nil, err
		}
		if len(targets) == 0 {
			b.mu.Unlock()
			return nil, ErrEmptyReplay
		}
		// 整批容量判定：死信不占容量，整批需要 len(targets) 个名额。
		// 任何情况下都不允许部分放入。
		if b.occupied()+len(targets) > b.cfg.MaxPending {
			if b.cfg.Policy == PolicyReject {
				// 拒绝策略：与 Produce 满容量时返回同一个错误，
				// 不创建重放意图、不消费序号、不改动任何消息。
				b.mu.Unlock()
				return nil, ErrBrokerFull
			}
			// 阻塞策略：等待容量释放（与 Produce 的阻塞完全同源），
			// 唤醒后重新冻结目标并重新做整批判定。
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-b.notify:
			case <-b.stop:
				return nil, ErrClosed
			}
			continue
		}
		replayID, err := b.applyReplayLocked(targets)
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
		b.signal()
		return &ReplayResult{
			ReplayID:  replayID,
			Group:     group,
			Total:     len(targets) + skipped,
			Replayed:  len(targets),
			Skipped:   skipped,
			Remaining: 0, // 整批原子完成：不存在"留待后台续跑"的剩余
		}, nil
	}
}

// freezeTargetsLocked 校验并冻结重放目标 ID（按旧序号升序）。
// 返回死信目标与"当前不是死信"（重复/并发调用时已被重放）被跳过的数量。
func (b *Broker) freezeTargetsLocked(group string, ids []string) ([]string, int, error) {
	var targets []string
	skipped := 0
	if ids != nil {
		seen := map[string]bool{}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			m, ok := b.state.Messages[id]
			if !ok {
				return nil, 0, fmt.Errorf("%w: %s", ErrUnknownMessage, id)
			}
			if m.Status == StatusDead {
				targets = append(targets, id)
			} else {
				skipped++
			}
		}
	} else {
		for _, m := range b.state.Messages {
			if m.Group == group && m.Status == StatusDead {
				targets = append(targets, m.ID)
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		return b.state.Messages[targets[i]].Seq < b.state.Messages[targets[j]].Seq
	})
	return targets, skipped, nil
}

// replayMsgSnapshot 是单条消息重放前的字段快照，落盘失败时整批回滚。
type replayMsgSnapshot struct {
	round      int
	seq        uint64
	status     Status
	attempts   int
	notBefore  time.Time
	deadReason DeadReason
	errorClass ErrorClass
	lastError  string
	replayID   string
	historyLen int
}

// applyReplayLocked 把冻结目标整批从 dead 置回 pending 并一次性原子落盘。
// 调用方必须已确认容量能容纳整批，且全部目标当前为 dead。
// 落盘失败时所有字段（含序号计数器）整批回滚，调用方看到失败即等价于
// 什么都没发生。返回本次重放请求的确定性 ID。调用方持锁。
func (b *Broker) applyReplayLocked(targets []string) (string, error) {
	now := time.Now()
	b.state.ReplaySeq++
	replayID := fmt.Sprintf("replay-%d", b.state.ReplaySeq)
	prevSeq := b.state.Seq
	snaps := make([]replayMsgSnapshot, len(targets))
	for i, id := range targets {
		m := b.state.Messages[id]
		snaps[i] = replayMsgSnapshot{
			round:      m.Round,
			seq:        m.Seq,
			status:     m.Status,
			attempts:   m.Attempts,
			notBefore:  m.NotBefore,
			deadReason: m.DeadReason,
			errorClass: m.ErrorClass,
			lastError:  m.LastError,
			replayID:   m.ReplayID,
			historyLen: len(m.ReplayHistory),
		}
		round := m.Round + 1
		fromSeq := m.Seq
		b.state.Seq++
		toSeq := b.state.Seq // 按旧序号顺序连续分配新序号：整批排到全局队尾
		m.Round = round
		m.Seq = toSeq // 不抢在未处理消息前
		m.Status = StatusPending
		m.Attempts = 0
		m.NotBefore = time.Time{} // 重放后立即可投递，重新获得完整重试机会
		m.DeadReason = ""
		m.ErrorClass = ""
		m.LastError = ""
		m.ReplayID = replayID
		m.ReplayHistory = append(m.ReplayHistory, ReplayEvent{
			Round: round, ReplayID: replayID,
			FromSeq: fromSeq, ToSeq: toSeq,
			PrevState: StatusDead, At: now,
		})
	}
	if err := b.st.save(b.state); err != nil {
		// 整批回滚：状态、序号、顺序、死信原因与调用前完全一致。
		for i, id := range targets {
			m := b.state.Messages[id]
			s := snaps[i]
			m.Round = s.round
			m.Seq = s.seq
			m.Status = s.status
			m.Attempts = s.attempts
			m.NotBefore = s.notBefore
			m.DeadReason = s.deadReason
			m.ErrorClass = s.errorClass
			m.LastError = s.lastError
			m.ReplayID = s.replayID
			m.ReplayHistory = m.ReplayHistory[:s.historyLen]
		}
		b.state.Seq = prevSeq
		b.state.ReplaySeq--
		return "", fmt.Errorf("persist replay: %w", err)
	}
	return replayID, nil
}
