package broker

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ReplayMessages 按消息 ID 批量重放死信。
//
// 整批原子语义（与既有背压策略对齐）：
//   - 仅当前处于 dead 的目标会被重放；未知 ID 返回 ErrUnknownMessage 且
//     整批不生效；目标均已不是死信（重复调用、并发调用被抢先复活）时
//     返回 ErrEmptyReplay，不会产生第二条在途消息；
//   - PolicyReject：整批放得下才重放，放不下立即返回 ErrBrokerFull，
//     与 Produce 满容时返回的是同一个错误；死信状态、序号、顺序、
//     死信原因一律不变，也不会留下任何"后台续跑"；
//   - PolicyBlock：等待到容量足以容纳整批后一次性完成（或 ctx 取消/
//     Broker 关闭）；等待期间被并发调用抢先复活的目标计入 Skipped；
//   - 成功结果中 Replayed+Skipped==Total、Remaining 恒为 0；
//   - 整批状态变更在同一个原子快照内落盘，任何时刻崩溃都只会停在
//     "整批未生效"或"整批已生效"两个点上，不会出现部分重放。
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
		// 等待容量期间，目标可能已被并发的重放调用复活：重新按状态分类，
		// 已不在死信列表的目标计入幂等跳过，绝不复活第二次。
		dead := targets[:0]
		for _, id := range targets {
			if b.state.Messages[id].Status == StatusDead {
				dead = append(dead, id)
			} else {
				skipped++
			}
		}
		if len(dead) == 0 {
			b.mu.Unlock()
			return nil, ErrEmptyReplay
		}
		// 容量预检按整批计算：dead 条消息复活后全部计入 pending，
		// 因此只有 occupied+len(dead) <= MaxPending 才允许整批生效。
		if b.occupied()+len(dead) > b.cfg.MaxPending {
			if b.cfg.Policy == PolicyReject {
				b.mu.Unlock()
				return nil, ErrBrokerFull
			}
			wait := b.waitToken()
			b.mu.Unlock()
			// PolicyBlock：等待容量释放（Ack/重放消息死亡等都会广播），
			// 醒来后重新冻结并重新分类，直到整批放得下。
			select {
			case <-ctx.Done():
				return nil, ctx.Err() // 未做任何状态变更
			case <-wait:
			}
			continue
		}
		res, err := b.applyReplayBatchLocked(group, dead, skipped)
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
		b.signal()
		return res, nil
	}
}

// freezeTargetsLocked 校验并冻结重放目标 ID（按旧序号升序）。
// 返回冻结时刻处于死信的目标，与"当前不是死信"（重复/并发调用时已被
// 复活）被跳过的数量。调用方持锁。
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

// replaySnapshot 保存一条消息重放前的字段，供落盘失败时整批回滚。
type replaySnapshot struct {
	round      int
	seq        uint64
	status     Status
	attempts   int
	notBefore  time.Time
	lastError  string
	errorClass ErrorClass
	deadReason DeadReason
	replayID   string
	historyLen int
}

// applyReplayBatchLocked 把整批仍处于 dead 的目标一次性复活，并在同一个
// 原子快照内落盘：要么整批生效，要么落盘失败时整批回滚、什么都不发生。
// 调用方持锁，且必须已确认 occupied+len(dead) <= MaxPending。
func (b *Broker) applyReplayBatchLocked(group string, dead []string, skipped int) (*ReplayResult, error) {
	b.state.ReplaySeq++
	replayID := fmt.Sprintf("replay-%d", b.state.ReplaySeq)
	now := time.Now()
	prevSeq := b.state.Seq

	snaps := make([]replaySnapshot, len(dead))
	for i, id := range dead {
		m := b.state.Messages[id]
		snaps[i] = replaySnapshot{
			round:      m.Round,
			seq:        m.Seq,
			status:     m.Status,
			attempts:   m.Attempts,
			notBefore:  m.NotBefore,
			lastError:  m.LastError,
			errorClass: m.ErrorClass,
			deadReason: m.DeadReason,
			replayID:   m.ReplayID,
			historyLen: len(m.ReplayHistory),
		}
		fromSeq := m.Seq
		b.state.Seq++
		toSeq := b.state.Seq
		m.Round++
		m.Seq = toSeq // 分配新序号：按旧序号顺序依次排到所属分组（全局）队尾
		m.Status = StatusPending
		m.Attempts = 0
		m.NotBefore = time.Time{} // 重放后立即可投递，重新获得完整重试机会
		m.DeadReason = ""
		m.ErrorClass = ""
		m.LastError = ""
		m.ReplayID = replayID
		m.ReplayHistory = append(m.ReplayHistory, ReplayEvent{
			Round: m.Round, ReplayID: replayID,
			FromSeq: fromSeq, ToSeq: toSeq,
			PrevState: StatusDead, At: now,
		})
	}

	if err := b.st.save(b.state); err != nil {
		// 整批回滚：恢复每条消息的全部字段与全局序号，对外等价于未调用。
		for i, id := range dead {
			m := b.state.Messages[id]
			s := snaps[i]
			m.Round = s.round
			m.Seq = s.seq
			m.Status = s.status
			m.Attempts = s.attempts
			m.NotBefore = s.notBefore
			m.LastError = s.lastError
			m.ErrorClass = s.errorClass
			m.DeadReason = s.deadReason
			m.ReplayID = s.replayID
			m.ReplayHistory = m.ReplayHistory[:s.historyLen]
		}
		b.state.Seq = prevSeq
		b.state.ReplaySeq-- // 失败的调用不占用 ReplayID
		return nil, fmt.Errorf("persist replay: %w", err)
	}

	return &ReplayResult{
		ReplayID:  replayID,
		Group:     group,
		Total:     len(dead) + skipped,
		Replayed:  len(dead),
		Skipped:   skipped,
		Remaining: 0,
	}, nil
}
