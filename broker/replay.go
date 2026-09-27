package broker

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// replayRequest 是一个持久化的死信重放请求（意图）。
// 创建时冻结目标消息 ID（按旧序号排序），随后在容量允许时分块推进；
// 进程在任意时刻崩溃，重启后由后台泵按相同冻结目标幂等续跑。
type replayRequest struct {
	ID      string          `json:"id"`
	Group   string          `json:"group,omitempty"`
	Targets []string        `json:"targets"`
	Applied map[string]bool `json:"applied"`
	Done    bool            `json:"done"`
}

// ReplayMessages 按消息 ID 批量重放死信。
//
// 语义：
//   - 仅当前处于 dead 的消息会被冻结进本次重放请求；未知 ID 报错且整批
//     不生效；目标均已不是死信（重复调用、并发调用）返回 ErrEmptyReplay，
//     不会产生第二条在途消息；
//   - 重放把同一条消息从 dead 置回 pending、清零本轮投递计数并分配新的
//     全局序号（排到所属分组队尾），随后走与正常消息完全一致的
//     投递/确认/重试/再次进死信流程；
//   - 容量：PolicyBlock 下阻塞至全部重放完成（或 ctx 取消）；
//     PolicyReject 下先重放当前容量内的一批，其余持久化后由后台泵在
//     容量释放时自动续跑，返回 Remaining；
//   - 重放意图先落盘再改状态（同一原子快照），崩溃后续跑严格幂等：
//     同一条消息不会重放两次，也不会丢。
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
		req, n := b.createAndApplyLocked(group, targets)
		b.mu.Unlock()
		b.signal()
		res := &ReplayResult{
			ReplayID:  req.ID,
			Group:     group,
			Total:     len(targets) + skipped,
			Skipped:   skipped,
			Replayed:  n,
			Remaining: len(req.Targets) - n,
		}
		if b.cfg.Policy == PolicyReject || res.Remaining == 0 {
			return res, nil
		}
		// PolicyBlock：阻塞直到后台泵（或本循环下一轮）推完整个请求。
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-b.notify:
		case <-b.pumpStop:
			return res, ErrClosed
		}
		for {
			b.mu.Lock()
			cur := b.state.Replays[req.ID]
			doneCount := 0
			if cur != nil {
				doneCount = len(cur.Applied)
			} else {
				doneCount = res.Total // 已完成并清理
			}
			closed := b.closed
			b.mu.Unlock()
			if doneCount >= res.Total {
				res.Replayed = res.Total
				res.Remaining = 0
				return res, nil
			}
			if closed {
				return res, ErrClosed
			}
			select {
			case <-ctx.Done():
				res.Replayed = doneCount
				res.Remaining = res.Total - doneCount
				return res, ctx.Err()
			case <-b.notify:
			case <-b.pumpStop:
				res.Replayed = doneCount
				res.Remaining = res.Total - doneCount
				return res, ErrClosed
			}
		}
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

// createAndApplyLocked 持久化重放意图并尽量推进一批。
// 意图创建与首批状态变更在同一个原子快照内落盘：崩溃只会发生在
// "整体未落盘"或"整体已落盘"两个点上，结果确定。
// 返回请求与本次实际重放数。调用方持锁。
func (b *Broker) createAndApplyLocked(group string, targets []string) (*replayRequest, int) {
	b.state.ReplaySeq++
	req := &replayRequest{
		ID:      fmt.Sprintf("replay-%d", b.state.ReplaySeq),
		Group:   group,
		Targets: targets,
		Applied: map[string]bool{},
	}
	b.state.Replays[req.ID] = req
	n := b.applyOneChunkLocked(req)
	if n == 0 {
		if _, still := b.state.Replays[req.ID]; still {
			// 容量满、零进展：意图必须先落盘，之后由泵续跑。
			_ = b.st.save(b.state)
		}
	}
	return req, n
}

// applyOneChunkLocked 在当前剩余容量内，把请求中尽可能多的死信重放，
// 每个目标只处理一次，完成的请求移出未完成表。整批变更一次原子落盘。
// 返回本次重放数量。调用方持锁。
func (b *Broker) applyOneChunkLocked(req *replayRequest) int {
	now := time.Now()
	n := 0
	for _, id := range req.Targets {
		if req.Applied[id] {
			continue
		}
		m, ok := b.state.Messages[id]
		if !ok {
			req.Applied[id] = true // 防御：消息不存在，避免请求永久卡住
			continue
		}
		if m.Status != StatusDead {
			// 同一条死信只会被重放请求复活；出现非 dead 说明已被处理，
			// 标记幂等跳过，绝不产生第二条在途消息。
			req.Applied[id] = true
			continue
		}
		if b.occupied() >= b.cfg.MaxPending {
			break // 容量受限：剩余的交给下次（泵/唤醒）续跑
		}
		round := m.Round + 1
		fromSeq := m.Seq
		b.state.Seq++
		toSeq := b.state.Seq
		m.Round = round
		m.Seq = toSeq // 分配新序号：排到所属分组（全局）队尾，不抢在未处理消息前
		m.Status = StatusPending
		m.Attempts = 0
		m.NotBefore = time.Time{} // 重放后立即可投递，重新获得完整重试机会
		m.DeadReason = ""
		m.ErrorClass = ""
		m.LastError = ""
		m.ReplayID = req.ID
		if m.PendingReplays == nil {
			m.PendingReplays = map[string]bool{}
		}
		m.PendingReplays[req.ID] = true
		m.ReplayHistory = append(m.ReplayHistory, ReplayEvent{
			Round: round, ReplayID: req.ID,
			FromSeq: fromSeq, ToSeq: toSeq,
			PrevState: StatusDead, At: now,
		})
		req.Applied[id] = true
		n++
	}
	finished := len(req.Applied) == len(req.Targets)
	if finished {
		delete(b.state.Replays, req.ID)
		for _, id := range req.Targets {
			if m, ok := b.state.Messages[id]; ok && m.PendingReplays != nil {
				delete(m.PendingReplays, req.ID)
				if len(m.PendingReplays) == 0 {
					m.PendingReplays = nil
				}
			}
		}
	}
	// 有重放、或本批把请求完成（可能仅含跳过项）都需要落盘；
	// 容量满且无任何进展时，createAndApplyLocked 负责落盘新建意图。
	if n > 0 || finished {
		_ = b.st.save(b.state)
	}
	return n
}

// replayPump 是后台续跑泵：启动时、容量释放/状态变化时，把所有未完成
// 重放请求在容量内向前推进。泵永不因容量阻塞，崩溃重启后自动接管。
func (b *Broker) replayPump() {
	defer close(b.pumpDone)
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return
		}
		advanced := b.pumpOnceLocked()
		b.mu.Unlock()
		if advanced {
			b.signal() // 唤醒阻塞中的 ReplayMessages/Group 调用
		}
		select {
		case <-b.pumpStop:
			return
		case <-b.notify:
		}
	}
}

// pumpOnceLocked 按请求 ID 顺序各推进一步（一个容量块），返回是否有进展。
func (b *Broker) pumpOnceLocked() bool {
	if len(b.state.Replays) == 0 {
		return false
	}
	ids := make([]string, 0, len(b.state.Replays))
	for id := range b.state.Replays {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	advanced := false
	for _, id := range ids {
		req := b.state.Replays[id]
		if req == nil {
			continue
		}
		if n := b.applyOneChunkLocked(req); n > 0 {
			advanced = true
		}
	}
	return advanced
}

// PendingReplays 返回未完成重放请求的快照（用于观测与验证）。
func (b *Broker) PendingReplays() []ReplayInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, 0, len(b.state.Replays))
	for id := range b.state.Replays {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]ReplayInfo, 0, len(ids))
	for _, id := range ids {
		req := b.state.Replays[id]
		applied := make(map[string]bool, len(req.Applied))
		for k, v := range req.Applied {
			applied[k] = v
		}
		targets := append([]string(nil), req.Targets...)
		out = append(out, ReplayInfo{
			ID: req.ID, Group: req.Group,
			Targets: targets, Applied: applied, Done: req.Done,
		})
	}
	return out
}
