package broker

import "fmt"

// CommitOffset 提交消费组位点。位点只允许严格单调前进：
// 回退或与已提交位点相同（崩溃恢复后的重复提交）都被明确拒绝，
// 返回 ErrOffsetRegression，防止从中间继续或重复提交。
// 位点变更与消息状态一样先落盘再生效。
func (b *Broker) CommitOffset(group string, offset uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	committed := b.state.Offsets[group]
	if offset <= committed {
		return fmt.Errorf("%w: group=%s committed=%d got=%d",
			ErrOffsetRegression, group, committed, offset)
	}
	b.state.Offsets[group] = offset
	if err := b.st.save(b.state); err != nil {
		b.state.Offsets[group] = committed
		return fmt.Errorf("persist offset: %w", err)
	}
	return nil
}

// Offset 返回消费组当前已提交位点（无记录时为 0）。
func (b *Broker) Offset(group string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Offsets[group]
}
