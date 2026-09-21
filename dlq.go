package broker

import (
	"context"
	"encoding/json"
)

// DeleteDeadLetter explicitly removes an inspected dead-letter entry and
// releases its topic capacity. It is idempotent: deleting an entry that is not
// retained returns without an error and never revives a message.
func (b *Broker) DeleteDeadLetter(ctx context.Context, topic, group, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if topic == "" || group == "" || messageID == "" {
		return invalidf("topic, group and message ID are required to delete a dead letter")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	byTopic := b.state.deadLetters[topic]
	var entry DeadLetterEntry
	if byTopic != nil && byTopic[group] != nil {
		entry = byTopic[group][messageID]
	}
	if entry.MessageID == "" {
		b.logf("delete dead letter input={topic:%s group:%s message:%s} decision=noop reason=not_retained", topic, group, messageID)
		return nil
	}

	ev := journalEvent{
		Type:      eventDLQDelete,
		At:        b.now(),
		Topic:     topic,
		Group:     group,
		Key:       entry.Key,
		MessageID: messageID,
	}
	raw, err := encodeEvent(ev)
	if err != nil {
		return err
	}
	if err := b.store.Commit([]json.RawMessage{raw}); err != nil {
		return err
	}
	if err := b.state.apply(ev); err != nil {
		return err
	}
	b.logf("delete dead letter input={topic:%s group:%s message:%s attempts:%d} decision=capacity_released reason=manual_delete", topic, group, messageID, entry.Attempts)
	b.broadcastCapacity()
	return nil
}
