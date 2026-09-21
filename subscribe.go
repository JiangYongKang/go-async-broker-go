package broker

import (
	"context"
	"encoding/json"
)

// Subscribe starts key-ordered delivery for a durable consumer group. The same
// group may only have one active handler in a broker process because ordering
// and lease ownership are maintained in local memory. Closing the broker or
// cancelling ctx stops the handler without deleting committed group state.
func (b *Broker) Subscribe(ctx context.Context, topic, group string, handler Handler) error {
	if topic == "" || group == "" {
		return invalidf("subscription topic and group must not be empty")
	}
	if handler == nil {
		return invalidf("subscription handler must not be nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	subKey := subscriptionKey(topic, group)
	if existing := b.subs[subKey]; existing != nil && !existing.stopped {
		return brokerError(CodeInvalidArgument, "consumer group already has an active handler", "topic="+topic+", group="+group, nil)
	}

	ts := b.state.topic(topic)
	if _, exists := ts.groups[group]; !exists {
		ev := journalEvent{Type: eventSubscribe, At: b.now(), Topic: topic, Group: group}
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
		b.logf("subscribe committed input={topic:%s group:%s} decision=durable_group_created", topic, group)
	}

	sub := &brokerSubscription{
		topic:    topic,
		group:    group,
		handler:  handler,
		notify:   make(chan string, 128),
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
	b.subs[subKey] = sub
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			if b.subs[subKey] == sub {
				sub.stopped = true
				delete(b.subs, subKey)
			}
			b.mu.Unlock()
		case <-sub.shutdown:
		case <-sub.done:
		}
	}()
	b.wg.Add(1)
	go b.runSubscription(ctx, sub)
	b.logf("subscribe started input={topic:%s group:%s} decision=key_serialized_dispatch", topic, group)
	return nil
}

func (b *Broker) notifyKeyLocked(topic, key string) {
	for _, sub := range b.subs {
		if sub.stopped || sub.topic != topic {
			continue
		}
		select {
		case sub.notify <- key:
		default:
			// The dispatcher scans retry/pending state on a short timer, so a
			// full notification channel cannot cause a lost wakeup.
		}
	}
}
