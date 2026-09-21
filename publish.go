package broker

import (
	"context"
	"encoding/json"
	"sort"
)

// Publish atomically stores an outgoing message and BusinessWrites. The message
// becomes visible only after that transaction is durable. A reused ID is
// rejected with CodeDuplicateMessage and applies no business writes.
func (b *Broker) Publish(ctx context.Context, input PublishInput) (PublishResult, error) {
	if input.Topic == "" {
		return PublishResult{}, invalidf("publish topic must not be empty")
	}
	if input.ID == "" {
		id, err := b.nextID()
		if err != nil {
			return PublishResult{}, err
		}
		input.ID = id
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return PublishResult{}, ErrBrokerClosed
	}
	if err := b.waitForCapacityLocked(ctx, input.Topic, input.NonBlocking); err != nil {
		return PublishResult{}, err
	}

	ts := b.state.topic(input.Topic)
	if _, exists := ts.messages[input.ID]; exists {
		err := brokerError(CodeDuplicateMessage, "message ID was already committed", "topic="+input.Topic+", id="+input.ID, nil)
		b.logf("publish rejected input={topic:%s id:%s key:%s body_len:%d nonblocking:%t} decision=reject reason=duplicate_message", input.Topic, input.ID, input.Key, len(input.Body), input.NonBlocking)
		return PublishResult{}, err
	}

	groupNames := make([]string, 0, len(ts.groups))
	for name := range ts.groups {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	at := b.now()
	ev := journalEvent{
		Type:           eventPublish,
		At:             at,
		Topic:          input.Topic,
		Key:            input.Key,
		MessageID:      input.ID,
		Offset:         ts.nextOffset,
		Groups:         groupNames,
		Body:           append([]byte(nil), input.Body...),
		Headers:        cloneHeaders(input.Headers),
		BusinessWrites: cloneByteMap(input.BusinessWrites),
	}
	raw, err := encodeEvent(ev)
	if err != nil {
		return PublishResult{}, err
	}
	if err := b.store.Commit([]json.RawMessage{raw}); err != nil {
		return PublishResult{}, err
	}
	if err := b.state.apply(ev); err != nil {
		return PublishResult{}, err
	}

	msg := ts.messages[input.ID].message
	b.logf("publish accepted input={topic:%s id:%s key:%s offset:%d body_len:%d groups:%v business_writes:%d} decision=commit_atomic", input.Topic, input.ID, input.Key, msg.Offset, len(input.Body), groupNames, len(input.BusinessWrites))
	b.notifyKeyLocked(input.Topic, input.Key)
	return PublishResult{Message: msg.Clone()}, nil
}

func (b *Broker) waitForCapacityLocked(ctx context.Context, topic string, nonBlocking bool) error {
	tc := b.cfg.topicConfig(topic)
	ts := b.state.topic(topic)
	if tc.MaxCapacity < 0 || ts.retained < tc.MaxCapacity {
		return nil
	}
	if nonBlocking {
		b.logf("backpressure rejected input={topic:%s retained:%d capacity:%d} decision=reject reason=capacity_exceeded", topic, ts.retained, tc.MaxCapacity)
		return brokerError(CodeCapacityExceeded, "topic capacity is exhausted", "topic="+topic+" retained="+itoa(ts.retained)+" capacity="+itoa(tc.MaxCapacity), nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	waiter := make(chan struct{})
	b.capacityWaiters = append(b.capacityWaiters, waiter)
	for {
		if ts.retained < tc.MaxCapacity {
			b.removeCapacityWaiter(waiter)
			return nil
		}
		if b.closed {
			b.removeCapacityWaiter(waiter)
			return ErrBrokerClosed
		}
		if err := ctx.Err(); err != nil {
			b.removeCapacityWaiter(waiter)
			return err
		}

		b.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-waiter:
		}
		b.mu.Lock()

		if err := ctx.Err(); err != nil {
			return err
		}
		if b.closed {
			return ErrBrokerClosed
		}
	}
}
