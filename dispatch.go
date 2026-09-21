package broker

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

func (b *Broker) runSubscription(ctx context.Context, sub *brokerSubscription) {
	defer b.wg.Done()
	defer close(sub.done)

	b.mu.Lock()
	inflight := make(map[string]bool)
	if b.activeDispatch[sub.topic] == nil {
		b.activeDispatch[sub.topic] = make(map[string]bool)
	}
	b.mu.Unlock()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sub.shutdown:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-sub.notify:
		}

		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return
		}
		ts := b.state.topics[sub.topic]
		var gs *groupState
		if ts != nil {
			gs = ts.groups[sub.group]
		}
		if gs == nil {
			b.mu.Unlock()
			continue
		}

		keys := make([]string, 0, len(gs.keys))
		for key := range gs.keys {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		now := b.now()

		type claimed struct {
			key      string
			delivery *Delivery
		}
		claimedDeliveries := make([]claimed, 0)
		topicInflight := b.activeDispatch[sub.topic]
		scanFailed := false

		for _, key := range keys {
			if topicInflight[key] || inflight[key] {
				continue
			}
			ks := gs.keys[key]
			if len(ks.queue) == 0 {
				continue
			}
			id := ks.queue[0]
			ms := gs.deliveries[id]
			if ms == nil {
				b.logf("dispatch skipped input={topic:%s group:%s key:%s message:%s} decision=skip reason=missing_state", sub.topic, sub.group, key, id)
				continue
			}
			if ms.status == statusRetryWait && now.Before(ms.visibleAt) {
				continue
			}

			attempt := ms.attempt
			if attempt < 1 {
				attempt = 1
			}
			if ms.status == statusRetryWait {
				attempt++
			}
			deliveryID, err := b.nextID()
			if err != nil {
				b.logf("dispatch failed input={topic:%s group:%s key:%s message:%s attempt:%d} decision=skip reason=id_error cause=%v", sub.topic, sub.group, key, id, attempt, err)
				scanFailed = true
				break
			}
			record := ts.messages[id]
			if record == nil {
				b.logf("dispatch failed input={topic:%s group:%s key:%s message:%s attempt:%d} decision=skip reason=missing_message", sub.topic, sub.group, key, id, attempt)
				scanFailed = true
				break
			}
			ev := journalEvent{
				Type:       eventDeliver,
				At:         now,
				Topic:      sub.topic,
				Group:      sub.group,
				Key:        key,
				MessageID:  id,
				Offset:     record.message.Offset,
				Seq:        ms.seq,
				DeliveryID: deliveryID,
				Attempt:    attempt,
				Status:     statusInflight,
			}
			raw, err := encodeEvent(ev)
			if err != nil {
				b.logf("dispatch failed input={topic:%s group:%s key:%s message:%s attempt:%d} decision=skip reason=encode_error cause=%v", sub.topic, sub.group, key, id, attempt, err)
				scanFailed = true
				break
			}
			if err := b.store.Commit([]json.RawMessage{raw}); err != nil {
				b.logf("dispatch failed input={topic:%s group:%s key:%s message:%s attempt:%d} decision=skip reason=commit_error cause=%v", sub.topic, sub.group, key, id, attempt, err)
				scanFailed = true
				break
			}
			if err := b.state.apply(ev); err != nil {
				b.logf("dispatch failed input={topic:%s group:%s key:%s message:%s attempt:%d} decision=skip reason=replay_error cause=%v", sub.topic, sub.group, key, id, attempt, err)
				scanFailed = true
				break
			}

			topicInflight[key] = true
			inflight[key] = true
			claimedDeliveries = append(claimedDeliveries, claimed{
				key: key,
				delivery: &Delivery{
					ID:          deliveryID,
					Group:       sub.group,
					Attempt:     attempt,
					Message:     record.message.Clone(),
					DeliveredAt: now,
				},
			})
			b.logf("dispatch committed input={topic:%s group:%s key:%s message:%s delivery:%s attempt:%d status:%s} decision=lease_visible", sub.topic, sub.group, key, id, deliveryID, attempt, ms.status)
		}
		b.mu.Unlock()
		if scanFailed {
			continue
		}

		for _, item := range claimedDeliveries {
			b.invokeHandler(ctx, sub, item.key, item.delivery)
			inflight[item.key] = false
		}
	}
}

func (b *Broker) invokeHandler(parent context.Context, sub *brokerSubscription, key string, delivery *Delivery) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ctx, cancel := context.WithCancel(parent)
		defer cancel()

		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			select {
			case <-sub.shutdown:
				cancel()
			case <-ctx.Done():
			}
		}()

		var result HandlerResult
		func() {
			defer func() {
				if r := recover(); r != nil {
					result = Retry("handler panic", asError(r))
				}
			}()
			result = sub.handler(ctx, delivery)
		}()
		cancel()
		<-handlerDone

		b.mu.Lock()
		defer b.mu.Unlock()
		if b.activeDispatch[sub.topic] != nil {
			delete(b.activeDispatch[sub.topic], key)
		}
		if !b.closed {
			if err := b.resolveLocked(delivery, result, true); err != nil {
				b.logf("handler result rejected input={topic:%s group:%s key:%s message:%s delivery:%s attempt:%d decision:%d} reason=%v", delivery.Message.Topic, delivery.Group, key, delivery.Message.ID, delivery.ID, delivery.Attempt, result.Decision, err)
			}
			b.notifyKeyLocked(sub.topic, key)
		}
	}()
}

func asError(v any) error {
	if err, ok := v.(error); ok {
		return err
	}
	return &simplePanic{value: v}
}

type simplePanic struct{ value any }

func (e *simplePanic) Error() string { return "panic: " + stringifyAny(e.value) }
