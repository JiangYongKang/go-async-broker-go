package broker

import (
	"context"
	"strings"
	"time"
)

// Ack commits a manually acknowledged delivery. Business writes in the option
// are committed in the same transaction as the terminal delivery event.
func (b *Broker) Ack(ctx context.Context, delivery *Delivery, opts ...AckOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delivery == nil || delivery.Message == nil {
		return invalidf("delivery and message must not be nil")
	}
	var opt AckOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Reason == "" {
		opt.Reason = "manual ack"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	return b.resolveLocked(delivery, HandlerResult{
		Decision:       DecisionAck,
		Reason:         opt.Reason,
		BusinessWrites: opt.BusinessWrites,
	}, false)
}

// Nack rejects a delivery. Retryable=true schedules another attempt according
// to the topic backoff; retryable=false immediately records a non-retryable
// dead-letter entry.
func (b *Broker) Nack(ctx context.Context, delivery *Delivery, opts NackOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delivery == nil || delivery.Message == nil {
		return invalidf("delivery and message must not be nil")
	}
	if opts.Reason == "" {
		return invalidf("nack reason must not be empty")
	}
	decision := DecisionRetry
	if !opts.Retryable {
		decision = DecisionDeadLetter
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	return b.resolveLocked(delivery, HandlerResult{
		Decision: decision,
		Reason:   opts.Reason,
		Cause:    opts.Cause,
	}, false)
}

func (b *Broker) resolveLocked(delivery *Delivery, result HandlerResult, fromHandler bool) error {
	if err := validateHandlerResult(result); err != nil {
		return err
	}

	msg := delivery.Message
	ts := b.state.topics[msg.Topic]
	if ts == nil {
		return ErrUnknownDelivery
	}
	gs := ts.groups[delivery.Group]
	if gs == nil {
		return ErrUnknownDelivery
	}
	current := gs.deliveries[msg.ID]
	if current == nil {
		if completion, ok := b.completion[delivery.ID]; ok {
			b.logf("terminal duplicate input={topic:%s group:%s key:%s message:%s delivery:%s attempt:%d decision:%d} reason=already_terminal terminal=%d", msg.Topic, delivery.Group, msg.Key, msg.ID, delivery.ID, delivery.Attempt, result.Decision, completion.decision)
			if completion.decision == DecisionAck {
				return ErrAlreadyAcked
			}
			return brokerError(CodeAlreadyDeadLettered, "delivery was already dead-lettered", completion.reason, nil)
		}
		if result.Decision == DecisionAck {
			return brokerError(CodeUnknownDelivery, "unknown acknowledgement", "message="+msg.ID+", delivery="+delivery.ID, nil)
		}
		return brokerError(CodeUnknownDelivery, "unknown delivery rejection", "message="+msg.ID+", delivery="+delivery.ID, nil)
	}

	if current.deliveryID != delivery.ID {
		b.logf("stale terminal input={topic:%s group:%s key:%s message:%s delivery:%s current_delivery:%s attempt:%d decision:%d} reason=superseded_attempt", msg.Topic, delivery.Group, msg.Key, msg.ID, delivery.ID, current.deliveryID, delivery.Attempt, result.Decision)
		return brokerError(CodeStaleDeliveryAttempt, "delivery attempt was superseded by a newer lease", "message="+msg.ID+", old="+delivery.ID+", current="+current.deliveryID, nil)
	}

	tc := b.cfg.topicConfig(msg.Topic)
	decision := result.Decision
	reason := strings.TrimSpace(result.Reason)
	lastError := ""
	if result.Cause != nil {
		lastError = result.Cause.Error()
	}
	at := b.now()

	if decision == DecisionRetry {
		if current.attempt >= tc.MaxAttempts {
			decision = DecisionDeadLetter
			reason = "retry attempts exhausted: " + reason
		}
	}

	var events []journalEvent
	if decision == DecisionRetry {
		nextNumber := current.attempt
		delay := tc.Backoff.Next(nextNumber)
		ev := journalEvent{
			Type:       eventRetry,
			At:         at,
			Topic:      msg.Topic,
			Group:      delivery.Group,
			Key:        current.key,
			MessageID:  msg.ID,
			Offset:     msg.Offset,
			Seq:        current.seq,
			DeliveryID: "",
			Attempt:    current.attempt,
			VisibleAt:  at.Add(delay),
			Status:     statusRetryWait,
			Reason:     reason,
			LastError:  lastError,
			Retryable:  true,
		}
		events = append(events, ev)
		b.logf("retry scheduled input={topic:%s group:%s key:%s message:%s delivery:%s attempt:%d max_attempts:%d delay:%s visible_at:%s} decision=retry reason=%s last_error=%s", msg.Topic, delivery.Group, current.key, msg.ID, delivery.ID, current.attempt, tc.MaxAttempts, delay, ev.VisibleAt.Format(time.RFC3339Nano), reason, lastError)
	} else {
		stableReason := reason
		if decision == DecisionDeadLetter && !strings.HasPrefix(stableReason, "retry attempts exhausted") && result.Decision != DecisionRetry {
			// Explicitly preserve a stable prefix for non-retryable results.
			stableReason = "non-retryable: " + stableReason
		} else if decision == DecisionDeadLetter {
			stableReason = "retry exhausted: " + stableReason
		}
		ev := journalEvent{
			Type:           eventTerminal,
			At:             at,
			Topic:          msg.Topic,
			Group:          delivery.Group,
			Key:            current.key,
			MessageID:      msg.ID,
			Offset:         msg.Offset,
			Seq:            current.seq,
			DeliveryID:     delivery.ID,
			Attempt:        current.attempt,
			Status:         groupDeliveryStatus(decision.String()),
			Decision:       decision,
			Reason:         stableReason,
			LastError:      lastError,
			BusinessWrites: cloneByteMap(result.BusinessWrites),
		}
		events = append(events, ev)
		b.logf("terminal committed input={topic:%s group:%s key:%s message:%s delivery:%s attempt:%d seq:%d decision:%d business_writes:%d} reason=%s last_error=%s", msg.Topic, delivery.Group, current.key, msg.ID, delivery.ID, current.attempt, current.seq, decision, len(result.BusinessWrites), stableReason, lastError)
	}

	rawEvents, err := encodeEvents(events)
	if err != nil {
		return err
	}
	if err := b.store.Commit(rawEvents); err != nil {
		return err
	}
	for _, ev := range events {
		if err := b.state.apply(ev); err != nil {
			return err
		}
	}

	if decision != DecisionRetry {
		b.rememberCompletion(delivery.ID, terminalCompletion{decision: decision, attempt: current.attempt, reason: reason})
		b.broadcastCapacity()
	}
	b.notifyKeyLocked(msg.Topic, current.key)
	return nil
}

func validateHandlerResult(result HandlerResult) error {
	switch result.Decision {
	case DecisionAck, DecisionRetry, DecisionDeadLetter:
	default:
		return invalidf("handler decision %d is not supported", result.Decision)
	}
	if result.Decision != DecisionAck && strings.TrimSpace(result.Reason) == "" {
		return invalidf("retry and dead-letter results must provide a reason")
	}
	return nil
}

func (b *Broker) rememberCompletion(deliveryID string, value terminalCompletion) {
	b.completion[deliveryID] = value
	maxEntries := b.cfg.CompletionCacheSize
	if maxEntries <= 0 {
		maxEntries = 4096
	}
	for len(b.completion) > maxEntries {
		for id := range b.completion {
			delete(b.completion, id)
			break
		}
	}
}
