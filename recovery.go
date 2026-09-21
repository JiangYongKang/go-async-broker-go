package broker

// resumeAfterOpen makes leases that were inflight during a crash visible again.
// Retry-waiting leases retain their durable VisibleAt; pending leases need no
// change. Durable groups are reactivated only when the application supplies a
// handler through Subscribe after Open.
func (b *Broker) resumeAfterOpen() error {
	var events []journalEvent

	for topic, ts := range b.state.topics {
		for group, gs := range ts.groups {
			for key, ks := range gs.keys {
				if len(ks.queue) == 0 {
					continue
				}
				id := ks.queue[0]
				ms := gs.deliveries[id]
				if ms == nil || ms.status != statusInflight {
					continue
				}
				record := ts.messages[id]
				if record == nil {
					return brokerError(CodeCorruptStorage, "inflight delivery has no message record", topic+"/"+group+"/"+id, nil)
				}
				events = append(events, journalEvent{
					Type:      eventRetry,
					At:        b.now(),
					Topic:     topic,
					Group:     group,
					Key:       key,
					MessageID: id,
					Offset:    record.message.Offset,
					Seq:       ms.seq,
					Attempt:   ms.attempt,
					VisibleAt: b.now(),
					Status:    statusPending,
					Reason:    "recovered inflight lease after broker restart",
					LastError: ms.lastError,
					Retryable: true,
				})
			}
		}
	}

	if len(events) == 0 {
		return nil
	}
	raw, err := encodeEvents(events)
	if err != nil {
		return err
	}
	if err := b.store.Commit(raw); err != nil {
		return err
	}
	for _, ev := range events {
		if err := b.state.apply(ev); err != nil {
			return err
		}
	}
	return nil
}
