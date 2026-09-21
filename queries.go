package broker

import "sort"

// OffsetStatus reports the durable ordered position for one group key.
// Committed is the number of terminal messages in that key; Next is the
// position the head will use.
type OffsetStatus struct {
	Key       string
	Committed int64
	Next      int64
	Backlog   int
}

// TopicStats describes retained capacity and dead-letter count.
type TopicStats struct {
	Topic       string
	Messages    int
	Retained    int
	DeadLetters int
	Capacity    int
	Full        bool
}

// DeadLetters returns dead-letter entries for a topic/group in original message
// offset order. Empty group means all groups. Returned values are safe copies.
func (b *Broker) DeadLetters(topic, group string) []DeadLetterEntry {
	b.mu.Lock()
	byTopic := b.state.deadLetters[topic]
	var out []DeadLetterEntry
	if byTopic != nil {
		if group != "" {
			for _, entry := range byTopic[group] {
				out = append(out, entry.clone())
			}
		} else {
			for _, entries := range byTopic {
				for _, entry := range entries {
					out = append(out, entry.clone())
				}
			}
		}
	}
	b.mu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if group == "" {
			if out[i].Group != out[j].Group {
				return out[i].Group < out[j].Group
			}
		}
		return out[i].Offset < out[j].Offset
	})
	return out
}

// GroupOffsets returns per-key durable consumer positions.
func (b *Broker) GroupOffsets(topic, group string) []OffsetStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	ts := b.state.topics[topic]
	if ts == nil || ts.groups[group] == nil {
		return nil
	}
	gs := ts.groups[group]
	keys := make([]string, 0, len(gs.keys))
	for key := range gs.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]OffsetStatus, 0, len(keys))
	for _, key := range keys {
		ks := gs.keys[key]
		out = append(out, OffsetStatus{
			Key:       key,
			Committed: ks.committed,
			Next:      ks.nextSeq + 1,
			Backlog:   len(ks.queue),
		})
	}
	return out
}

// Stats returns bounded-capacity observability for one topic.
func (b *Broker) Stats(topic string) TopicStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	ts := b.state.topic(topic)
	tc := b.cfg.topicConfig(topic)
	return TopicStats{
		Topic:       topic,
		Messages:    len(ts.messages),
		Retained:    ts.retained,
		DeadLetters: ts.dlqCount,
		Capacity:    tc.MaxCapacity,
		Full:        tc.MaxCapacity >= 0 && ts.retained >= tc.MaxCapacity,
	}
}
