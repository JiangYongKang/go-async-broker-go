package broker

import (
	"encoding/json"
	"sort"
	"time"
)

type eventType string

const (
	eventSubscribe eventType = "subscribe"
	eventPublish   eventType = "publish"
	eventDeliver   eventType = "deliver"
	eventRetry     eventType = "retry"
	eventTerminal  eventType = "terminal"
	eventBusiness  eventType = "business"
	eventDLQDelete eventType = "dead_letter_delete"
)

type groupDeliveryStatus string

const (
	statusPending   groupDeliveryStatus = "pending"
	statusInflight  groupDeliveryStatus = "inflight"
	statusRetryWait groupDeliveryStatus = "retry_wait"
)

// journalEvent is the durable representation of every state transition.
type journalEvent struct {
	Type      eventType `json:"type"`
	At        time.Time `json:"at"`
	Topic     string    `json:"topic,omitempty"`
	Group     string    `json:"group,omitempty"`
	Key       string    `json:"key,omitempty"`
	MessageID string    `json:"message_id,omitempty"`
	Offset    int64     `json:"offset,omitempty"`
	Seq       int64     `json:"seq,omitempty"`
	Groups    []string  `json:"groups,omitempty"`

	// Message payload.
	Body    []byte            `json:"body,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`

	// Delivery/retry state.
	DeliveryID  string              `json:"delivery_id,omitempty"`
	Attempt     int                 `json:"attempt,omitempty"`
	VisibleAt   time.Time           `json:"visible_at,omitempty"`
	Status      groupDeliveryStatus `json:"status,omitempty"`
	Decision    HandlerDecision     `json:"decision,omitempty"`
	Reason      string              `json:"reason,omitempty"`
	LastError   string              `json:"last_error,omitempty"`
	Retryable   bool                `json:"retryable,omitempty"`
	BusinessKey string              `json:"business_key,omitempty"`

	// Publishing or acknowledgement writes.
	BusinessWrites map[string][]byte `json:"business_writes,omitempty"`

	// Standalone business-only transactions.
	KVKey   string `json:"kv_key,omitempty"`
	KVValue []byte `json:"kv_value,omitempty"`
}

func encodeEvent(ev journalEvent) (json.RawMessage, error) {
	raw, err := json.Marshal(ev)
	if err != nil {
		return nil, brokerError(CodeCorruptStorage, "encode journal event", string(ev.Type), err)
	}
	return raw, nil
}

func encodeEvents(events []journalEvent) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(events))
	for _, ev := range events {
		raw, err := encodeEvent(ev)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

func decodeEvents(rawEntries []json.RawMessage) ([]journalEvent, error) {
	events := make([]journalEvent, 0, len(rawEntries))
	for i, raw := range rawEntries {
		var ev journalEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, brokerError(CodeCorruptStorage, "decode journal event", "index="+itoa(i), err)
		}
		events = append(events, ev)
	}
	return events, nil
}

type groupKeyState struct {
	queue     []string
	committed int64
	nextSeq   int64
	seq       map[string]int64
}

type groupState struct {
	topic      string
	name       string
	subscribed bool
	keys       map[string]*groupKeyState
	deliveries map[string]*groupMessageState
}

type groupMessageState struct {
	topic      string
	group      string
	key        string
	messageID  string
	seq        int64
	deliveryID string
	attempt    int
	status     groupDeliveryStatus
	visibleAt  time.Time
	reason     string
	lastError  string
}

type messageRecord struct {
	message       *Message
	pendingGroups int
}

type deadLetterRecord struct {
	entry DeadLetterEntry
}

type engineState struct {
	topics       map[string]*topicState
	business     map[string][]byte
	deadLetters  map[string]map[string]map[string]DeadLetterEntry
	completedOld map[string]HandlerDecision
}

type topicState struct {
	name       string
	nextOffset int64
	groups     map[string]*groupState
	messages   map[string]*messageRecord
	// retained counts live messages plus retained DLQ entries. A dead-lettered
	// live message is moved, not copied: during terminal handling it remains
	// counted once; after a restart it is reconstructed from the message and
	// terminal events and must also remain exactly one.
	retained     int
	liveMessages int
	dlqCount     int
}

func newEngineState() *engineState {
	return &engineState{
		topics:       make(map[string]*topicState),
		business:     make(map[string][]byte),
		deadLetters:  make(map[string]map[string]map[string]DeadLetterEntry),
		completedOld: make(map[string]HandlerDecision),
	}
}

func (s *engineState) topic(name string) *topicState {
	ts := s.topics[name]
	if ts == nil {
		ts = &topicState{
			name:       name,
			nextOffset: 1,
			groups:     make(map[string]*groupState),
			messages:   make(map[string]*messageRecord),
		}
		s.topics[name] = ts
		s.deadLetters[name] = make(map[string]map[string]DeadLetterEntry)
	}
	return ts
}

func (ts *topicState) group(name string) *groupState {
	gs := ts.groups[name]
	if gs == nil {
		gs = &groupState{
			topic:      ts.name,
			name:       name,
			keys:       make(map[string]*groupKeyState),
			deliveries: make(map[string]*groupMessageState),
		}
		ts.groups[name] = gs
	}
	return gs
}

func (gs *groupState) keyState(key string) *groupKeyState {
	ks := gs.keys[key]
	if ks == nil {
		ks = &groupKeyState{seq: make(map[string]int64)}
		gs.keys[key] = ks
	}
	return ks
}

func replay(events []journalEvent) (*engineState, error) {
	state := newEngineState()
	for seq, ev := range events {
		if err := state.apply(ev); err != nil {
			if _, ok := AsBrokerError(err); ok {
				return nil, err
			}
			return nil, brokerError(CodeCorruptStorage, "replay journal event", "sequence="+itoa(seq), err)
		}
	}
	return state, nil
}

func (s *engineState) apply(ev journalEvent) error {
	switch ev.Type {
	case eventSubscribe:
		return s.applySubscribe(ev)
	case eventPublish:
		return s.applyPublish(ev)
	case eventDeliver, eventRetry:
		return s.applyDeliveryUpdate(ev)
	case eventTerminal:
		return s.applyTerminal(ev)
	case eventBusiness:
		if ev.KVKey != "" {
			s.business[ev.KVKey] = append([]byte(nil), ev.KVValue...)
		}
		return nil
	case eventDLQDelete:
		return s.applyDLQDelete(ev)
	default:
		return invalidf("unknown journal event type %q", ev.Type)
	}
}

func (s *engineState) applySubscribe(ev journalEvent) error {
	if ev.Topic == "" || ev.Group == "" {
		return invalidf("subscribe event missing identity")
	}
	ts := s.topic(ev.Topic)
	if existing, exists := ts.groups[ev.Group]; exists {
		// Replays use append-only identity: a second subscribe for the same
		// group simply confirms an existing durable subscription.
		existing.subscribed = true
		return nil
	}
	gs := &groupState{
		topic:      ev.Topic,
		name:       ev.Group,
		subscribed: true,
		keys:       make(map[string]*groupKeyState),
		deliveries: make(map[string]*groupMessageState),
	}
	ts.groups[ev.Group] = gs

	// A new consumer group receives every message visible at the transaction
	// point, in global offset order. Iterating the journal replay in order has
	// populated exactly that set in ts.messages.
	type ordered struct {
		offset int64
		id     string
	}
	records := make([]ordered, 0, len(ts.messages))
	for id, record := range ts.messages {
		records = append(records, ordered{offset: record.message.Offset, id: id})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].offset < records[j].offset })
	for _, item := range records {
		record := ts.messages[item.id]
		msg := record.message
		ks := gs.keyState(msg.Key)
		if ks.seq == nil {
			ks.seq = make(map[string]int64)
		}
		ks.nextSeq++
		ks.seq[msg.ID] = ks.nextSeq
		ks.queue = append(ks.queue, msg.ID)
		gs.deliveries[msg.ID] = &groupMessageState{
			topic:     ev.Topic,
			group:     ev.Group,
			key:       msg.Key,
			messageID: msg.ID,
			seq:       ks.seq[msg.ID],
			status:    statusPending,
		}
		record.pendingGroups++
	}
	return nil
}

func (s *engineState) applyPublish(ev journalEvent) error {
	if ev.Topic == "" || ev.MessageID == "" {
		return invalidf("publish event missing identity")
	}
	ts := s.topic(ev.Topic)
	if ev.Offset != ts.nextOffset {
		return brokerError(CodeCorruptStorage, "unexpected message offset during replay",
			"topic="+ev.Topic+" expected="+itoa64(ts.nextOffset)+" got="+itoa64(ev.Offset), nil)
	}
	if _, exists := ts.messages[ev.MessageID]; exists {
		return brokerError(CodeCorruptStorage, "duplicate message ID in journal", ev.MessageID, nil)
	}

	msg := &Message{
		ID:        ev.MessageID,
		Topic:     ev.Topic,
		Key:       ev.Key,
		Body:      append([]byte(nil), ev.Body...),
		Headers:   cloneHeaders(ev.Headers),
		Offset:    ev.Offset,
		CreatedAt: ev.At,
	}
	record := &messageRecord{message: msg}
	for _, groupName := range ev.Groups {
		gs := ts.group(groupName)
		ks := gs.keyState(ev.Key)
		if ks.seq == nil {
			ks.seq = make(map[string]int64)
		}
		ks.nextSeq++
		ks.seq[ev.MessageID] = ks.nextSeq
		ks.queue = append(ks.queue, ev.MessageID)
		gs.deliveries[ev.MessageID] = &groupMessageState{
			topic:     ev.Topic,
			group:     groupName,
			key:       ev.Key,
			messageID: ev.MessageID,
			seq:       ks.seq[ev.MessageID],
			status:    statusPending,
		}
		record.pendingGroups++
	}
	ts.messages[ev.MessageID] = record
	ts.nextOffset++
	ts.liveMessages++
	ts.retained++

	for key, value := range ev.BusinessWrites {
		s.business[key] = append([]byte(nil), value...)
	}
	return nil
}

func (s *engineState) applyDeliveryUpdate(ev journalEvent) error {
	ts := s.topic(ev.Topic)
	gs := ts.group(ev.Group)
	current := gs.deliveries[ev.MessageID]
	if current == nil {
		// A terminal event may already have removed it if this log is being
		// combined with an older snapshot format. The current implementation
		// has no snapshots, so a delivery update without pending state means
		// an inconsistent journal.
		return brokerError(CodeCorruptStorage, "delivery update without pending group state", ev.Topic+"/"+ev.Group+"/"+ev.MessageID, nil)
	}
	// Global offsets are sparse inside one key. The strict monotonic rule is
	// enforced on the per-key sequence only when a terminal result commits.
	if ev.Seq != 0 {
		if expected := gs.keyState(ev.Key).seq[ev.MessageID]; expected != 0 && ev.Seq != expected {
			return brokerError(CodeOffsetRollback, "delivery update sequence mismatch", "message="+ev.MessageID+" expected="+itoa64(expected)+" got="+itoa64(ev.Seq), nil)
		}
	}
	current.deliveryID = ev.DeliveryID
	current.attempt = ev.Attempt
	if ev.Seq != 0 {
		current.seq = ev.Seq
	}
	current.status = ev.Status
	current.visibleAt = ev.VisibleAt
	current.reason = ev.Reason
	current.lastError = ev.LastError
	return nil
}

func (s *engineState) applyTerminal(ev journalEvent) error {
	ts := s.topic(ev.Topic)
	gs := ts.group(ev.Group)
	ks := gs.keyState(ev.Key)
	current := gs.deliveries[ev.MessageID]
	if current == nil {
		return brokerError(CodeCorruptStorage, "terminal event without pending group state", ev.Topic+"/"+ev.Group+"/"+ev.MessageID, nil)
	}
	if len(ks.queue) == 0 || ks.queue[0] != ev.MessageID {
		return brokerError(CodeCorruptStorage, "terminal event did not match ordered queue head", ev.Topic+"/"+ev.Group+"/"+ev.Key, nil)
	}
	expectedSeq := ks.seq[ev.MessageID]
	if expectedSeq == 0 {
		return brokerError(CodeCorruptStorage, "terminal event has no per-key sequence", ev.MessageID, nil)
	}
	if ev.Seq != expectedSeq {
		return brokerError(CodeOffsetRollback, "terminal event sequence mismatch", "message="+ev.MessageID+" expected="+itoa64(expectedSeq)+" got="+itoa64(ev.Seq), nil)
	}
	if err := validateStrictOffsetStep(ks.committed, ev.Seq); err != nil {
		return err
	}

	for key, value := range ev.BusinessWrites {
		s.business[key] = append([]byte(nil), value...)
	}

	record := ts.messages[ev.MessageID]
	if ev.Decision == DecisionDeadLetter {
		groups := s.deadLetters[ev.Topic]
		if groups == nil {
			groups = make(map[string]map[string]DeadLetterEntry)
			s.deadLetters[ev.Topic] = groups
		}
		byGroup := groups[ev.Group]
		if byGroup == nil {
			byGroup = make(map[string]DeadLetterEntry)
			groups[ev.Group] = byGroup
		}
		if _, exists := byGroup[ev.MessageID]; !exists {
			ts.dlqCount++
		}
		var body []byte
		var headers map[string]string
		if record != nil {
			body = append([]byte(nil), record.message.Body...)
			headers = cloneHeaders(record.message.Headers)
		}
		byGroup[ev.MessageID] = DeadLetterEntry{
			Topic:     ev.Topic,
			Group:     ev.Group,
			MessageID: ev.MessageID,
			Offset:    ev.Offset,
			Key:       ev.Key,
			Body:      body,
			Headers:   headers,
			Attempts:  ev.Attempt,
			Reason:    ev.Reason,
			LastError: ev.LastError,
			DeadAt:    ev.At,
		}
	}

	ks.queue = ks.queue[1:]
	delete(ks.seq, ev.MessageID)
	ks.committed = ev.Seq
	delete(gs.deliveries, ev.MessageID)

	if record != nil {
		record.pendingGroups--
		if record.pendingGroups <= 0 {
			delete(ts.messages, ev.MessageID)
			ts.liveMessages--
		}
	}
	ts.retained = ts.liveMessages + ts.dlqCount
	return nil
}

func (s *engineState) applyDLQDelete(ev journalEvent) error {
	byTopic := s.deadLetters[ev.Topic]
	if byTopic == nil || byTopic[ev.Group] == nil {
		return nil
	}
	if _, exists := byTopic[ev.Group][ev.MessageID]; !exists {
		return nil
	}
	delete(byTopic[ev.Group], ev.MessageID)
	if ts := s.topics[ev.Topic]; ts != nil {
		ts.dlqCount--
		ts.retained = ts.liveMessages + ts.dlqCount
	}
	return nil
}

func cloneHeaders(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
