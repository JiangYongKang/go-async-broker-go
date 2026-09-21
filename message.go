package broker

import (
	"context"
	"time"
)

// Message is an immutable published record. Offset is assigned before the
// publish transaction commits and therefore never changes after visibility.
type Message struct {
	ID        string
	Topic     string
	Key       string
	Body      []byte
	Headers   map[string]string
	Offset    int64
	CreatedAt time.Time
}

// Clone returns a defensive copy so callers cannot mutate records retained by
// the broker.
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Body = append([]byte(nil), m.Body...)
	if m.Headers != nil {
		cp.Headers = make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			cp.Headers[k] = v
		}
	}
	return &cp
}

// Delivery is a lease to process one message. The same message may receive
// several Delivery values under at-least-once delivery; each has a unique ID.
type Delivery struct {
	ID          string
	Group       string
	Attempt     int
	Message     *Message
	DeliveredAt time.Time
}

func (d *Delivery) clone() *Delivery {
	cp := *d
	cp.Message = d.Message.Clone()
	return &cp
}

// HandlerDecision tells the dispatcher how to resolve a completed handler call.
type HandlerDecision int

const (
	// DecisionAck commits the message and the optional business writes.
	DecisionAck HandlerDecision = iota + 1
	// DecisionRetry records a retryable failure and schedules another attempt.
	DecisionRetry
	// DecisionDeadLetter records a non-retryable failure without using further
	// retry attempts.
	DecisionDeadLetter
)

// HandlerResult is the explicit result returned by a consumer. The broker does
// not interpret a panic or an ordinary Go error on its own; the result states
// whether the cause is retryable and records the reason shown in retry/DLQ
// state.
type HandlerResult struct {
	Decision HandlerDecision

	// Reason is required for failure decisions and useful for audit on ack.
	Reason string

	// Cause is the underlying business error, if any.
	Cause error

	// BusinessWrites are committed in the same local transaction as the ack.
	// Keeping the writes with the terminal resolution is the supported way to
	// prevent a duplicate delivery from duplicating local side effects.
	BusinessWrites map[string][]byte
}

// Retry builds a retryable failure result.
func Retry(reason string, cause error) HandlerResult {
	return HandlerResult{Decision: DecisionRetry, Reason: reason, Cause: cause}
}

// DeadLetter builds a non-retryable failure result.
func DeadLetter(reason string, cause error) HandlerResult {
	return HandlerResult{Decision: DecisionDeadLetter, Reason: reason, Cause: cause}
}

// AckResult commits a delivery. Writes are optional and atomic with the ack.
func AckResult(reason string, businessWrites map[string][]byte) HandlerResult {
	return HandlerResult{Decision: DecisionAck, Reason: reason, BusinessWrites: businessWrites}
}

func (d HandlerDecision) String() string {
	switch d {
	case DecisionAck:
		return "ack"
	case DecisionRetry:
		return "retry"
	case DecisionDeadLetter:
		return "dead_letter"
	default:
		return "unknown"
	}
}

// Handler consumes one delivery. It is invoked at most once at a time per
// (topic, group, message key). Different keys may run concurrently.
type Handler func(context.Context, *Delivery) HandlerResult

// PublishInput contains an outgoing message and optional business state that
// must become visible atomically with the message. If a caller-provided ID is
// reused, the operation is rejected rather than creating a second record.
type PublishInput struct {
	Topic          string
	Key            string
	ID             string
	Body           []byte
	Headers        map[string]string
	BusinessWrites map[string][]byte

	// NonBlocking returns CodeCapacityExceeded immediately instead of waiting.
	NonBlocking bool
}

// PublishResult reports the stored message. Duplicate is reserved for future
// idempotent submit semantics; the current API rejects duplicate IDs explicitly.
type PublishResult struct {
	Message   *Message
	Duplicate bool
}

// AckOptions may attach local writes to a manual acknowledgement.
type AckOptions struct {
	Reason         string
	BusinessWrites map[string][]byte
}

// NackOptions distinguishes retryable rejection from explicit dead-lettering.
type NackOptions struct {
	Retryable bool
	Reason    string
	Cause     error
}
