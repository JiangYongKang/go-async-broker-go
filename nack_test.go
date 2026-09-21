package broker

import (
	"context"
	"errors"
	"testing"
)

func TestNonRetryableNackIsTerminalAndRejectsDuplicate(t *testing.T) {
	b := newTestBroker(t, testConfig())
	started := make(chan *Delivery, 1)
	release := make(chan struct{})
	if err := b.Subscribe(context.Background(), "nacks", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		started <- d
		<-release
		return AckResult("handler sees terminal result", nil)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	publishTestMessage(t, b, "nacks", "k", "m1")
	d := <-started

	cause := errors.New("schema version unsupported")
	t.Logf("test input={message:%s delivery:%s retryable:false cause:%q} decision=dead_letter", d.Message.ID, d.ID, cause)
	if err := b.Nack(context.Background(), d, NackOptions{
		Retryable: false,
		Reason:    "unsupported schema",
		Cause:     cause,
	}); err != nil {
		t.Fatalf("first nack: %v", err)
	}
	close(release)

	err := b.Nack(context.Background(), d, NackOptions{Retryable: false, Reason: "duplicate rejection"})
	if !IsCode(err, CodeAlreadyDeadLettered) {
		t.Fatalf("duplicate nack err=%v, want %s", err, CodeAlreadyDeadLettered)
	}
	t.Logf("test input={delivery:%s} decision=reject code=%s reason=already_dead_lettered", d.ID, CodeAlreadyDeadLettered)

	entries := b.DeadLetters("nacks", "workers")
	if len(entries) != 1 {
		t.Fatalf("dead letters=%d, want 1", len(entries))
	}
	if entries[0].LastError != cause.Error() || entries[0].Attempts != 1 {
		t.Fatalf("dead letter entry=%+v", entries[0])
	}
	stats := b.Stats("nacks")
	if stats.DeadLetters != 1 || stats.Retained != 1 {
		t.Fatalf("stats after duplicate nack=%+v", stats)
	}
}
