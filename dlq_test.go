package broker

import (
	"context"
	"testing"
	"time"
)

func TestDeadLetterDeleteReleasesCapacityAndPersists(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCapacity = 1
	dir, b := newTestBrokerDir(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.Subscribe(ctx, "dlq", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		return DeadLetter("explicit terminal cause", nil)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	publishTestMessage(t, b, "dlq", "k", "m1")
	waitFor(t, timeout5s, func() bool { return len(b.DeadLetters("dlq", "workers")) == 1 })
	before := b.Stats("dlq")
	if before.Retained != 1 || before.DeadLetters != 1 || !before.Full {
		t.Fatalf("stats before delete=%+v", before)
	}
	_, err := b.Publish(context.Background(), PublishInput{Topic: "dlq", Key: "k", ID: "blocked", NonBlocking: true})
	if !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("capacity before delete err=%v", err)
	}

	if err := b.DeleteDeadLetter(context.Background(), "dlq", "workers", "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Stop the local handler so the capacity-probe publish is retained as a
	// pending message instead of immediately becoming a new dead letter. The
	// durable consumer group is still replayed on restart.
	cancel()
	time.Sleep(20 * time.Millisecond)
	if err := b.DeleteDeadLetter(context.Background(), "dlq", "workers", "m1"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	if len(b.DeadLetters("dlq", "workers")) != 0 {
		t.Fatal("dead letter still visible after delete")
	}
	if _, err := b.Publish(context.Background(), PublishInput{Topic: "dlq", Key: "k", ID: "after"}); err != nil {
		t.Fatalf("publish after delete: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(context.Background(), dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close(context.Background())
	if len(reopened.DeadLetters("dlq", "workers")) != 0 {
		t.Fatal("deleted dead letter reappeared after restart")
	}
}
