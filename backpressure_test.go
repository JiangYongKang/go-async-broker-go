package broker

import (
	"context"
	"testing"
	"time"
)

func TestCapacityRejectsNonBlockingAndUnblocksAfterTerminal(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCapacity = 1
	cfg.DefaultBackoff = ExponentialBackoff{Initial: time.Millisecond, Factor: 2, Maximum: time.Millisecond}
	b := newTestBroker(t, cfg)

	if _, err := b.Publish(context.Background(), PublishInput{Topic: "cap", Key: "k", ID: "m1"}); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	_, err := b.Publish(context.Background(), PublishInput{Topic: "cap", Key: "k", ID: "m2", NonBlocking: true})
	if !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("full topic err=%v, want %s", err, CodeCapacityExceeded)
	}
	t.Logf("test input={topic:cap retained:1 capacity:1} decision=reject code=%s reason=capacity_exceeded", CodeCapacityExceeded)
	if stats := b.Stats("cap"); !stats.Full || stats.Retained != 1 {
		t.Fatalf("stats at capacity=%+v", stats)
	}

	waiter := newResultWaiter()
	if err := b.Subscribe(context.Background(), "cap", "workers", waiter.handler(AckResult("consumed", nil))); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waiter.expect(t, "m1#1")
	waitFor(t, timeout5s, func() bool {
		stats := b.Stats("cap")
		return stats.Retained == 0 && !stats.Full
	})

	_, err = b.Publish(context.Background(), PublishInput{Topic: "cap", Key: "k", ID: "m2", NonBlocking: true})
	if err != nil {
		t.Fatalf("publish after capacity release: %v", err)
	}
}

func TestBlockingPublishWaitsAndAcceptedMessageIsNotLost(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCapacity = 1
	b := newTestBroker(t, cfg)

	publishTestMessage(t, b, "cap", "k", "m1")
	stats := b.Stats("cap")
	if !stats.Full || stats.Retained != 1 {
		t.Fatalf("precondition stats=%+v", stats)
	}

	done := make(chan error, 1)
	go func() {
		_, err := b.Publish(context.Background(), PublishInput{Topic: "cap", Key: "k", ID: "m2", Body: []byte("waiting")})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("blocking publisher returned before capacity release: err=%v", err)
	case <-time.After(30 * time.Millisecond):
	}

	waiter := newResultWaiter()
	if err := b.Subscribe(context.Background(), "cap", "workers", waiter.handler(AckResult("consumed", nil))); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waiter.expect(t, "m1#1")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocking publish after release: %v", err)
		}
	case <-time.After(timeout5s):
		t.Fatal("blocking publisher did not resume after capacity release")
	}
	waiter.expect(t, "m2#1")
	waitFor(t, timeout5s, func() bool {
		offsets := b.GroupOffsets("cap", "workers")
		return len(offsets) == 1 && offsets[0].Committed == 2
	})
}
