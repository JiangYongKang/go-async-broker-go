package broker

import (
	"context"
	"testing"
)

func TestDuplicateStaleAndUnknownAcknowledgements(t *testing.T) {
	b := newTestBroker(t, testConfig())
	deliveries := make(chan *Delivery, 4)
	release := make(map[string]chan struct{})
	release["m1"] = make(chan struct{})
	release["m2"] = make(chan struct{})

	err := b.Subscribe(context.Background(), "acks", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		deliveries <- d
		if gate := release[d.Message.ID]; gate != nil {
			<-gate
		}
		return AckResult("manual handler completed", nil)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	publishTestMessage(t, b, "acks", "k", "m1")

	d1 := <-deliveries
	t.Logf("test input={message:%s delivery:%s attempt:%d} decision=manual_ack", d1.Message.ID, d1.ID, d1.Attempt)
	if err := b.Ack(context.Background(), d1); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	close(release["m1"])

	err = b.Ack(context.Background(), d1)
	if !IsCode(err, CodeAlreadyAcked) {
		t.Fatalf("duplicate ack err=%v, want code=%s", err, CodeAlreadyAcked)
	}
	t.Logf("test input={delivery:%s} decision=reject code=%s reason=already_acked", d1.ID, CodeAlreadyAcked)

	unknown := d1.clone()
	unknown.ID = "does-not-exist"
	err = b.Ack(context.Background(), unknown)
	if !IsCode(err, CodeUnknownDelivery) {
		t.Fatalf("unknown ack err=%v, want code=%s", err, CodeUnknownDelivery)
	}
	t.Logf("test input={delivery:%s} decision=reject code=%s reason=unknown_delivery", unknown.ID, CodeUnknownDelivery)

	publishTestMessage(t, b, "acks", "k", "m2")
	d2 := <-deliveries
	stale := d2.clone()
	stale.ID = "old-delivery-token"
	err = b.Ack(context.Background(), stale)
	if !IsCode(err, CodeStaleDeliveryAttempt) {
		t.Fatalf("stale ack err=%v, want code=%s (current=%s old=%s)", err, CodeStaleDeliveryAttempt, d2.ID, stale.ID)
	}
	t.Logf("test input={message:m2 old_delivery:%s current_delivery:%s} decision=reject code=%s reason=superseded_attempt", stale.ID, d2.ID, CodeStaleDeliveryAttempt)

	if err := b.Ack(context.Background(), d2); err != nil {
		t.Fatalf("ack second delivery: %v", err)
	}
	close(release["m2"])
	waitFor(t, timeout5s, func() bool {
		offsets := b.GroupOffsets("acks", "workers")
		return len(offsets) == 1 && offsets[0].Committed == 2 && offsets[0].Backlog == 0
	})
}
