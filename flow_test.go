package broker

import (
	"context"
	"testing"
)

func TestProduceConsumeAckAndRestart(t *testing.T) {
	dir, b := newTestBrokerDir(t, testConfig())
	waiter := newResultWaiter()
	err := b.Subscribe(context.Background(), "orders", "workers", waiter.handler(AckResult("processed", map[string][]byte{
		"business:m1": []byte("effect-1"),
	})))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishTestMessage(t, b, "orders", "customer-1", "m1")
	waiter.expect(t, "m1#1")
	waitFor(t, timeout5s, func() bool {
		offsets := b.GroupOffsets("orders", "workers")
		return len(offsets) == 1 && offsets[0].Committed == 1 && offsets[0].Backlog == 0
	})
	if value, ok, err := b.BusinessGet("business:m1"); err != nil || !ok || string(value) != "effect-1" {
		t.Fatalf("business write after ack: value=%q ok=%v err=%v", value, ok, err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopened.SetLogger(discardLogger())
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	if value, ok, err := reopened.BusinessGet("business:m1"); err != nil || !ok || string(value) != "effect-1" {
		t.Fatalf("business write after restart: value=%q ok=%v err=%v", value, ok, err)
	}
	offsets := reopened.GroupOffsets("orders", "workers")
	if len(offsets) != 1 || offsets[0].Committed != 1 || offsets[0].Backlog != 0 {
		t.Fatalf("offsets after restart=%+v", offsets)
	}
	if len(reopened.DeadLetters("orders", "workers")) != 0 {
		t.Fatal("successful message entered DLQ")
	}
}
