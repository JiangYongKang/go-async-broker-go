package broker

import (
	"context"
	"testing"
)

func TestInflightDeliveryRecoversWithoutOffsetAdvance(t *testing.T) {
	dir, b := newTestBrokerDir(t, testConfig())
	started := make(chan *Delivery, 1)
	proceed := make(chan struct{})
	if err := b.Subscribe(context.Background(), "tasks", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		started <- d
		<-proceed
		return AckResult("after recovery", nil)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	publishTestMessage(t, b, "tasks", "k", "m1")
	d := <-started
	if d.Attempt != 1 {
		t.Fatalf("first attempt=%d, want 1", d.Attempt)
	}
	t.Logf("test input={message:%s delivery:%s} decision=simulate_crash_before_terminal_commit", d.Message.ID, d.ID)

	// Simulate a hard process crash: close files without releasing the handler
	// result. The durable lease is already present.
	if err := b.store.Close(); err != nil {
		t.Fatalf("crash close: %v", err)
	}
	close(proceed)

	reopened, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopened.SetLogger(discardLogger())
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	offsets := reopened.GroupOffsets("tasks", "workers")
	if len(offsets) != 1 || offsets[0].Committed != 0 || offsets[0].Backlog != 1 {
		t.Fatalf("offset after crash=%+v", offsets)
	}

	waiter := newResultWaiter()
	if err := reopened.Subscribe(context.Background(), "tasks", "workers", waiter.handler(AckResult("after recovery", nil))); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	waiter.expect(t, "m1#1")
	waitFor(t, timeout5s, func() bool {
		os := reopened.GroupOffsets("tasks", "workers")
		return len(os) == 1 && os[0].Committed == 1 && os[0].Backlog == 0
	})
}
