package broker

import (
	"context"
	"testing"
	"time"
)

func TestRetryWaitSurvivesRestartAndPreservesBackoff(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultAttempts = 2
	cfg.DefaultBackoff = ExponentialBackoff{Initial: 250 * time.Millisecond, Factor: 2, Maximum: time.Second}
	dir, b := newTestBrokerDir(t, cfg)

	attempts := make(chan int, 4)
	release := make(chan struct{})
	if err := b.Subscribe(context.Background(), "tasks", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		attempts <- d.Attempt
		if d.Attempt == 1 {
			return Retry("temporary before restart", nil)
		}
		<-release
		return AckResult("second attempt after restart", nil)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	publishTestMessage(t, b, "tasks", "k", "m1")
	if got := <-attempts; got != 1 {
		t.Fatalf("first attempt=%d, want 1", got)
	}

	// Wait until retry state is durable before simulated crash.
	waitFor(t, timeout5s, func() bool {
		b.mu.Lock()
		ms := b.state.topics["tasks"].groups["workers"].deliveries["m1"]
		waiting := ms != nil && ms.status == statusRetryWait && ms.visibleAt.After(b.now())
		b.mu.Unlock()
		return waiting
	})
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close during retry wait: %v", err)
	}

	reopened, err := Open(context.Background(), dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopened.SetLogger(discardLogger())
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	ms := reopened.state.topics["tasks"].groups["workers"].deliveries["m1"]
	if ms == nil || ms.status != statusRetryWait || !ms.visibleAt.After(reopened.now()) {
		t.Fatalf("retry state not preserved: %#v", ms)
	}
	t.Logf("test input={message:m1 attempt:1 visible_at:%s now:%s} decision=preserve_backoff", ms.visibleAt, reopened.now())

	waiter := newResultWaiter()
	if err := reopened.Subscribe(context.Background(), "tasks", "workers", waiter.handler(AckResult("second attempt after restart", nil))); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	select {
	case got := <-waiter.ch:
		t.Fatalf("retry happened before backoff expired: %s", got)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case got := <-waiter.ch:
		if got != "m1#2" {
			t.Fatalf("post-restart retry=%s, want m1#2", got)
		}
	case <-time.After(timeout5s):
		t.Fatal("did not redeliver after durable backoff expired")
	}
	close(release)
	waitFor(t, timeout5s, func() bool {
		offsets := reopened.GroupOffsets("tasks", "workers")
		return len(offsets) == 1 && offsets[0].Committed == 1
	})
}
