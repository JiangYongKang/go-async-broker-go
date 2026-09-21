package broker

import (
	"context"
	"strings"
	"testing"
)

func TestRetryExhaustionAndNonRetryableDeadLetter(t *testing.T) {
	dir, b := newTestBrokerDir(t, testConfig())
	events := make(chan string, 16)
	err := b.Subscribe(context.Background(), "failures", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		if d.Message.ID == "bad" {
			t.Logf("test input={message:%s attempt:%d} decision=dead_letter reason=explicit_non_retryable", d.Message.ID, d.Attempt)
			events <- d.Message.ID + "#" + itoa(d.Attempt)
			return DeadLetter("explicit bad payload", nil)
		}
		t.Logf("test input={message:%s attempt:%d max_attempts:3} decision=retry reason=temporary", d.Message.ID, d.Attempt)
		events <- d.Message.ID + "#" + itoa(d.Attempt)
		return Retry("temporary failure", nil)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishTestMessage(t, b, "failures", "retry", "flaky")
	publishTestMessage(t, b, "failures", "poison", "bad")

	var got []string
	for i := 0; i < 4; i++ {
		got = append(got, <-events)
	}
	// Different keys may run concurrently; order is asserted only within each
	// key, not across keys.
	flakyAttempts := map[string]bool{}
	hasBad := false
	for _, item := range got {
		if item == "bad#1" {
			hasBad = true
		}
		if strings.HasPrefix(item, "flaky#") {
			flakyAttempts[item] = true
		}
	}
	for _, want := range []string{"flaky#1", "flaky#2", "flaky#3"} {
		if !flakyAttempts[want] {
			t.Fatalf("missing %q in deliveries=%v", want, got)
		}
	}
	if !hasBad {
		t.Fatalf("missing bad#1 in deliveries=%v", got)
	}

	waitFor(t, timeout5s, func() bool {
		return len(b.DeadLetters("failures", "workers")) == 2
	})
	entries := b.DeadLetters("failures", "workers")
	byID := make(map[string]DeadLetterEntry, len(entries))
	for _, entry := range entries {
		byID[entry.MessageID] = entry
		t.Logf("test input={message:%s attempts:%d} decision=dead_letter reason=%q last_error=%q", entry.MessageID, entry.Attempts, entry.Reason, entry.LastError)
	}

	flaky := byID["flaky"]
	if flaky.Attempts != 3 {
		t.Fatalf("flaky attempts = %d, want 3", flaky.Attempts)
	}
	if !strings.Contains(flaky.Reason, "retry exhausted") {
		t.Fatalf("flaky reason = %q, want retry exhausted marker", flaky.Reason)
	}

	bad := byID["bad"]
	if bad.Attempts != 1 {
		t.Fatalf("bad attempts = %d, want 1", bad.Attempts)
	}
	if !strings.Contains(bad.Reason, "non-retryable") {
		t.Fatalf("bad reason = %q, want non-retryable marker", bad.Reason)
	}

	stats := b.Stats("failures")
	if stats.DeadLetters != 2 {
		t.Fatalf("DLQ count=%d, want 2", stats.DeadLetters)
	}

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close before dead-letter capacity replay: %v", err)
	}
	reopened, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("reopen dead-letter state: %v", err)
	}
	defer reopened.Close(context.Background())
	stats = reopened.Stats("failures")
	if stats.Retained != 2 || stats.DeadLetters != 2 || stats.Messages != 0 {
		t.Fatalf("capacity after DLQ replay=%+v, want retained=2 deadletters=2 live=0", stats)
	}
	if len(reopened.DeadLetters("failures", "workers")) != 2 {
		t.Fatal("dead letters were not preserved after restart")
	}
}
