package broker

import (
	"context"
	"sync"
	"testing"
)

func TestGroupKeyOrderStableAcrossRetry(t *testing.T) {
	b := newTestBroker(t, testConfig())

	var mu sync.Mutex
	var completed []string
	started := make(chan string, 16)
	releaseA := make(chan struct{})
	attempts := make(map[string]int)

	err := b.Subscribe(context.Background(), "tasks", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		mu.Lock()
		attempts[d.Message.ID]++
		attempt := attempts[d.Message.ID]
		mu.Unlock()

		t.Logf("test input={message:%s key:%s attempt:%d} decision=dispatched", d.Message.ID, d.Message.Key, attempt)
		started <- d.Message.ID + "#" + itoa(attempt)

		if d.Message.Key == "A" && d.Message.ID == "a1" && attempt < 3 {
			return Retry("temporary A failure", nil)
		}
		if d.Message.ID == "a2" {
			// Do not acknowledge until the test proves a2 was not delivered
			// before a1 reached its terminal attempt.
			<-releaseA
		}

		mu.Lock()
		completed = append(completed, d.Message.ID)
		mu.Unlock()
		return AckResult("processed in key order", nil)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishTestMessage(t, b, "tasks", "A", "a1")
	if got := <-started; got != "a1#1" {
		t.Fatalf("first delivery = %s, want a1#1", got)
	}
	if got := <-started; got != "a1#2" {
		t.Fatalf("retry delivery = %s, want a1#2", got)
	}

	publishTestMessage(t, b, "tasks", "A", "a2")
	publishTestMessage(t, b, "tasks", "B", "b1")

	if got := <-started; got != "b1#1" {
		t.Fatalf("other key delivery = %s, want b1#1", got)
	}
	if got := <-started; got != "a1#3" {
		t.Fatalf("third a1 delivery = %s, want a1#3", got)
	}

	// Only now is a2 allowed to run. The dispatcher may wake repeatedly, but it
	// must not have started it while a1 was still the ordered queue head.
	select {
	case early := <-started:
		t.Fatalf("same-group key order violated, saw %s before releasing a2", early)
	default:
	}
	close(releaseA)

	if got := <-started; got != "a2#1" {
		t.Fatalf("post-retry delivery = %s, want a2#1", got)
	}

	waitFor(t, timeout5s, func() bool {
		offsets := b.GroupOffsets("tasks", "workers")
		var aBacklog, bBacklog int
		for _, o := range offsets {
			if o.Key == "A" {
				aBacklog = o.Backlog
			}
			if o.Key == "B" {
				bBacklog = o.Backlog
			}
		}
		return aBacklog == 0 && bBacklog == 0
	})
	if got := len(b.DeadLetters("tasks", "workers")); got != 0 {
		t.Fatalf("dead letters = %d, want 0", got)
	}
}
