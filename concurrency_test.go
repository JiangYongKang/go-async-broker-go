package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentProducersConsumersAndCommitsAreConsistent(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultCapacity = -1
	cfg.DefaultAttempts = 2
	cfg.DefaultBackoff = ExponentialBackoff{Initial: time.Millisecond, Factor: 2, Maximum: time.Millisecond}
	b := newTestBroker(t, cfg)

	const producers = 8
	const perProducer = 20
	total := producers * perProducer

	var handled int64
	seenMu := sync.Mutex{}
	seen := make(map[string]int)
	var mu sync.Mutex
	order := map[string][]string{}
	prevOffset := map[string]int{}
	prevID := map[string]string{}

	err := b.Subscribe(context.Background(), "events", "workers", func(ctx context.Context, d *Delivery) HandlerResult {
		mu.Lock()
		seq := int(d.Message.Offset)
		lastID, redelivery := prevID[d.Message.Key]
		if redelivery && lastID != d.Message.ID && seq <= prevOffset[d.Message.Key] {
			mu.Unlock()
			t.Errorf("offset ordering violation key=%s last=%d current=%d", d.Message.Key, prevOffset[d.Message.Key], seq)
			return DeadLetter("offset ordering violation", nil)
		}
		prevOffset[d.Message.Key] = seq
		prevID[d.Message.Key] = d.Message.ID
		order[d.Message.Key] = append(order[d.Message.Key], d.Message.ID)
		mu.Unlock()

		seenMu.Lock()
		seen[d.Message.ID]++
		count := seen[d.Message.ID]
		seenMu.Unlock()

		n := atomic.AddInt64(&handled, 1)
		if d.ID == "" {
			t.Errorf("empty delivery id")
		}
		// Occasional retry on first attempt proves retry and later ack remain
		// key-ordered without losing business writes.
		if n%37 == 0 && count == 1 {
			return Retry("scheduled concurrent transient failure", nil)
		}
		return AckResult("processed", map[string][]byte{
			"effect:" + d.Message.ID: []byte(fmt.Sprintf("attempt-%d", d.Attempt)),
		})
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var publishWG sync.WaitGroup
	errCh := make(chan error, producers)
	for p := 0; p < producers; p++ {
		publishWG.Add(1)
		go func(producer int) {
			defer publishWG.Done()
			for i := 0; i < perProducer; i++ {
				id := fmt.Sprintf("p%d-m%02d", producer, i)
				key := fmt.Sprintf("key-%d", i%5)
				_, err := b.Publish(context.Background(), PublishInput{
					Topic: "events",
					Key:   key,
					ID:    id,
					Body:  []byte(id),
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(p)
	}
	publishWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent publish: %v", err)
		}
	}

	waitFor(t, 10*time.Second, func() bool {
		offsets := b.GroupOffsets("events", "workers")
		var backlog int
		for _, o := range offsets {
			backlog += o.Backlog
		}
		return backlog == 0
	})

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != total {
		t.Fatalf("unique handled messages=%d, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count < 1 || count > 2 {
			t.Fatalf("message %s handled %d times, want 1 or 2", id, count)
		}
		if value, ok, _ := b.BusinessGet("effect:" + id); !ok || string(value) == "" {
			t.Fatalf("missing business effect for %s", id)
		}
	}
	if len(b.DeadLetters("events", "workers")) != 0 {
		t.Fatalf("unexpected dead letters: %+v", b.DeadLetters("events", "workers"))
	}
	for key, ids := range order {
		if len(ids) == 0 {
			t.Fatalf("empty order for key %s", key)
		}
		t.Logf("test input={key:%s messages:%d} decision=in_order offsets_verified", key, len(ids))
	}
}
