package broker

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		DefaultCapacity: 8,
		DefaultAttempts: 3,
		DefaultBackoff: ExponentialBackoff{
			Initial: time.Millisecond,
			Factor:  2,
			Maximum: 3 * time.Millisecond,
		},
	}
}

func newTestBroker(t *testing.T, cfg Config) *Broker {
	t.Helper()
	dir := t.TempDir()
	b, err := Open(context.Background(), dir, cfg)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	b.SetLogger(log.New(io.Discard, "", 0))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	})
	return b
}

func newTestBrokerDir(t *testing.T, cfg Config) (string, *Broker) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	b, err := Open(context.Background(), dir, cfg)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	b.SetLogger(log.New(io.Discard, "", 0))
	return dir, b
}

func publishTestMessage(t *testing.T, b *Broker, topic, key, id string) {
	t.Helper()
	_, err := b.Publish(context.Background(), PublishInput{
		Topic: topic,
		Key:   key,
		ID:    id,
		Body:  []byte("body-" + id),
	})
	if err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

const timeout5s = 5 * time.Second

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached before timeout")
}

func discardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

type resultWaiter struct {
	mu   sync.Mutex
	seen []string

	ch chan string
}

func newResultWaiter() *resultWaiter {
	return &resultWaiter{ch: make(chan string, 128)}
}

func (w *resultWaiter) handler(result HandlerResult) Handler {
	return func(ctx context.Context, d *Delivery) HandlerResult {
		w.mu.Lock()
		w.seen = append(w.seen, d.Message.ID+"#"+itoa(d.Attempt))
		w.mu.Unlock()
		w.ch <- d.Message.ID + "#" + itoa(d.Attempt)
		return result
	}
}

func (w *resultWaiter) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-w.ch:
		t.Logf("test input=%q decision=observed result=%q", want, got)
		if got != want {
			t.Fatalf("got delivery %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q; seen=%v", want, w.seen)
	}
}

func TestMinimumSkeletonCompiles(t *testing.T) {
	// This test documents that the package API can be opened in an empty local
	// directory and closed without producing a message or a retained file error.
	dir, err := os.MkdirTemp("", "broker-skeleton-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	b, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func timeNowForTest() time.Time { return time.Now() }
