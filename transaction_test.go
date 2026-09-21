package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishAndBusinessWritesAreAtomic(t *testing.T) {
	dir, b := newTestBrokerDir(t, testConfig())
	_, err := b.Publish(context.Background(), PublishInput{
		Topic: "orders",
		Key:   "k1",
		ID:    "m1",
		Body:  []byte("payload"),
		BusinessWrites: map[string][]byte{
			"business:m1": []byte("committed-effect"),
		},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	value, ok, err := reopened.BusinessGet("business:m1")
	if err != nil || !ok || string(value) != "committed-effect" {
		t.Fatalf("business state after restart value=%q ok=%v err=%v", value, ok, err)
	}
	if stats := reopened.Stats("orders"); stats.Messages != 1 || stats.Retained != 1 {
		t.Fatalf("stats after restart=%+v", stats)
	}
}

func TestDuplicatePublishRejectsAndDoesNotOverwriteState(t *testing.T) {
	b := newTestBroker(t, testConfig())
	input := PublishInput{
		Topic: "orders",
		Key:   "k",
		ID:    "same-id",
		Body:  []byte("first"),
		BusinessWrites: map[string][]byte{
			"state:same": []byte("first"),
		},
	}
	if _, err := b.Publish(context.Background(), input); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	input.Body = []byte("second")
	input.BusinessWrites = map[string][]byte{"state:same": []byte("second")}
	_, err := b.Publish(context.Background(), input)
	if !IsCode(err, CodeDuplicateMessage) {
		t.Fatalf("duplicate publish err=%v, want %s", err, CodeDuplicateMessage)
	}
	t.Logf("test input={id:same-id} decision=reject code=%s reason=duplicate_message", CodeDuplicateMessage)

	value, ok, err := b.BusinessGet("state:same")
	if err != nil || !ok || string(value) != "first" {
		t.Fatalf("state contaminated by duplicate: value=%q ok=%v err=%v", value, ok, err)
	}
	if stats := b.Stats("orders"); stats.Messages != 1 || stats.Retained != 1 {
		t.Fatalf("duplicate publish changed retained state: %+v", stats)
	}
}

func TestTornTrailingTransactionIsIgnoredOnRestart(t *testing.T) {
	dir, b := newTestBrokerDir(t, testConfig())
	publishTestMessage(t, b, "orders", "k", "committed")
	if _, err := b.Publish(context.Background(), PublishInput{
		Topic:          "orders",
		Key:            "k",
		ID:             "also-committed",
		Body:           []byte("x"),
		BusinessWrites: map[string][]byte{"committed": []byte("yes")},
	}); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	walPath := filepath.Join(dir, walName)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	// Simulate a crash after a partial frame was appended but before the batch
	// end and fsync boundary.
	if _, err := f.Write([]byte{0x42, 0x52, 0x4f, 0x4b, frameData, 0, 0, 0}); err != nil {
		t.Fatalf("append torn bytes: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn WAL: %v", err)
	}

	reopened, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("reopen torn WAL through public API: %v", err)
	}
	reopened.SetLogger(discardLogger())
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	if value, ok, _ := reopened.BusinessGet("committed"); !ok || string(value) != "yes" {
		t.Fatalf("committed batch not preserved: value=%q ok=%v", value, ok)
	}
	if stats := reopened.Stats("orders"); stats.Messages != 2 {
		t.Fatalf("committed messages after trim=%d, want 2", stats.Messages)
	}
	if _, err := reopened.Publish(context.Background(), PublishInput{Topic: "orders", Key: "k", ID: "after-trim"}); err != nil {
		t.Fatalf("publish after trim recovery: %v", err)
	}
	if err := reopened.Close(context.Background()); err != nil {
		t.Fatalf("close recovered broker: %v", err)
	}

	final, err := Open(context.Background(), dir, testConfig())
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	t.Cleanup(func() { _ = final.Close(context.Background()) })
	if stats := final.Stats("orders"); stats.Messages != 3 {
		t.Fatalf("messages after recovered append=%d, want 3; stats=%+v", stats.Messages, stats)
	}
}

func openStoreForInspection(t *testing.T, dir string) (*Broker, bool, error) {
	t.Helper()
	store, err := OpenStore(dir)
	if err != nil {
		return nil, false, err
	}
	rawEntries, trimmed, err := store.ReadEntries()
	if err != nil {
		_ = store.Close()
		return nil, false, err
	}
	events, err := decodeEvents(rawEntries)
	if err != nil {
		_ = store.Close()
		return nil, false, err
	}
	state, err := replay(events)
	if err != nil {
		_ = store.Close()
		return nil, false, err
	}
	cfg := testConfig().normalized()
	b := &Broker{dir: dir, cfg: cfg, store: store, state: state, nowFunc: timeNowForTest, idFunc: randomID, subs: map[string]*brokerSubscription{}, activeDispatch: map[string]map[string]bool{}, completion: map[string]terminalCompletion{}}
	return b, trimmed, nil
}
