package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type brokerSubscription struct {
	topic    string
	group    string
	handler  Handler
	notify   chan string
	shutdown chan struct{}
	done     chan struct{}
	stopped  bool
}

// Broker is a local durable, asynchronous message and task engine.
type Broker struct {
	dir     string
	cfg     Config
	store   *Store
	logger  *log.Logger
	nowFunc func() time.Time
	idFunc  func() (string, error)

	capacityWaiters []chan struct{}
	mu              sync.Mutex
	state           *engineState
	closed          bool
	subs            map[string]*brokerSubscription
	activeDispatch  map[string]map[string]bool
	completion      map[string]terminalCompletion
	wg              sync.WaitGroup
}

type terminalCompletion struct {
	decision HandlerDecision
	attempt  int
	reason   string
}

// Open loads committed state from dir and starts retry/dispatch timers.
func Open(ctx context.Context, dir string, cfg Config) (*Broker, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cfg = cfg.normalized()
	store, err := OpenStore(dir)
	if err != nil {
		return nil, err
	}
	rawEntries, _, err := store.ReadEntries()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	events, err := decodeEvents(rawEntries)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	state, err := replay(events)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	b := &Broker{
		dir:            dir,
		cfg:            cfg,
		store:          store,
		logger:         log.New(os.Stderr, "broker: ", log.LstdFlags|log.Lmicroseconds),
		nowFunc:        time.Now,
		idFunc:         randomID,
		state:          state,
		subs:           make(map[string]*brokerSubscription),
		activeDispatch: make(map[string]map[string]bool),
		completion:     make(map[string]terminalCompletion),
	}

	if err := b.resumeAfterOpen(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return b, nil
}

// SetLogger overrides the default stderr logger. It must be called before
// subscriptions or publishes.
func (b *Broker) SetLogger(logger *log.Logger) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if logger != nil {
		b.logger = logger
	}
}

// Close stops dispatch and flushes the durable log.
func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := make([]*brokerSubscription, 0, len(b.subs))
	for _, sub := range b.subs {
		if !sub.stopped {
			sub.stopped = true
			close(sub.shutdown)
			subs = append(subs, sub)
		}
	}
	b.broadcastCapacity()
	b.mu.Unlock()

	_ = ctx // Timers observe shutdown directly; callers can still cancel context.
	b.wg.Wait()
	return b.store.Close()
}

// BusinessGet reads state committed in the same transaction as a publish or
// acknowledgement.
func (b *Broker) BusinessGet(key string) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	value, ok := b.state.business[key]
	return append([]byte(nil), value...), ok, nil
}

func (b *Broker) now() time.Time {
	return b.nowFunc()
}

func (b *Broker) nextID() (string, error) {
	return b.idFunc()
}

func randomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", brokerError(CodeInvalidArgument, "generate random ID", "", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func subscriptionKey(topic, group string) string {
	return topic + "\x00" + group
}

func (b *Broker) logf(format string, args ...any) {
	b.logger.Output(2, fmt.Sprintf(format, args...))
}

func (b *Broker) broadcastCapacity() {
	waiters := b.capacityWaiters
	b.capacityWaiters = nil
	for _, waiter := range waiters {
		close(waiter)
	}
}

func (b *Broker) removeCapacityWaiter(target chan struct{}) {
	for i, waiter := range b.capacityWaiters {
		if waiter == target {
			b.capacityWaiters = append(b.capacityWaiters[:i], b.capacityWaiters[i+1:]...)
			return
		}
	}
}
