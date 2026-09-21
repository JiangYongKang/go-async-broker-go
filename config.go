package broker

import "time"

// Backoff calculates the delay before retryNumber. retryNumber starts at one
// for the first retry. Implementations should be deterministic; random jitter
// can still preserve order because dispatch is key-serialized, but deterministic
// backoff makes local tests and logs easier to reproduce.
type Backoff interface {
	Next(retryNumber int) time.Duration
}

// ExponentialBackoff is Initial * Factor^(retryNumber-1), capped at Maximum.
type ExponentialBackoff struct {
	Initial time.Duration
	Factor  float64
	Maximum time.Duration
}

func (b ExponentialBackoff) Next(retryNumber int) time.Duration {
	if retryNumber < 1 {
		retryNumber = 1
	}
	initial := b.Initial
	if initial <= 0 {
		initial = 20 * time.Millisecond
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2
	}

	d := initial
	for i := 1; i < retryNumber; i++ {
		next := time.Duration(float64(d) * factor)
		// Detect an unreasonable multiplication overflow by treating any value
		// that becomes negative or jumps beyond the configured maximum as max.
		if next <= 0 || (b.Maximum > 0 && next > b.Maximum) {
			if b.Maximum > 0 {
				return b.Maximum
			}
			return d
		}
		d = next
	}
	if b.Maximum > 0 && d > b.Maximum {
		d = b.Maximum
	}
	return d
}

// TopicConfig controls one topic's capacity and retry policy.
type TopicConfig struct {
	// MaxCapacity is the number of retained records in this topic. A zero
	// value uses Config.DefaultCapacity; -1 means unbounded.
	MaxCapacity int

	// MaxAttempts includes the original delivery. A message that fails attempt
	// MaxAttempts and remains retryable is dead-lettered as exhausted.
	MaxAttempts int

	// Backoff calculates delays between attempts. Nil uses Config.DefaultBackoff.
	Backoff Backoff
}

// Config contains broker-wide defaults.
type Config struct {
	// Topics optionally provides per-topic overrides.
	Topics map[string]TopicConfig

	// DefaultCapacity is used by topics that do not specify MaxCapacity.
	DefaultCapacity int

	// DefaultAttempts is used by topics that do not specify MaxAttempts.
	DefaultAttempts int

	// DefaultBackoff is used when neither config supplies a backoff.
	DefaultBackoff Backoff

	// CompletionCacheSize bounds the in-memory acknowledgement history used
	// to distinguish duplicate terminal operations after their durable state
	// has been compacted. Zero uses a built-in default.
	CompletionCacheSize int
}

func defaultBackoff() Backoff {
	return ExponentialBackoff{
		Initial: 20 * time.Millisecond,
		Factor:  2,
		Maximum: time.Second,
	}
}

func (c Config) normalized() Config {
	out := c
	if out.DefaultCapacity == 0 {
		out.DefaultCapacity = 1024
	}
	if out.DefaultAttempts <= 0 {
		out.DefaultAttempts = 5
	}
	if out.DefaultBackoff == nil {
		out.DefaultBackoff = defaultBackoff()
	}
	if out.CompletionCacheSize <= 0 {
		out.CompletionCacheSize = 4096
	}
	if out.Topics == nil {
		out.Topics = make(map[string]TopicConfig)
		return out
	}

	topics := make(map[string]TopicConfig, len(out.Topics))
	for name, tc := range out.Topics {
		if tc.MaxCapacity == 0 {
			tc.MaxCapacity = out.DefaultCapacity
		}
		if tc.MaxAttempts <= 0 {
			tc.MaxAttempts = out.DefaultAttempts
		}
		if tc.Backoff == nil {
			tc.Backoff = out.DefaultBackoff
		}
		topics[name] = tc
	}
	out.Topics = topics
	return out
}

func (c Config) topicConfig(topic string) TopicConfig {
	if tc, ok := c.Topics[topic]; ok {
		if tc.MaxCapacity == 0 {
			tc.MaxCapacity = c.DefaultCapacity
		}
		if tc.MaxAttempts <= 0 {
			tc.MaxAttempts = c.DefaultAttempts
		}
		if tc.Backoff == nil {
			tc.Backoff = c.DefaultBackoff
		}
		return tc
	}
	return TopicConfig{
		MaxCapacity: c.DefaultCapacity,
		MaxAttempts: c.DefaultAttempts,
		Backoff:     c.DefaultBackoff,
	}
}
