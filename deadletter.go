package broker

import "time"

// DeadLetterEntry retains a message that must not be silently dropped.
//
// Reason is one of the stable decision explanations:
//   - NON_RETRYABLE: handler explicitly marked the cause terminal;
//   - RETRY_EXHAUSTED: every configured attempt failed with a retryable cause.
type DeadLetterEntry struct {
	Topic     string
	Group     string
	MessageID string
	Offset    int64
	Key       string
	Body      []byte
	Headers   map[string]string
	Attempts  int
	Reason    string
	LastError string
	DeadAt    time.Time
}

func (d DeadLetterEntry) clone() DeadLetterEntry {
	cp := d
	cp.Body = append([]byte(nil), d.Body...)
	if d.Headers != nil {
		cp.Headers = make(map[string]string, len(d.Headers))
		for k, v := range d.Headers {
			cp.Headers[k] = v
		}
	}
	return cp
}
