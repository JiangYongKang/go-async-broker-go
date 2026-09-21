package broker

import (
	"errors"
	"fmt"
)

// ErrorCode is a stable, machine-readable classification used by callers and
// tests instead of matching human-readable error text.
type ErrorCode string

const (
	CodeUnknownDelivery      ErrorCode = "UNKNOWN_DELIVERY"
	CodeAlreadyAcked         ErrorCode = "ALREADY_ACKED"
	CodeAlreadyDeadLettered  ErrorCode = "ALREADY_DEAD_LETTERED"
	CodeStaleDeliveryAttempt ErrorCode = "STALE_DELIVERY_ATTEMPT"
	CodeRetryExhausted       ErrorCode = "RETRY_EXHAUSTED"
	CodeNonRetryable         ErrorCode = "NON_RETRYABLE"
	CodeCapacityExceeded     ErrorCode = "CAPACITY_EXCEEDED"
	CodeDuplicateMessage     ErrorCode = "DUPLICATE_MESSAGE"
	CodeOffsetRollback       ErrorCode = "OFFSET_ROLLBACK"
	CodeInvalidArgument      ErrorCode = "INVALID_ARGUMENT"
	CodeBrokerClosed         ErrorCode = "BROKER_CLOSED"
	CodeCorruptStorage       ErrorCode = "CORRUPT_STORAGE"
	CodeLocked               ErrorCode = "BROKER_LOCKED"
)

// Error describes why an operation was rejected. Reason is a short stable
// explanation; Err optionally carries the underlying cause.
type Error struct {
	Code    ErrorCode
	Reason  string
	Message string
	Err     error
}

func (e *Error) Error() string {
	msg := string(e.Code)
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	if e.Message != "" {
		msg += " (" + e.Message + ")"
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

// AsBrokerError extracts the broker's classified error.
func AsBrokerError(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

// IsCode reports whether err has the specified broker error code.
func IsCode(err error, code ErrorCode) bool {
	if be, ok := AsBrokerError(err); ok {
		return be.Code == code
	}
	return false
}

func brokerError(code ErrorCode, reason, message string, cause error) *Error {
	return &Error{Code: code, Reason: reason, Message: message, Err: cause}
}

func invalidf(format string, args ...any) error {
	return brokerError(CodeInvalidArgument, "invalid argument", fmt.Sprintf(format, args...), nil)
}

var (
	ErrBrokerClosed      = brokerError(CodeBrokerClosed, "broker is closed", "", nil)
	ErrOffsetRollback    = brokerError(CodeOffsetRollback, "offset cannot move backward", "", nil)
	ErrUnknownDelivery   = brokerError(CodeUnknownDelivery, "delivery token is unknown", "", nil)
	ErrAlreadyAcked      = brokerError(CodeAlreadyAcked, "delivery was already acknowledged", "", nil)
	ErrAlreadyDeadLetter = brokerError(CodeAlreadyDeadLettered, "delivery was already dead-lettered", "", nil)
)
