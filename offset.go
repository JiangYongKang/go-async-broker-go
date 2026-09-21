package broker

// validateOffsetAdvance checks a candidate durable offset against the current
// ordered head.
//
//   - next == current: harmless idempotent replay,
//   - next > current: normal forward advance,
//   - next < current: explicit offset rollback and always rejected.
func validateOffsetAdvance(current int64, next int64) error {
	if next < current {
		return brokerError(
			CodeOffsetRollback,
			"offset cannot move backward",
			"current="+itoa64(current)+", requested="+itoa64(next),
			nil,
		)
	}
	return nil
}

// validateStrictOffsetStep additionally ensures the head advances exactly one
// message, preventing data alignment bugs from jumping across a gap.
func validateStrictOffsetStep(current int64, next int64) error {
	if err := validateOffsetAdvance(current, next); err != nil {
		return err
	}
	if next > current+1 {
		return brokerError(
			CodeOffsetRollback,
			"offset cannot skip ahead",
			"current="+itoa64(current)+", requested="+itoa64(next),
			nil,
		)
	}
	return nil
}
