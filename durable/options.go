package durable

import "time"

// RetryPolicy configures retry behavior for handler activities.
// Fields map to Temporal's native retry policy.
// When all retries are exhausted and a recovery handler is registered
// (via WithRecoveryHandler on the registration), the recovery handler is
// invoked instead of failing the routine.
type RetryPolicy struct {
	MaxAttempts        int
	InitialInterval    time.Duration
	MaxInterval        time.Duration
	BackoffCoefficient float64
}

// HandlerOptions configures behavior for a registered handler.
// Use HandlerOptions{} for Temporal defaults.
type HandlerOptions struct {
	RetryPolicy            RetryPolicy
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	recoveryHandlerKey string // set internally by WithRecoveryHandler; looked up in worker handlers
}

// RecoveryHandlerKey returns the handler key for the recovery handler,
// or "" if none is registered.
func (o HandlerOptions) RecoveryHandlerKey() string {
	return o.recoveryHandlerKey
}
