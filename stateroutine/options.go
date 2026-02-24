package stateroutine

import "time"

// RetryPolicy configures retry behavior for handler activities.
// Fields map to Temporal's native retry policy.
// When all retries are exhausted and a terminal error handler is registered
// (via WithTerminalErrorHandler on the registration), the terminal error handler is
// invoked instead of failing the stateroutine.
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
	terminalErrorHandlerKey string // set internally by WithTerminalErrorHandler; looked up in worker handlers
}

// WithTerminalErrorHandlerKey returns the handler key for the terminal error handler,
// or "" if none is registered.
func (o HandlerOptions) WithTerminalErrorHandlerKey() string {
	return o.terminalErrorHandlerKey
}
