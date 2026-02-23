package stateroutine

import "time"

// RetryPolicy configures retry behavior for handler activities.
// Fields map to Temporal's native retry policy and activity timeout options.
// When all retries are exhausted and a terminal error handler is registered
// (via OnTerminalError on the registration), the terminal error handler is
// invoked instead of failing the stateroutine.
type RetryPolicy struct {
	MaxAttempts            int
	InitialInterval        time.Duration
	MaxInterval            time.Duration
	BackoffCoefficient     float64
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
}

// HandlerOptions configures behavior for a registered handler.
// RetryPolicy controls retry and timeout settings.
// Use HandlerOptions{} for Temporal defaults.
type HandlerOptions struct {
	RetryPolicy        RetryPolicy
	onTerminalErrorKey string // set internally by OnTerminalError; looked up in worker handlers
}
