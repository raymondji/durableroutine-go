package stateroutine

import "time"

// ErrorPolicy configures retry and error handling behavior for handler activities.
// Fields map to Temporal's native retry policy and activity timeout options.
// When all retries are exhausted and a terminal error handler is registered,
// the terminal error handler is invoked instead of failing the routine.
type ErrorPolicy struct {
	MaxAttempts            int
	InitialInterval        time.Duration
	MaxInterval            time.Duration
	BackoffCoefficient     float64
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	onTerminalErrorKey     string // set internally by addEntry; looked up in worker handlers
}
