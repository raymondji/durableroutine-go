package durable

import "time"

// RetryPolicy configures retry behavior for handler activities.
// Fields map to Temporal's native retry policy and activity timeout options.
type RetryPolicy struct {
	MaxAttempts            int
	InitialInterval        time.Duration
	MaxInterval            time.Duration
	BackoffCoefficient     float64
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
}

// HandlerOption configures handler registration.
type HandlerOption func(*handlerOpts)

// CaseOption configures individual suspend cases (carries RetryPolicy).
type CaseOption = HandlerOption

type handlerOpts struct {
	retryPolicy *RetryPolicy
}

func applyOpts(opts []HandlerOption) handlerOpts {
	var o handlerOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithRetryPolicy sets the retry policy for a handler or case.
func WithRetryPolicy(p RetryPolicy) HandlerOption {
	return func(o *handlerOpts) {
		o.retryPolicy = &p
	}
}
