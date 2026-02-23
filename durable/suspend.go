package durable

import "time"

// Suspend describes what a routine should wait for before invoking the next
// handler. It is an opaque value built via the After and Select constructors.
type Suspend struct {
	cases []Case
}

// Case is a single wait condition inside a Suspend (a timer, inbox receive,
// method call, or query) paired with the handler to invoke when that
// condition fires. Internally type-erased; type safety is enforced at
// construction via generic constructor functions.
type Case struct {
	// Exactly one of the following is set (or none if immediate is true).
	timerDuration *time.Duration
	inboxName     string
	methodName    string
	queryName     string

	// immediate marks this case as firing without waiting. Used by Continue
	// (sole case in a Suspend — skip the selector entirely) and Default
	// (inside a Select — maps to Temporal's sel.AddDefault).
	immediate bool

	// handler and state are stored as any because each case may carry
	// different state and handler types, erased at this level.
	handler any
	state   any

	// handlerKey is the composite key for looking up the registered handler
	// (e.g., "cast:booking.reserved"). Set by constructors for non-query cases.
	handlerKey string

	// retryPolicy overrides the handler-level retry policy for this case.
	retryPolicy *RetryPolicy
}

// After builds a Suspend that waits for the given duration and then calls
// handler with the provided state. This is the simple "sleep then continue" primitive.
func After[S HandlerState](d time.Duration, handler HandlerFunc[S], state S, opts ...CaseOption) *Suspend {
	return &Suspend{
		cases: []Case{AfterFunc(d, handler, state, opts...)},
	}
}

// Select builds a Suspend that waits for the first of several cases to fire,
// similar to Go's select statement.
func Select(cases ...Case) *Suspend {
	return &Suspend{cases: cases}
}

// AfterFunc returns a Case that fires after the given duration.
// Use this inside a Select when you want a timer alongside other cases.
func AfterFunc[S HandlerState](d time.Duration, handler HandlerFunc[S], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		timerDuration: &d,
		handler:       handler,
		state:         state,
		handlerKey:    "handler:" + state.Kind(),
		retryPolicy:   o.retryPolicy,
	}
}

// OnCast returns a Case that fires when a message arrives on the given inbox.
// The message is deserialized into type M before the handler is called.
// Maps to a Temporal Signal handler.
func OnCast[S HandlerState, M any](inbox Inbox[M], handler CastFunc[S, M], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		inboxName:   inbox.Name,
		handler:     handler,
		state:       state,
		handlerKey:  "cast:" + state.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// OnCall returns a Case that fires when a client calls the given method.
// The handler receives a request and returns a response. Blocks the caller
// until the handler completes. Maps to a Temporal Update handler.
func OnCall[S HandlerState, Req, Resp any](method Method[Req, Resp], handler CallFunc[S, Req, Resp], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		methodName:  method.Name,
		handler:     handler,
		state:       state,
		handlerKey:  "call:" + state.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// Continue builds a Suspend that checkpoints state (continue-as-new boundary)
// and immediately invokes the handler as the next activity, without waiting for
// a timer, inbox, or method. Use this for multi-step processing where you want
// explicit continue-as-new boundaries between steps.
func Continue[S HandlerState](handler HandlerFunc[S], state S, opts ...CaseOption) *Suspend {
	o := applyOpts(opts)
	return &Suspend{
		cases: []Case{{
			immediate:   true,
			handler:     handler,
			state:       state,
			handlerKey:  "handler:" + state.Kind(),
			retryPolicy: o.retryPolicy,
		}},
	}
}

// Default returns a Case that fires immediately if no other cases in the
// Select are ready. Maps to Temporal's sel.AddDefault(). Use this to drain
// buffered signals: if no signals are pending, the default case fires.
func Default[S HandlerState](handler HandlerFunc[S], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		immediate:   true,
		handler:     handler,
		state:       state,
		handlerKey:  "handler:" + state.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// OnQuery returns a Case that fires when a client queries the given query
// descriptor. The handler receives the state by value (read-only) and returns
// a response. Does not advance the state machine. Maps to a Temporal Query handler.
// OnQuery does not accept CaseOption because queries run synchronously in
// workflow context and are not retried as activities.
func OnQuery[S HandlerState, Req, Resp any](query Query[Req, Resp], handler QueryFunc[S, Req, Resp], state S) Case {
	return Case{
		queryName:  query.Name,
		handler:    handler,
		state:      state,
		handlerKey: "query:" + state.Kind(),
	}
}
