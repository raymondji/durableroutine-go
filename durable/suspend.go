package durable

import "time"

// Suspend describes what a routine should wait for before invoking the next
// handler. It is an opaque value built via the After and Select constructors.
type Suspend struct {
	done  bool
	cases []Case
}

// Done returns a Suspend that completes the routine. Use this instead of
// returning nil to make the intent explicit.
func Done() *Suspend {
	return &Suspend{done: true}
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
	// (e.g., "cast:booking.reserved:payment"). Set by constructors for
	// non-query cases.
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

// OnCast returns a Case that fires when a message arrives on the inbox
// identified by M.Kind(). The message is deserialized into type M before the
// handler is called. Maps to a Temporal Signal handler.
func OnCast[S HandlerState, M Message](handler CastFunc[S, M], state S, opts ...CaseOption) Case {
	var zeroM M
	o := applyOpts(opts)
	return Case{
		inboxName:   zeroM.Kind(),
		handler:     handler,
		state:       state,
		handlerKey:  "cast:" + state.Kind() + ":" + zeroM.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// OnCall returns a Case that fires when a client calls the method identified
// by Req.Kind(). The handler receives a request and returns a response.
// Blocks the caller until the handler completes. Maps to a Temporal Update handler.
func OnCall[S HandlerState, Req Message, Resp any](handler CallFunc[S, Req, Resp], state S, opts ...CaseOption) Case {
	var zeroReq Req
	o := applyOpts(opts)
	return Case{
		methodName:  zeroReq.Kind(),
		handler:     handler,
		state:       state,
		handlerKey:  "call:" + state.Kind() + ":" + zeroReq.Kind(),
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

// OnQuery returns a Case that fires when a client queries using a request
// identified by Req.Kind(). The handler receives the state by value (read-only)
// and returns a response. Does not advance the state machine. Maps to a
// Temporal Query handler.
// OnQuery does not accept CaseOption because queries run synchronously in
// workflow context and are not retried as activities.
func OnQuery[S HandlerState, Req Message, Resp any](handler QueryFunc[S, Req, Resp], state S) Case {
	var zeroReq Req
	return Case{
		queryName:  zeroReq.Kind(),
		handler:    handler,
		state:      state,
		handlerKey: "query:" + state.Kind() + ":" + zeroReq.Kind(),
	}
}
