package durable

import "time"

// Suspend describes what a routine should wait for before invoking the next
// handler. T is the routine's result type, returned via Done and retrieved
// via ClientGet. It is an opaque value built via Done, After, Select, etc.
type Suspend[T any] struct {
	done   bool
	result T
	cases  []Case
}

// Unit is a convenience type for routines that don't produce a result.
type Unit struct{}

// Done returns a Suspend that completes the routine with the given result.
// Clients can retrieve the result via ClientGet.
func Done[T any](result T) *Suspend[T] {
	return &Suspend[T]{done: true, result: result}
}

// Case is a single wait condition inside a Suspend (a timer, inbox receive,
// or method call) paired with the handler to invoke when that condition fires.
// Internally type-erased; type safety is enforced at construction via generic
// constructor functions.
type Case struct {
	// Exactly one of the following is set (or none if immediate is true).
	timerDuration *time.Duration
	castName      string
	callName      string

	// immediate marks this case as firing without waiting. Used by Continue
	// (sole case in a Suspend — skip the selector entirely) and Default
	// (inside a Select — maps to Temporal's sel.AddDefault).
	immediate bool

	// handler and state are stored as any because each case may carry
	// different state and handler types, erased at this level.
	state any

	// handlerKey is the composite key for looking up the registered handler
	// (e.g., "cast:booking.reserved:payment"). Set by constructors for
	// non-query cases.
	handlerKey string

	// retryPolicy overrides the handler-level retry policy for this case.
	retryPolicy *RetryPolicy
}

// After builds a Suspend that waits for the given duration and then calls
// handler with the provided state. This is the simple "sleep then continue" primitive.
func After[S HandlerState, T any](d time.Duration, handler HandlerFunc[S, T], state S, opts ...CaseOption) *Suspend[T] {
	return &Suspend[T]{
		cases: []Case{OnTimer(d, handler, state, opts...)},
	}
}

// Select builds a Suspend that waits for the first of several cases to fire,
// similar to Go's select statement.
//
// This prioritizes cases in the following way:
// 1. Calls (caller is blocking, may modify state, should be fast)
// 2. Casts and Timers
// 3. Default (if present, runs if no other cases are ready)
func Select[T any](cases ...Case) *Suspend[T] {
	return &Suspend[T]{cases: cases}
}

// OnTimer returns a Case that fires after the given duration.
// Use this inside a Select when you want a timer alongside other cases.
func OnTimer[S HandlerState, T any](d time.Duration, handler HandlerFunc[S, T], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		timerDuration: &d,
		state:         state,
		handlerKey:    "handler:" + state.Kind(),
		retryPolicy:   o.retryPolicy,
	}
}

// OnCast returns a Case that fires when a message arrives on the inbox
// identified by M.Kind(). The message is deserialized into type M before the
// handler is called. Maps to a Temporal Signal handler.
func OnCast[S HandlerState, M Message, T any](handler CastFunc[S, M, T], state S, opts ...CaseOption) Case {
	var zeroM M
	o := applyOpts(opts)
	return Case{
		castName:    zeroM.Kind(),
		state:       state,
		handlerKey:  "cast:" + state.Kind() + ":" + zeroM.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// OnCall returns a Case that fires when a client calls the method identified
// by Req.Kind(). The handler receives a request and returns a response.
// Blocks the caller until the handler completes. Maps to a Temporal Update handler.
func OnCall[S HandlerState, Req Message, Resp any, T any](handler CallFunc[S, Req, Resp, T], state S, opts ...CaseOption) Case {
	var zeroReq Req
	o := applyOpts(opts)
	return Case{
		callName:    zeroReq.Kind(),
		state:       state,
		handlerKey:  "call:" + state.Kind() + ":" + zeroReq.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

// Continue builds a Suspend that checkpoints state (continue-as-new boundary)
// and immediately invokes the handler as the next activity, without waiting for
// a timer, inbox, or method. Use this for multi-step processing where you want
// explicit continue-as-new boundaries between steps.
func Continue[S HandlerState, T any](handler HandlerFunc[S, T], state S, opts ...CaseOption) *Suspend[T] {
	o := applyOpts(opts)
	return &Suspend[T]{
		cases: []Case{{
			immediate:   true,
			state:       state,
			handlerKey:  "handler:" + state.Kind(),
			retryPolicy: o.retryPolicy,
		}},
	}
}

// Default returns a Case that fires immediately if no other cases in the
// Select are ready. Maps to Temporal's sel.AddDefault(). Use this to drain
// buffered signals: if no signals are pending, the default case fires.
func Default[S HandlerState, T any](handler HandlerFunc[S, T], state S, opts ...CaseOption) Case {
	o := applyOpts(opts)
	return Case{
		immediate:   true,
		state:       state,
		handlerKey:  "handler:" + state.Kind(),
		retryPolicy: o.retryPolicy,
	}
}

