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
	// Exactly one of the following is set.
	timerDuration *time.Duration
	inboxName     string
	methodName    string
	queryName     string

	// handler and state are stored as any because each case may carry
	// different state and handler types, erased at this level.
	handler any
	state   any
}

// After builds a Suspend that waits for the given duration and then calls
// handler with the provided state. This is the simple "sleep then continue" primitive.
func After[S any](d time.Duration, handler HandlerFunc[S], state S) *Suspend {
	return &Suspend{
		cases: []Case{AfterFunc(d, handler, state)},
	}
}

// Select builds a Suspend that waits for the first of several cases to fire,
// similar to Go's select statement.
func Select(cases ...Case) *Suspend {
	return &Suspend{cases: cases}
}

// AfterFunc returns a Case that fires after the given duration.
// Use this inside a Select when you want a timer alongside other cases.
func AfterFunc[S any](d time.Duration, handler HandlerFunc[S], state S) Case {
	return Case{
		timerDuration: &d,
		handler:       handler,
		state:         state,
	}
}

// OnCast returns a Case that fires when a message arrives on the given inbox.
// The message is deserialized into type M before the handler is called.
// Maps to a Temporal Signal handler.
func OnCast[S, M any](inbox Inbox[M], handler CastFunc[S, M], state S) Case {
	return Case{
		inboxName: inbox.Name,
		handler:   handler,
		state:     state,
	}
}

// OnCall returns a Case that fires when a client calls the given method.
// The handler receives a request and returns a response. Blocks the caller
// until the handler completes. Maps to a Temporal Update handler.
func OnCall[S, Req, Resp any](method Method[Req, Resp], handler CallFunc[S, Req, Resp], state S) Case {
	return Case{
		methodName: method.Name,
		handler:    handler,
		state:      state,
	}
}

// OnQuery returns a Case that fires when a client queries the given query
// descriptor. The handler receives the state by value (read-only) and returns
// a response. Does not advance the state machine. Maps to a Temporal Query handler.
func OnQuery[S, Req, Resp any](query Query[Req, Resp], handler QueryFunc[S, Req, Resp], state S) Case {
	return Case{
		queryName: query.Name,
		handler:   handler,
		state:     state,
	}
}
