// Package stateroutine provides primitives for building durable, long-running
// routines on top of Temporal. User handler functions run as activities
// (no determinism constraints). When a handler needs to suspend — wait for
// a timer or an incoming message — it returns a declarative Suspend value
// that the runtime interprets as workflow code.
package stateroutine

// HandlerState is the interface that all handler state types must implement.
// Kind returns a stable string identifier used for handler lookup and
// serialization across continue-as-new boundaries.
type HandlerState interface {
	Kind() string
}

// Message is the interface that all message and request types must implement.
// Kind returns a stable string used as the Temporal signal/update/query name
// and as part of the handler registration key. This decouples routing from
// Go type names.
type Message interface {
	Kind() string
}

// ─── Handler function signatures ───

// HandlerFunc is a function that receives state and returns a Suspend
// describing what the routine should wait for next.
// Return Done(result) to complete the routine.
type HandlerFunc[State HandlerState, Result any] func(ctx *Context, state State) (*Suspend[Result], error)

// SendFunc handles a fire-and-forget message (Signal).
type SendFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M) (*Suspend[Result], error)

// CallFunc handles a synchronous request-response (Update).
type CallFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req) (Resp, *Suspend[Result], error)

// ─── Terminal error handler function signatures ───
//
// Terminal error handlers are invoked only after all retries configured in the
// ErrorPolicy are exhausted. They receive the same inputs as the original
// handler plus the final error, and can compensate, transition to a different
// state, or fail the routine.

// TerminalErrorFunc handles terminal errors for a HandlerFunc.
type TerminalErrorFunc[State HandlerState, Result any] func(ctx *Context, state State, err error) (*Suspend[Result], error)

// SendTerminalErrorFunc handles terminal errors for a SendFunc.
type SendTerminalErrorFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M, err error) (*Suspend[Result], error)

// CallTerminalErrorFunc handles terminal errors for a CallFunc.
type CallTerminalErrorFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req, err error) (Resp, *Suspend[Result], error)

// ─── Worker registry ───

// handlerEntry wraps a handler with its error policy configuration.
type handlerEntry struct {
	handler     any
	errorPolicy ErrorPolicy
}

func addEntry(w *Worker, key string, handler any, policy ErrorPolicy, onTerminalError any) {
	if _, exists := w.handlers[key]; exists {
		panic("stateroutine: handler already registered for key: " + key)
	}

	w.handlers[key] = handlerEntry{handler: handler, errorPolicy: policy}

	// If a terminal error handler was provided, register it under the error:
	// prefix and set the onTerminalErrorKey in the error policy.
	if onTerminalError != nil {
		errorKey := "error:" + key
		w.handlers[errorKey] = handlerEntry{handler: onTerminalError}
		entry := w.handlers[key]
		entry.errorPolicy.onTerminalErrorKey = errorKey
		w.handlers[key] = entry
	}
}

// AddHandler registers a HandlerFunc keyed by state Kind.
// Any HandlerFunc can serve as a routine entry point (via Start) or as a
// continuation target (via After, Continue, Default, OnTimer).
// ErrorPolicy is required and configures retry behavior for the handler.
// An optional TerminalErrorFunc can be provided — it is invoked only after all
// retries are exhausted, instead of failing the routine. The terminal error
// handler must have the same State and Result types as the main handler.
func AddHandler[S HandlerState, T any](w *Worker, h HandlerFunc[S, T], p ErrorPolicy, onTerminalError ...TerminalErrorFunc[S, T]) {
	if len(onTerminalError) > 1 {
		panic("stateroutine: at most one terminal error handler allowed")
	}
	var te any
	if len(onTerminalError) == 1 {
		te = onTerminalError[0]
	}
	var zero S
	addEntry(w, "handler:"+zero.Kind(), h, p, te)
}

// AddSendHandler registers a SendFunc keyed by state Kind and message Kind.
// ErrorPolicy is required and configures retry behavior for the handler.
// An optional SendTerminalErrorFunc can be provided — it is invoked only after
// all retries are exhausted. The terminal error handler must have the same
// State, Message, and Result types as the main handler.
func AddSendHandler[S HandlerState, M Message, T any](w *Worker, h SendFunc[S, M, T], p ErrorPolicy, onTerminalError ...SendTerminalErrorFunc[S, M, T]) {
	if len(onTerminalError) > 1 {
		panic("stateroutine: at most one terminal error handler allowed")
	}
	var te any
	if len(onTerminalError) == 1 {
		te = onTerminalError[0]
	}
	var zeroS S
	var zeroM M
	addEntry(w, "send:"+zeroS.Kind()+":"+zeroM.Kind(), h, p, te)
}

// AddCallHandler registers a CallFunc keyed by state Kind and request Kind.
// ErrorPolicy is required and configures retry behavior for the handler.
// An optional CallTerminalErrorFunc can be provided — it is invoked only after
// all retries are exhausted. The terminal error handler must have the same
// State, Request, Response, and Result types as the main handler.
func AddCallHandler[S HandlerState, Req Message, Resp any, T any](w *Worker, h CallFunc[S, Req, Resp, T], p ErrorPolicy, onTerminalError ...CallTerminalErrorFunc[S, Req, Resp, T]) {
	if len(onTerminalError) > 1 {
		panic("stateroutine: at most one terminal error handler allowed")
	}
	var te any
	if len(onTerminalError) == 1 {
		te = onTerminalError[0]
	}
	var zeroS S
	var zeroReq Req
	addEntry(w, "call:"+zeroS.Kind()+":"+zeroReq.Kind(), h, p, te)
}
