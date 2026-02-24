// Package stateroutine provides primitives for building durable, long-running
// stateroutines on top of Temporal. User handler functions run as activities
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
// describing what the stateroutine should wait for next.
// Return Done(result) to complete the stateroutine.
type HandlerFunc[State HandlerState, Result any] func(ctx *Context, state State) (*Suspend[Result], error)

// SendFunc handles a fire-and-forget message (Signal).
type SendFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M) (*Suspend[Result], error)

// CallFunc handles a synchronous request-response (Update).
type CallFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req) (Resp, *Suspend[Result], error)

// ─── Terminal error handler function signatures ───
//
// Terminal error handlers are invoked only after all retries configured in the
// RetryPolicy are exhausted. They receive the same inputs as the original
// handler plus the final error, and can compensate, transition to a different
// state, or fail the stateroutine.

// TerminalErrorFunc handles terminal errors for a HandlerFunc.
type TerminalErrorFunc[State HandlerState, Result any] func(ctx *Context, state State, err error) (*Suspend[Result], error)

// SendTerminalErrorFunc handles terminal errors for a SendFunc.
type SendTerminalErrorFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M, err error) (*Suspend[Result], error)

// CallTerminalErrorFunc handles terminal errors for a CallFunc.
type CallTerminalErrorFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req, err error) (Resp, *Suspend[Result], error)

// ─── Worker registry ───

// handlerEntry wraps a handler with its options configuration.
type handlerEntry struct {
	handler any
	options HandlerOptions
}

func addEntry(w *Worker, key string, handler any, opts HandlerOptions) {
	if _, exists := w.handlers[key]; exists {
		panic("stateroutine: handler already registered for key: " + key)
	}
	w.handlers[key] = handlerEntry{handler: handler, options: opts}
}

func registerTerminalError(w *Worker, primaryKey string, teHandler any, opts HandlerOptions) {
	errorKey := "error:" + primaryKey
	w.handlers[errorKey] = handlerEntry{handler: teHandler, options: opts}
	entry := w.handlers[primaryKey]
	entry.options.onTerminalErrorKey = errorKey
	w.handlers[primaryKey] = entry
}

// ─── Registration types with OnTerminalError builder methods ───

// handlerReg is returned by AddHandler to allow chaining .OnTerminalError().
type handlerReg[S HandlerState, T any] struct {
	w   *Worker
	key string
}

// OnTerminalError registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the stateroutine. The terminal error handler must have the same State and
// Result types as the main handler.
func (r handlerReg[S, T]) OnTerminalError(te TerminalErrorFunc[S, T], opts HandlerOptions) {
	registerTerminalError(r.w, r.key, te, opts)
}

// sendHandlerReg is returned by AddSendHandler to allow chaining .OnTerminalError().
type sendHandlerReg[S HandlerState, M Message, T any] struct {
	w   *Worker
	key string
}

// OnTerminalError registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the stateroutine. The terminal error handler must have the same State, Message,
// and Result types as the main handler.
func (r sendHandlerReg[S, M, T]) OnTerminalError(te SendTerminalErrorFunc[S, M, T], opts HandlerOptions) {
	registerTerminalError(r.w, r.key, te, opts)
}

// callHandlerReg is returned by AddCallHandler to allow chaining .OnTerminalError().
type callHandlerReg[S HandlerState, Req Message, Resp any, T any] struct {
	w   *Worker
	key string
}

// OnTerminalError registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the stateroutine. The terminal error handler must have the same State, Request,
// Response, and Result types as the main handler.
func (r callHandlerReg[S, Req, Resp, T]) OnTerminalError(te CallTerminalErrorFunc[S, Req, Resp, T], opts HandlerOptions) {
	registerTerminalError(r.w, r.key, te, opts)
}

// ─── Add* registration functions ───

// AddHandler registers a HandlerFunc keyed by state Kind.
// Any HandlerFunc can serve as a stateroutine entry point (via Start) or as a
// continuation target (via After, Continue, Default, OnTimer).
// HandlerOptions configures retry behavior for the handler.
// Chain .OnTerminalError() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the stateroutine.
func AddHandler[S HandlerState, T any](w *Worker, h HandlerFunc[S, T], opts HandlerOptions) handlerReg[S, T] {
	var zero S
	key := "handler:" + zero.Kind()
	addEntry(w, key, h, opts)
	return handlerReg[S, T]{w: w, key: key}
}

// AddSendHandler registers a SendFunc keyed by state Kind and message Kind.
// HandlerOptions configures retry behavior for the handler.
// Chain .OnTerminalError() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the stateroutine.
func AddSendHandler[S HandlerState, M Message, T any](w *Worker, h SendFunc[S, M, T], opts HandlerOptions) sendHandlerReg[S, M, T] {
	var zeroS S
	var zeroM M
	key := "send:" + zeroS.Kind() + ":" + zeroM.Kind()
	addEntry(w, key, h, opts)
	return sendHandlerReg[S, M, T]{w: w, key: key}
}

// AddCallHandler registers a CallFunc keyed by state Kind and request Kind.
// HandlerOptions configures retry behavior for the handler.
// Chain .OnTerminalError() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the stateroutine.
func AddCallHandler[S HandlerState, Req Message, Resp any, T any](w *Worker, h CallFunc[S, Req, Resp, T], opts HandlerOptions) callHandlerReg[S, Req, Resp, T] {
	var zeroS S
	var zeroReq Req
	key := "call:" + zeroS.Kind() + ":" + zeroReq.Kind()
	addEntry(w, key, h, opts)
	return callHandlerReg[S, Req, Resp, T]{w: w, key: key}
}
