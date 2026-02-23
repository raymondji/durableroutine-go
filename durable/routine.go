// Package durable provides primitives for building durable, long-running
// routines on top of Temporal. User handler functions run as activities
// (no determinism constraints). When a handler needs to suspend — wait for
// a timer or an incoming message — it returns a declarative Suspend value
// that the runtime interprets as workflow code.
package durable

// HandlerState is the interface that all handler state types must implement.
// Kind returns a stable string identifier used for handler lookup and
// serialization across continue-as-new boundaries.
type HandlerState interface {
	Kind() string
}

// RoutineArgs is the interface that routine input types must implement.
// Kind returns a stable string that identifies the routine type, used as the
// Temporal workflow type. This decouples the routine's identity from Go type
// names, allowing safe renames across deploys.
type RoutineArgs interface {
	HandlerState
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
// Return Done() to complete the routine.
type HandlerFunc[State HandlerState] func(ctx *Context, state State) (*Suspend, error)

// CastFunc handles a fire-and-forget message (Signal).
type CastFunc[State HandlerState, M Message] func(ctx *Context, state State, msg M) (*Suspend, error)

// CallFunc handles a synchronous request-response (Update).
type CallFunc[State HandlerState, Req Message, Resp any] func(ctx *Context, state State, req Req) (Resp, *Suspend, error)

// QueryFunc handles a synchronous read-only query (Query).
type QueryFunc[State HandlerState, Req Message, Resp any] func(ctx *Context, state State, req Req) (Resp, error)

// ─── Worker registry ───

// handlerEntry wraps a handler with its retry policy configuration.
type handlerEntry struct {
	handler     any
	retryPolicy *RetryPolicy
}

func addEntry(w *Worker, key string, handler any, opts []HandlerOption) {
	if _, exists := w.handlers[key]; exists {
		panic("durable: handler already registered for key: " + key)
	}
	o := applyOpts(opts)
	w.handlers[key] = handlerEntry{handler: handler, retryPolicy: o.retryPolicy}
}

// AddRoutineHandler registers a handler function for a routine type identified
// by Args.Kind(). Panics if a handler is already registered for the same kind.
func AddRoutineHandler[Args RoutineArgs](w *Worker, handler HandlerFunc[Args], opts ...HandlerOption) {
	var zero Args
	addEntry(w, "routine:"+zero.Kind(), handler, opts)
}

// AddHandler registers a HandlerFunc for a state Kind.
func AddHandler[S HandlerState](w *Worker, h HandlerFunc[S], opts ...HandlerOption) {
	var zero S
	addEntry(w, "handler:"+zero.Kind(), h, opts)
}

// AddCastHandler registers a CastFunc keyed by state Kind and message Kind.
func AddCastHandler[S HandlerState, M Message](w *Worker, h CastFunc[S, M], opts ...HandlerOption) {
	var zeroS S
	var zeroM M
	addEntry(w, "cast:"+zeroS.Kind()+":"+zeroM.Kind(), h, opts)
}

// AddCallHandler registers a CallFunc keyed by state Kind and request Kind.
func AddCallHandler[S HandlerState, Req Message, Resp any](w *Worker, h CallFunc[S, Req, Resp], opts ...HandlerOption) {
	var zeroS S
	var zeroReq Req
	addEntry(w, "call:"+zeroS.Kind()+":"+zeroReq.Kind(), h, opts)
}

// AddQueryHandler registers a QueryFunc keyed by state Kind and request Kind.
func AddQueryHandler[S HandlerState, Req Message, Resp any](w *Worker, h QueryFunc[S, Req, Resp], opts ...HandlerOption) {
	var zeroS S
	var zeroReq Req
	addEntry(w, "query:"+zeroS.Kind()+":"+zeroReq.Kind(), h, opts)
}
