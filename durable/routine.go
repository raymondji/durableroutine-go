// Package durable provides primitives for building durable, long-running
// routines on top of Temporal. User handler functions run as activities
// (no determinism constraints). When a handler needs to suspend — wait for
// a timer or an incoming message — it returns a declarative Suspend value
// that the runtime interprets as workflow code.
package durable

// RoutineArgs is the interface that routine input types must implement.
// Kind returns a stable string that identifies the routine type, used as the
// Temporal workflow type. This decouples the routine's identity from Go type
// names, allowing safe renames across deploys.
type RoutineArgs interface {
	Kind() string
}

// ─── Descriptors (compile-time type safety) ───

// Inbox is a typed descriptor for Cast (fire-and-forget) messages.
// Maps to a Temporal Signal.
type Inbox[M any] struct{ Name string }

// Method is a typed descriptor for Call (synchronous request-response).
// Maps to a Temporal Update.
type Method[Req, Resp any] struct{ Name string }

// Query is a typed descriptor for Query (synchronous read-only).
// Maps to a Temporal Query.
type Query[Req, Resp any] struct{ Name string }

// ─── Handler function signatures ───

// HandlerFunc is a function that receives state and returns a Suspend
// describing what the routine should wait for next.
// Returning a nil *Suspend completes the routine.
type HandlerFunc[State any] func(ctx *Context, state State) (*Suspend, error)

// CastFunc handles a fire-and-forget message (Signal).
type CastFunc[State, M any] func(ctx *Context, state State, msg M) (*Suspend, error)

// CallFunc handles a synchronous request-response (Update).
type CallFunc[State, Req, Resp any] func(ctx *Context, state State, req Req) (Resp, *Suspend, error)

// QueryFunc handles a synchronous read-only query (Query).
type QueryFunc[State, Req, Resp any] func(ctx *Context, state State, req Req) (Resp, error)

// ─── Worker registry ───

// Workers is a type-safe registry of routine handlers, following the River
// pattern. Create with NewWorkers, register handlers with AddRoutine.
type Workers struct {
	handlers map[string]any
}

// NewWorkers creates a new routine handler registry.
func NewWorkers() *Workers {
	return &Workers{handlers: make(map[string]any)}
}

// AddRoutine registers a handler function for a routine type identified by
// Args.Kind(). Panics if a handler is already registered for the same kind.
func AddRoutine[Args RoutineArgs](workers *Workers, handler HandlerFunc[Args]) {
	var zero Args
	kind := zero.Kind()
	if _, exists := workers.handlers[kind]; exists {
		panic("durable: routine already registered for kind: " + kind)
	}
	workers.handlers[kind] = handler
}
