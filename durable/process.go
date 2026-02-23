// Package durable provides primitives for building durable, long-running
// processes on top of Temporal. User handler functions run as activities
// (no determinism constraints). When a handler needs to suspend — wait for
// a timer or an incoming message — it returns a declarative Suspend value
// that the runtime interprets as workflow code.
package durable

import "context"

// Process defines a top-level durable process parameterised by its state type S.
// S must be serialisable (JSON by default).
type Process[S any] struct {
	// Name identifies this process type (used as the Temporal workflow type).
	Name string

	// InitState returns the initial process state for a new execution.
	// args is the value passed to Client.Start — the function should type-assert
	// it to the expected type.
	InitState func(args any) S

	// Initial is the handler invoked when the process first starts.
	Initial HandlerFunc[S]
}

// HandlerFunc is a function that receives the current state, may mutate it,
// and returns a Suspend describing what the process should wait for next.
// Returning a nil *Suspend completes the process.
type HandlerFunc[S any] func(ctx context.Context, state *S) (*Suspend[S], error)

// MessageHandlerFunc is like HandlerFunc but also receives a typed message.
type MessageHandlerFunc[S any, M any] func(ctx context.Context, state *S, msg M) (*Suspend[S], error)
