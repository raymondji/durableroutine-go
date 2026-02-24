// Package durable provides primitives for building durable, long-running
// routines on top of Temporal. User handler functions run as activities
// (no determinism constraints). When a handler needs to suspend — wait for
// a timer or an incoming message — it returns a declarative Continuation value
// that the runtime interprets as workflow code.
package durable

import (
	"encoding/json"
	"fmt"

	"github.com/raymondji/durableroutine-go/internal/durablecore"
)

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

// --- Handler function signatures ---

// HandlerFunc is a function that receives state and returns a Continuation
// describing what the routine should wait for next.
// Return Done(result) to complete the routine.
type HandlerFunc[State HandlerState, Result any] func(ctx *Context, state State) (*Continuation[Result], error)

// SendFunc handles a fire-and-forget message (Signal).
type SendFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M) (*Continuation[Result], error)

// CallFunc handles a synchronous request-response (Update).
type CallFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req) (Resp, *Continuation[Result], error)

// --- Terminal error handler function signatures ---
//
// Terminal error handlers are invoked only after all retries configured in the
// RetryPolicy are exhausted. They receive the same inputs as the original
// handler plus the final error, and can compensate, transition to a different
// state, or fail the routine.

// TerminalErrorFunc handles terminal errors for a HandlerFunc.
type TerminalErrorFunc[State HandlerState, Result any] func(ctx *Context, state State, err error) (*Continuation[Result], error)

// SendTerminalErrorFunc handles terminal errors for a SendFunc.
type SendTerminalErrorFunc[State HandlerState, M Message, Result any] func(ctx *Context, state State, msg M, err error) (*Continuation[Result], error)

// CallTerminalErrorFunc handles terminal errors for a CallFunc.
type CallTerminalErrorFunc[State HandlerState, Req Message, Resp any, Result any] func(ctx *Context, state State, req Req, err error) (Resp, *Continuation[Result], error)

// --- Runner types ---

// HandlerRunner is a closure that deserializes inputs, invokes the handler,
// and extracts the Continuation — all without reflection. Created at registration
// time when full generic type information is available.
type HandlerRunner func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error)

// RunOutput is the type-erased result of a HandlerRunner invocation.
type RunOutput struct {
	Done         bool
	Result       json.RawMessage
	Cases        []Case
	CallResponse json.RawMessage
}

// buildRunOutput extracts the fields from a generic *Continuation[T] into a RunOutput.
func buildRunOutput[T any](cont *Continuation[T], key string) (*RunOutput, error) {
	if cont == nil {
		return nil, fmt.Errorf("handler %s returned nil Continuation with nil error — must always return a Continuation when there is no error", key)
	}
	out := &RunOutput{}
	if cont.IsDone() {
		out.Done = true
		resultBytes, err := json.Marshal(cont.Result())
		if err != nil {
			return nil, fmt.Errorf("marshal result: %w", err)
		}
		out.Result = resultBytes
	} else {
		out.Cases = cont.Cases()
	}
	return out, nil
}

// --- Worker registry ---

// handlerEntry wraps a handler with its options configuration.
type handlerEntry struct {
	handler any
	options HandlerOptions
	runner  HandlerRunner
}

func addEntry(w *Worker, key string, handler any, runner HandlerRunner, opts HandlerOptions) {
	if _, exists := w.handlers[key]; exists {
		panic("durable: handler already registered for key: " + key)
	}
	w.handlers[key] = handlerEntry{handler: handler, runner: runner, options: opts}
}

func registerTerminalError(w *Worker, primaryKey string, teHandler any, runner HandlerRunner, opts HandlerOptions) {
	errorKey := durablecore.ErrorKey(primaryKey)
	w.handlers[errorKey] = handlerEntry{handler: teHandler, runner: runner, options: opts}
	entry := w.handlers[primaryKey]
	entry.options.terminalErrorHandlerKey = errorKey
	w.handlers[primaryKey] = entry
}

// --- Registration types with WithTerminalErrorHandler builder methods ---

// handlerReg is returned by AddHandler to allow chaining .WithTerminalErrorHandler().
type handlerReg[S HandlerState, T any] struct {
	w   *Worker
	key string
}

// WithTerminalErrorHandler registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The terminal error handler must have the same State and
// Result types as the main handler.
func (r handlerReg[S, T]) WithTerminalErrorHandler(te TerminalErrorFunc[S, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		cont, herr := te(ctx, state, fmt.Errorf("%s", errStr))
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, durablecore.ErrorKey(r.key))
	}
	registerTerminalError(r.w, r.key, te, runner, opts)
}

// sendHandlerReg is returned by AddSendHandler to allow chaining .WithTerminalErrorHandler().
type sendHandlerReg[S HandlerState, M Message, T any] struct {
	w   *Worker
	key string
}

// WithTerminalErrorHandler registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The terminal error handler must have the same State, Message,
// and Result types as the main handler.
func (r sendHandlerReg[S, M, T]) WithTerminalErrorHandler(te SendTerminalErrorFunc[S, M, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		var msg M
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			return nil, fmt.Errorf("deserialize msg: %w", err)
		}
		cont, herr := te(ctx, state, msg, fmt.Errorf("%s", errStr))
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, durablecore.ErrorKey(r.key))
	}
	registerTerminalError(r.w, r.key, te, runner, opts)
}

// callHandlerReg is returned by AddCallHandler to allow chaining .WithTerminalErrorHandler().
type callHandlerReg[S HandlerState, Req Message, Resp any, T any] struct {
	w   *Worker
	key string
}

// WithTerminalErrorHandler registers a terminal error handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The terminal error handler must have the same State, Request,
// Response, and Result types as the main handler.
func (r callHandlerReg[S, Req, Resp, T]) WithTerminalErrorHandler(te CallTerminalErrorFunc[S, Req, Resp, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		var req Req
		if err := json.Unmarshal(rawMsg, &req); err != nil {
			return nil, fmt.Errorf("deserialize req: %w", err)
		}
		resp, cont, herr := te(ctx, state, req, fmt.Errorf("%s", errStr))
		if herr != nil {
			return nil, herr
		}
		out, err := buildRunOutput(cont, "error:"+r.key)
		if err != nil {
			return nil, err
		}
		respBytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("marshal call response: %w", err)
		}
		out.CallResponse = respBytes
		return out, nil
	}
	registerTerminalError(r.w, r.key, te, runner, opts)
}

// --- Add* registration functions ---

// RegisterHandler registers a HandlerFunc keyed by state Kind.
// Any HandlerFunc can serve as a routine entry point (via Go) or as a
// continuation target (via After, Continue, Default).
// HandlerOptions configures retry behavior for the handler.
// Chain .WithTerminalErrorHandler() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterHandler[S HandlerState, T any](w *Worker, h HandlerFunc[S, T], opts HandlerOptions) handlerReg[S, T] {
	var zero S
	key := durablecore.HandlerKey(zero.Kind())
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		cont, herr := h(ctx, state)
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, key)
	}
	addEntry(w, key, h, runner, opts)
	return handlerReg[S, T]{w: w, key: key}
}

// RegisterSendHandler registers a SendFunc keyed by state Kind and message Kind.
// HandlerOptions configures retry behavior for the handler.
// Chain .WithTerminalErrorHandler() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterSendHandler[S HandlerState, M Message, T any](w *Worker, h SendFunc[S, M, T], opts HandlerOptions) sendHandlerReg[S, M, T] {
	var zeroS S
	var zeroM M
	key := durablecore.SendKey(zeroS.Kind(), zeroM.Kind())
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		var msg M
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			return nil, fmt.Errorf("deserialize msg: %w", err)
		}
		cont, herr := h(ctx, state, msg)
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, key)
	}
	addEntry(w, key, h, runner, opts)
	return sendHandlerReg[S, M, T]{w: w, key: key}
}

// RegisterCallHandler registers a CallFunc keyed by state Kind and request Kind.
// HandlerOptions configures retry behavior for the handler.
// Chain .WithTerminalErrorHandler() on the returned registration to register a terminal
// error handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterCallHandler[S HandlerState, Req Message, Resp any, T any](w *Worker, h CallFunc[S, Req, Resp, T], opts HandlerOptions) callHandlerReg[S, Req, Resp, T] {
	var zeroS S
	var zeroReq Req
	key := durablecore.CallKey(zeroS.Kind(), zeroReq.Kind())
	runner := func(ctx *Context, rawState json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var state S
		if err := json.Unmarshal(rawState, &state); err != nil {
			return nil, fmt.Errorf("deserialize state: %w", err)
		}
		var req Req
		if err := json.Unmarshal(rawMsg, &req); err != nil {
			return nil, fmt.Errorf("deserialize req: %w", err)
		}
		resp, cont, herr := h(ctx, state, req)
		if herr != nil {
			return nil, herr
		}
		out, err := buildRunOutput(cont, key)
		if err != nil {
			return nil, err
		}
		respBytes, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("marshal call response: %w", err)
		}
		out.CallResponse = respBytes
		return out, nil
	}
	addEntry(w, key, h, runner, opts)
	return callHandlerReg[S, Req, Resp, T]{w: w, key: key}
}
