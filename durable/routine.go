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

// Payload is the interface that all handler input, message, result, and query
// response types must implement. DurableKind returns a stable string identifier
// used for handler lookup and serialization across continue-as-new boundaries.
type Payload interface {
	DurableKind() string
}

// --- Handler function signatures ---

// Handler is a function that receives input and returns a Continuation
// describing what the routine should wait for next.
// Return Done(result) to complete the routine.
type Handler[Input Payload, Result Payload] func(ctx *Context, input Input) (*Continuation[Result], error)

// SendHandler handles a fire-and-forget message (Signal).
type SendHandler[Input Payload, ExternalInput Payload, Result Payload] func(ctx *Context, input Input, externalInput ExternalInput) (*Continuation[Result], error)

// CallHandler handles a synchronous request-response (Update).
type CallHandler[Input Payload, ExternalReq Payload, ExternalResp Payload, Result Payload] func(ctx *Context, input Input, externalReq ExternalReq) (ExternalResp, *Continuation[Result], error)

// --- Recovery handler function signatures ---
//
// Recovery handlers are invoked only after all retries configured in the
// RetryPolicy are exhausted. They receive the same inputs as the original
// handler plus the final error, and can compensate, transition to a different
// state, or fail the routine.

// RecoveryHandler handles terminal errors for a Handler.
type RecoveryHandler[Input Payload, Result Payload] func(ctx *Context, input Input, err error) (*Continuation[Result], error)

// SendRecoveryHandler handles terminal errors for a SendHandler.
type SendRecoveryHandler[Input Payload, ExternalInput Payload, Result Payload] func(ctx *Context, input Input, externalInput ExternalInput, err error) (*Continuation[Result], error)

// CallRecoveryHandler handles terminal errors for a CallHandler.
type CallRecoveryHandler[Input Payload, ExternalReq Payload, ExternalResp Payload, Result Payload] func(ctx *Context, input Input, externalReq ExternalReq, err error) (ExternalResp, *Continuation[Result], error)

// --- Runner types ---

// HandlerRunner is a closure that deserializes inputs, invokes the handler,
// and extracts the Continuation — all without reflection. Created at registration
// time when full generic type information is available.
type HandlerRunner func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error)

// RunOutput is the type-erased result of a HandlerRunner invocation.
type RunOutput struct {
	Done         bool
	Result       json.RawMessage
	Cases        []Case
	CallResponse json.RawMessage
}

// buildRunOutput extracts the fields from a generic *Continuation[T] into a RunOutput.
func buildRunOutput[T Payload](cont *Continuation[T], key string) (*RunOutput, error) {
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

func registerRecoveryHandler(w *Worker, primaryKey string, teHandler any, runner HandlerRunner, opts HandlerOptions) {
	errorKey := durablecore.ErrorKey(primaryKey)
	w.handlers[errorKey] = handlerEntry{handler: teHandler, runner: runner, options: opts}
	entry := w.handlers[primaryKey]
	entry.options.recoveryHandlerKey = errorKey
	w.handlers[primaryKey] = entry
}

// --- Registration types with WithRecoveryHandler builder methods ---

// handlerReg is returned by RegisterHandler to allow chaining .WithRecoveryHandler().
type handlerReg[I Payload, T Payload] struct {
	w   *Worker
	key string
}

// WithRecoveryHandler registers a recovery handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The recovery handler must have the same Input and
// Result types as the main handler.
func (r handlerReg[I, T]) WithRecoveryHandler(te RecoveryHandler[I, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		cont, herr := te(ctx, input, fmt.Errorf("%s", errStr))
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, durablecore.ErrorKey(r.key))
	}
	registerRecoveryHandler(r.w, r.key, te, runner, opts)
}

// sendHandlerReg is returned by RegisterSendHandler to allow chaining .WithRecoveryHandler().
type sendHandlerReg[I Payload, E Payload, T Payload] struct {
	w   *Worker
	key string
}

// WithRecoveryHandler registers a recovery handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The recovery handler must have the same Input, ExternalInput,
// and Result types as the main handler.
func (r sendHandlerReg[I, E, T]) WithRecoveryHandler(te SendRecoveryHandler[I, E, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		var externalInput E
		if err := json.Unmarshal(rawMsg, &externalInput); err != nil {
			return nil, fmt.Errorf("deserialize externalInput: %w", err)
		}
		cont, herr := te(ctx, input, externalInput, fmt.Errorf("%s", errStr))
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, durablecore.ErrorKey(r.key))
	}
	registerRecoveryHandler(r.w, r.key, te, runner, opts)
}

// callHandlerReg is returned by RegisterCallHandler to allow chaining .WithRecoveryHandler().
type callHandlerReg[I Payload, EReq Payload, EResp Payload, T Payload] struct {
	w   *Worker
	key string
}

// WithRecoveryHandler registers a recovery handler that is invoked only
// after all retries in the RetryPolicy are exhausted, instead of failing
// the routine. The recovery handler must have the same Input, ExternalReq,
// ExternalResp, and Result types as the main handler.
func (r callHandlerReg[I, EReq, EResp, T]) WithRecoveryHandler(te CallRecoveryHandler[I, EReq, EResp, T], opts HandlerOptions) {
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		var externalReq EReq
		if err := json.Unmarshal(rawMsg, &externalReq); err != nil {
			return nil, fmt.Errorf("deserialize externalReq: %w", err)
		}
		resp, cont, herr := te(ctx, input, externalReq, fmt.Errorf("%s", errStr))
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
	registerRecoveryHandler(r.w, r.key, te, runner, opts)
}

// --- Register* registration functions ---

// RegisterHandler registers a Handler keyed by input DurableKind and result DurableKind.
// Any Handler can serve as a routine entry point (via Go) or as a
// continuation target (via After, Continue, Default).
// HandlerOptions configures retry behavior for the handler.
// Chain .WithRecoveryHandler() on the returned registration to register a recovery
// handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterHandler[I Payload, T Payload](w *Worker, h Handler[I, T], opts HandlerOptions) handlerReg[I, T] {
	var zeroI I
	var zeroT T
	key := durablecore.HandlerKey(zeroI.DurableKind(), zeroT.DurableKind())
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		cont, herr := h(ctx, input)
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, key)
	}
	addEntry(w, key, h, runner, opts)
	return handlerReg[I, T]{w: w, key: key}
}

// RegisterSendHandler registers a SendHandler keyed by input DurableKind, message DurableKind, and result DurableKind.
// HandlerOptions configures retry behavior for the handler.
// Chain .WithRecoveryHandler() on the returned registration to register a recovery
// handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterSendHandler[I Payload, E Payload, T Payload](w *Worker, h SendHandler[I, E, T], opts HandlerOptions) sendHandlerReg[I, E, T] {
	var zeroI I
	var zeroE E
	var zeroT T
	key := durablecore.SendKey(zeroI.DurableKind(), zeroE.DurableKind(), zeroT.DurableKind())
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		var externalInput E
		if err := json.Unmarshal(rawMsg, &externalInput); err != nil {
			return nil, fmt.Errorf("deserialize externalInput: %w", err)
		}
		cont, herr := h(ctx, input, externalInput)
		if herr != nil {
			return nil, herr
		}
		return buildRunOutput(cont, key)
	}
	addEntry(w, key, h, runner, opts)
	return sendHandlerReg[I, E, T]{w: w, key: key}
}

// RegisterCallHandler registers a CallHandler keyed by input DurableKind, request DurableKind,
// response DurableKind, and result DurableKind.
// HandlerOptions configures retry behavior for the handler.
// Chain .WithRecoveryHandler() on the returned registration to register a recovery
// handler — it is invoked only after all retries are exhausted, instead
// of failing the routine.
func RegisterCallHandler[I Payload, EReq Payload, EResp Payload, T Payload](w *Worker, h CallHandler[I, EReq, EResp, T], opts HandlerOptions) callHandlerReg[I, EReq, EResp, T] {
	var zeroI I
	var zeroEReq EReq
	var zeroEResp EResp
	var zeroT T
	key := durablecore.CallKey(zeroI.DurableKind(), zeroEReq.DurableKind(), zeroEResp.DurableKind(), zeroT.DurableKind())
	runner := func(ctx *Context, rawInput json.RawMessage, rawMsg json.RawMessage, errStr string) (*RunOutput, error) {
		var input I
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("deserialize input: %w", err)
		}
		var externalReq EReq
		if err := json.Unmarshal(rawMsg, &externalReq); err != nil {
			return nil, fmt.Errorf("deserialize externalReq: %w", err)
		}
		resp, cont, herr := h(ctx, input, externalReq)
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
	return callHandlerReg[I, EReq, EResp, T]{w: w, key: key}
}
