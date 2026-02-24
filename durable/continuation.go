package durable

import (
	"time"

	"github.com/raymondji/durableroutine-go/internal/durablecore"
)

// Continuation describes what a routine should wait for before invoking the next
// handler. T is the routine's result type, returned via Done and retrieved
// via Get. It is an opaque value built via Done, After, Select, etc.
//
// Every constructor (After, ReceiveSend, ReceiveCall, Default) returns a
// *Continuation[T] that works standalone. To wait on multiple conditions, pass
// them to Select which merges their cases.
type Continuation[T Payload] struct {
	done   bool
	result T
	cases  []Case
}

// Unit is a convenience type for routines that don't produce a result.
type Unit struct{}

func (Unit) DurableKind() string { return "unit" }

// Done returns a Continuation that completes the routine with the given result.
// Clients can retrieve the result via Get.
func Done[T Payload](result T) *Continuation[T] {
	return &Continuation[T]{done: true, result: result}
}

// Case is a single wait condition inside a Continuation (a timer, inbox receive,
// or method call) paired with the handler to invoke when that condition fires.
// Internally type-erased; type safety is enforced at construction via generic
// constructor functions.
type Case struct {
	// Exactly one of the following is set (or none if immediate is true).
	timerDuration *time.Duration
	sendName      string
	callName      string

	// immediate marks this case as firing without waiting. Used by Continue
	// (sole case in a Continuation — skip the selector entirely) and Default
	// (inside a Select — maps to Temporal's sel.AddDefault).
	immediate bool

	// handler and state are stored as any because each case may carry
	// different state and handler types, erased at this level.
	state any

	// handlerKey is the composite key for looking up the registered handler
	// (e.g., "send:booking.reserved:payment:booking-result"). Set by constructors for
	// non-query cases.
	handlerKey string
}

// After returns a Continuation that fires after the given duration. Use standalone
// as a simple "sleep then continue", or pass to Select alongside other Continuations.
func After[S Payload, T Payload](d time.Duration, handler Handler[S, T], state S) *Continuation[T] {
	var zeroT T
	return &Continuation[T]{
		cases: []Case{{
			timerDuration: &d,
			state:         state,
			handlerKey:    durablecore.HandlerKey(state.DurableKind(), zeroT.DurableKind()),
		}},
	}
}

// ReceiveSend returns a Continuation that fires when a message arrives on the inbox
// identified by M.DurableKind(). Use standalone to wait for a single message, or pass
// to Select alongside other Continuations. Maps to a Temporal Signal handler.
func ReceiveSend[S Payload, M Payload, T Payload](handler SendHandler[S, M, T], state S) *Continuation[T] {
	var zeroM M
	var zeroT T
	return &Continuation[T]{
		cases: []Case{{
			sendName:   zeroM.DurableKind(),
			state:      state,
			handlerKey: durablecore.SendKey(state.DurableKind(), zeroM.DurableKind(), zeroT.DurableKind()),
		}},
	}
}

// ReceiveCall returns a Continuation that fires when a client calls the method identified
// by Req.DurableKind(). Use standalone to wait for a single call, or pass to Select
// alongside other Continuations. Maps to a Temporal Update handler.
func ReceiveCall[S Payload, Req Payload, Resp Payload, T Payload](handler CallHandler[S, Req, Resp, T], state S) *Continuation[T] {
	var zeroReq Req
	var zeroResp Resp
	var zeroT T
	return &Continuation[T]{
		cases: []Case{{
			callName:   zeroReq.DurableKind(),
			state:      state,
			handlerKey: durablecore.CallKey(state.DurableKind(), zeroReq.DurableKind(), zeroResp.DurableKind(), zeroT.DurableKind()),
		}},
	}
}

// Select merges multiple Continuations into one that waits for the first to fire,
// similar to Go's select statement. T is inferred from the arguments.
//
// This prioritizes cases in the following way:
// 1. Timers (as timers are often used for timeouts, we don't want them to be blocked by other work)
// 2. Calls (caller is waiting for a response)
// 3. Sends
// 4. Default (if present, runs if no other cases are ready)
func Select[T Payload](conts ...*Continuation[T]) *Continuation[T] {
	var cases []Case
	for _, c := range conts {
		cases = append(cases, c.cases...)
	}
	return &Continuation[T]{cases: cases}
}

// Continue builds a Continuation that checkpoints state (continue-as-new boundary)
// and immediately invokes the handler as the next activity, without waiting for
// a timer, inbox, or method. Use this for multi-step processing where you want
// explicit continue-as-new boundaries between steps.
func Continue[S Payload, T Payload](handler Handler[S, T], state S) *Continuation[T] {
	var zeroT T
	return &Continuation[T]{
		cases: []Case{{
			immediate:  true,
			state:      state,
			handlerKey: durablecore.HandlerKey(state.DurableKind(), zeroT.DurableKind()),
		}},
	}
}

// Default returns a Continuation that fires immediately if no other cases in a
// Select are ready. Maps to Temporal's sel.AddDefault(). Use this to drain
// buffered sends: if no sends are pending, the default case fires.
// Must be used inside Select alongside other Continuations.
func Default[S Payload, T Payload](handler Handler[S, T], state S) *Continuation[T] {
	var zeroT T
	return &Continuation[T]{
		cases: []Case{{
			immediate:  true,
			state:      state,
			handlerKey: durablecore.HandlerKey(state.DurableKind(), zeroT.DurableKind()),
		}},
	}
}

// --- Exported accessors for implementation packages ---

// IsDone returns true if the Continuation completes the routine.
func (s *Continuation[T]) IsDone() bool { return s.done }

// Result returns the routine result (only meaningful when IsDone is true).
func (s *Continuation[T]) Result() T { return s.result }

// Cases returns the wait conditions.
func (s *Continuation[T]) Cases() []Case { return s.cases }

// TimerDuration returns the timer duration, or nil if this is not a timer case.
func (c Case) TimerDuration() *time.Duration { return c.timerDuration }

// SendName returns the send name, or "" if this is not a send case.
func (c Case) SendName() string { return c.sendName }

// CallName returns the call name, or "" if this is not a call case.
func (c Case) CallName() string { return c.callName }

// Immediate returns true if this case fires without waiting.
func (c Case) Immediate() bool { return c.immediate }

// State returns the handler state for this case.
func (c Case) State() any { return c.state }

// HandlerKey returns the composite handler lookup key.
func (c Case) HandlerKey() string { return c.handlerKey }
