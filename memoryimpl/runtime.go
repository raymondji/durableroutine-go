package memoryimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
)

type instance struct {
	id             string
	cases          []stateroutine.Case
	timerDeadlines []time.Time // absolute deadline per case (zero for non-timer)
	queryResults   map[string]any
	signals        map[string][]any // keyed by "send:{stateKind}:{msgKind}"
	done           bool
	result         any
	err            error
	waiters        []chan struct{}
}

// Runtime is an in-memory stateroutine runtime for deterministic unit testing.
type Runtime struct {
	handlers  map[string]stateroutine.HandlerEntry
	clock     *Clock
	instances map[string]*instance
}

// NewRuntime creates a Runtime from a Worker's registered handlers.
func NewRuntime(w *stateroutine.Worker) *Runtime {
	return &Runtime{
		handlers:  w.Handlers(),
		clock:     NewClock(),
		instances: make(map[string]*instance),
	}
}

// Clock returns the controllable clock.
func (r *Runtime) Clock() *Clock {
	return r.clock
}

// Client returns a stateroutine.Client backed by this runtime.
func (r *Runtime) Client() stateroutine.Client {
	return stateroutine.NewClientFrom(&client{runtime: r})
}

// Step advances one instance by one step. Returns true if progress was made.
// Per-instance priority matches Temporal's Select semantics:
//  1. Continue (sole immediate case) — fires immediately
//  2. Buffered signals matching an OnSend case
//  3. Expired timers
//  4. Default (immediate case among multiple) — fires only if no sends/timers are ready
func (r *Runtime) Step() bool {
	for _, inst := range r.instances {
		if inst.done {
			continue
		}
		if r.stepInstance(inst) {
			return true
		}
	}
	return false
}

// stepInstance tries to advance a single instance. Returns true if progress was made.
func (r *Runtime) stepInstance(inst *instance) bool {
	// Continue: sole immediate case fires unconditionally.
	if len(inst.cases) == 1 && inst.cases[0].Immediate() {
		r.fireCase(inst, 0, nil)
		return true
	}

	// Buffered signals matching an OnSend case.
	for i, c := range inst.cases {
		if c.SendName() == "" {
			continue
		}
		signalKey := c.HandlerKey()
		if msgs, ok := inst.signals[signalKey]; ok && len(msgs) > 0 {
			msg := msgs[0]
			inst.signals[signalKey] = msgs[1:]
			r.fireCase(inst, i, msg)
			return true
		}
	}

	// Expired timers.
	for i, c := range inst.cases {
		if c.TimerDuration() == nil {
			continue
		}
		if !r.clock.Now().Before(inst.timerDeadlines[i]) {
			r.fireCase(inst, i, nil)
			return true
		}
	}

	// Default: immediate case among multiple — fires only if nothing else matched.
	for i, c := range inst.cases {
		if c.Immediate() {
			r.fireCase(inst, i, nil)
			return true
		}
	}

	return false
}

// StepAll calls Step repeatedly until no more progress can be made.
func (r *Runtime) StepAll() {
	for r.Step() {
	}
}

// AdvanceTime advances the clock by d, then runs StepAll to fire any timers.
func (r *Runtime) AdvanceTime(d time.Duration) {
	r.clock.Advance(d)
	r.StepAll()
}

// fireCase runs the handler for the given case index on the instance.
func (r *Runtime) fireCase(inst *instance, caseIdx int, msg any) {
	c := inst.cases[caseIdx]
	r.runHandler(inst, c.HandlerKey(), c.State(), msg)
}

// start creates a new instance and runs its initial handler.
func (r *Runtime) start(id string, kind string, state any) error {
	if _, exists := r.instances[id]; exists {
		return fmt.Errorf("stateroutine %s already exists", id)
	}
	inst := &instance{
		id:           id,
		queryResults: make(map[string]any),
		signals:      make(map[string][]any),
	}
	r.instances[id] = inst

	handlerKey := "handler:" + kind
	r.runHandler(inst, handlerKey, state, nil)
	return nil
}

// runHandler looks up the runner, marshals inputs, invokes it, and processes output.
func (r *Runtime) runHandler(inst *instance, handlerKey string, state any, msg any) {
	entry, ok := r.handlers[handlerKey]
	if !ok {
		inst.done = true
		inst.err = fmt.Errorf("no handler registered for key: %s", handlerKey)
		r.closeWaiters(inst)
		return
	}

	rawState, err := json.Marshal(state)
	if err != nil {
		inst.done = true
		inst.err = fmt.Errorf("marshal state: %w", err)
		r.closeWaiters(inst)
		return
	}

	var rawMsg json.RawMessage
	if msg != nil {
		rawMsg, err = json.Marshal(msg)
		if err != nil {
			inst.done = true
			inst.err = fmt.Errorf("marshal msg: %w", err)
			r.closeWaiters(inst)
			return
		}
	}

	sctx := stateroutine.NewContext(context.Background(), inst.id)
	output, err := entry.Runner(sctx, rawState, rawMsg, "")
	if err != nil {
		// Check for a terminal error handler.
		teKey := entry.Options.WithTerminalErrorHandlerKey()
		if teKey != "" {
			if teEntry, ok := r.handlers[teKey]; ok {
				teSctx := stateroutine.NewContext(context.Background(), inst.id)
				teOutput, teErr := teEntry.Runner(teSctx, rawState, rawMsg, err.Error())
				if teErr != nil {
					inst.done = true
					inst.err = teErr
					r.closeWaiters(inst)
					return
				}
				r.processOutput(inst, teOutput, teSctx)
				return
			}
		}
		inst.done = true
		inst.err = err
		r.closeWaiters(inst)
		return
	}

	r.processOutput(inst, output, sctx)
}

// processOutput applies a RunOutput to an instance.
func (r *Runtime) processOutput(inst *instance, output *stateroutine.RunOutput, sctx *stateroutine.Context) {
	// Merge query results.
	for _, qr := range sctx.QueryResults() {
		inst.queryResults[qr.QueryName] = qr.Result
	}

	if output.Done {
		inst.done = true
		if output.Result != nil {
			var result any
			if err := json.Unmarshal(output.Result, &result); err != nil {
				inst.err = fmt.Errorf("unmarshal result: %w", err)
			} else {
				inst.result = result
			}
		}
		r.closeWaiters(inst)
	} else {
		inst.cases = output.Cases
		inst.timerDeadlines = make([]time.Time, len(output.Cases))
		for i, c := range output.Cases {
			if d := c.TimerDuration(); d != nil {
				inst.timerDeadlines[i] = r.clock.Now().Add(*d)
			}
		}
	}

	// Process start requests.
	for _, sr := range sctx.StartRequests() {
		r.start(sr.StateroutineID, sr.StateKind, sr.State)
	}

	// Process send requests — buffer on target instance.
	for _, sr := range sctx.SendRequests() {
		signalKey := "send:" + sr.StateKind + ":" + sr.MsgKind
		target, ok := r.instances[sr.StateroutineID]
		if !ok {
			continue
		}
		target.signals[signalKey] = append(target.signals[signalKey], sr.Msg)
	}
}

// closeWaiters closes all waiter channels on the instance.
func (r *Runtime) closeWaiters(inst *instance) {
	for _, ch := range inst.waiters {
		close(ch)
	}
	inst.waiters = nil
}
