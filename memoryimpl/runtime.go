package memoryimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
)

// signal is a message delivered to an instance's signalChan.
type signal struct {
	key string
	msg any
}

// callReq is a synchronous call request delivered to an instance's callChan.
type callReq struct {
	callName   string
	handlerKey string
	req        any
	respCh     chan callResp
}

// callResp is the response to a callReq.
type callResp struct {
	result any
	err    error
}

// instance is a running stateroutine backed by its own goroutine.
type instance struct {
	id         string
	signalChan chan signal  // buffered, receives sends
	callChan   chan callReq // unbuffered, for synchronous calls
	doneChan   chan struct{}

	mu           sync.Mutex
	queryResults map[string]any

	// Owned by instance goroutine, no external access needed.
	buffered map[string][]any // signals for future states

	// Only read after doneChan is closed.
	result any
	err    error
}

// Runtime is an in-memory stateroutine runtime where each instance runs in its own goroutine.
type Runtime struct {
	handlers map[string]stateroutine.HandlerEntry

	mu        sync.Mutex
	instances map[string]*instance
}

// NewRuntime creates a Runtime from a Worker's registered handlers.
func NewRuntime(w *stateroutine.Worker) *Runtime {
	return &Runtime{
		handlers:  w.Handlers(),
		instances: make(map[string]*instance),
	}
}

// Client returns a stateroutine.Client backed by this runtime.
func (r *Runtime) Client() stateroutine.Client {
	return stateroutine.NewClientFrom(&client{runtime: r})
}

// start creates a new instance and launches its goroutine. Caller must NOT hold r.mu.
func (r *Runtime) start(id string, kind string, state any) error {
	r.mu.Lock()
	if _, exists := r.instances[id]; exists {
		r.mu.Unlock()
		return fmt.Errorf("stateroutine %s already exists", id)
	}
	inst := &instance{
		id:           id,
		signalChan:   make(chan signal, 1024),
		callChan:     make(chan callReq),
		doneChan:     make(chan struct{}),
		queryResults: make(map[string]any),
		buffered:     make(map[string][]any),
	}
	r.instances[id] = inst
	r.mu.Unlock()

	handlerKey := "handler:" + kind
	go r.runInstance(inst, handlerKey, state, nil)
	return nil
}

// runInstance is the goroutine entry point for a stateroutine instance.
func (r *Runtime) runInstance(inst *instance, handlerKey string, state any, msg any) {
	defer close(inst.doneChan)

	// respCh is non-nil when the current handler invocation is from a Call.
	var respCh chan callResp

	for {
		output, err := r.runHandler(inst, handlerKey, state, msg)
		if err != nil {
			if respCh != nil {
				respCh <- callResp{err: err}
				respCh = nil
			}
			inst.err = err
			return
		}

		// For calls, send the response back to the caller.
		if respCh != nil {
			var callResult any
			if output.CallResponse != nil {
				if err := json.Unmarshal(output.CallResponse, &callResult); err != nil {
					respCh <- callResp{err: fmt.Errorf("unmarshal call response: %w", err)}
					inst.err = err
					return
				}
			}
			respCh <- callResp{result: callResult}
			respCh = nil
		}

		if output.Done {
			if output.Result != nil {
				var result any
				if err := json.Unmarshal(output.Result, &result); err != nil {
					inst.err = fmt.Errorf("unmarshal result: %w", err)
				} else {
					inst.result = result
				}
			}
			return
		}

		// Wait for a case to fire.
		var caseIdx int
		var caseMsg any
		caseIdx, caseMsg, respCh = r.waitForCase(inst, output.Cases)
		c := output.Cases[caseIdx]
		handlerKey = c.HandlerKey()
		state = c.State()
		msg = caseMsg
	}
}

// runHandler invokes a handler, with terminal error handler fallback.
func (r *Runtime) runHandler(inst *instance, handlerKey string, state any, msg any) (*stateroutine.RunOutput, error) {
	entry, ok := r.handlers[handlerKey]
	if !ok {
		return nil, fmt.Errorf("no handler registered for key: %s", handlerKey)
	}

	rawState, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}

	var rawMsg json.RawMessage
	if msg != nil {
		rawMsg, err = json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal msg: %w", err)
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
					return nil, teErr
				}
				r.applyContextEffects(inst, teSctx)
				return teOutput, nil
			}
		}
		return nil, err
	}

	r.applyContextEffects(inst, sctx)
	return output, nil
}

// applyContextEffects merges query results and processes side effects from the context.
func (r *Runtime) applyContextEffects(inst *instance, sctx *stateroutine.Context) {
	// Merge query results.
	qrs := sctx.QueryResults()
	if len(qrs) > 0 {
		inst.mu.Lock()
		for _, qr := range qrs {
			inst.queryResults[qr.QueryName] = qr.Result
		}
		inst.mu.Unlock()
	}

	// Start child stateroutines.
	for _, sr := range sctx.StartRequests() {
		r.start(sr.StateroutineID, sr.StateKind, sr.State)
	}

	// Send signals to other instances.
	for _, sr := range sctx.SendRequests() {
		signalKey := "send:" + sr.StateKind + ":" + sr.MsgKind
		r.mu.Lock()
		target, ok := r.instances[sr.StateroutineID]
		r.mu.Unlock()
		if !ok {
			continue
		}
		target.signalChan <- signal{key: signalKey, msg: sr.Msg}
	}
}

// waitForCase blocks until one of the cases fires. Returns the case index,
// message, and for calls the response channel (nil for non-call cases).
func (r *Runtime) waitForCase(inst *instance, cases []stateroutine.Case) (int, any, chan callResp) {
	// 1. Continue: sole immediate case fires unconditionally.
	if len(cases) == 1 && cases[0].Immediate() {
		return 0, nil, nil
	}

	// Compute timer deadlines.
	var timers []timerInfo
	for i, c := range cases {
		if d := c.TimerDuration(); d != nil {
			timers = append(timers, timerInfo{idx: i, deadline: time.Now().Add(*d)})
		}
	}

	hasDefault := false
	defaultIdx := -1
	for i, c := range cases {
		if c.Immediate() {
			hasDefault = true
			defaultIdx = i
			break
		}
	}

	for {
		// 2. Check buffered signals.
		for i, c := range cases {
			if c.SendName() == "" {
				continue
			}
			key := c.HandlerKey()
			if msgs, ok := inst.buffered[key]; ok && len(msgs) > 0 {
				msg := msgs[0]
				inst.buffered[key] = msgs[1:]
				return i, msg, nil
			}
		}

		// 3. Check expired timers.
		now := time.Now()
		for _, t := range timers {
			if !now.Before(t.deadline) {
				return t.idx, nil, nil
			}
		}

		// 4. Non-blocking channel checks (priority: calls > sends).
		if idx, req, ok := r.tryReceiveCall(inst, cases); ok {
			return idx, req.req, req.respCh
		}

		if idx, msg, ok := r.tryReceiveSignal(inst, cases); ok {
			return idx, msg, nil
		}

		// 5. Default: immediate among multiple.
		if hasDefault {
			return defaultIdx, nil, nil
		}

		// 6. Blocking select on channels + timers.
		idx, msg, respCh := r.blockForCase(inst, cases, timers)
		if idx >= 0 {
			return idx, msg, respCh
		}
		// idx < 0 means a signal was buffered; loop back.
	}
}

// tryReceiveCall does a non-blocking receive on callChan and matches against OnCall cases.
func (r *Runtime) tryReceiveCall(inst *instance, cases []stateroutine.Case) (int, callReq, bool) {
	hasCall := false
	for _, c := range cases {
		if c.CallName() != "" {
			hasCall = true
			break
		}
	}
	if !hasCall {
		return 0, callReq{}, false
	}

	select {
	case req := <-inst.callChan:
		for i, c := range cases {
			if c.CallName() != "" && req.callName == c.CallName() {
				return i, req, true
			}
		}
		// No matching call case — error response and continue.
		req.respCh <- callResp{err: fmt.Errorf("no matching OnCall case for %s", req.handlerKey)}
		return 0, callReq{}, false
	default:
		return 0, callReq{}, false
	}
}

// tryReceiveSignal does a non-blocking receive on signalChan and matches against OnSend cases.
// If the signal doesn't match, it's buffered for future states, and we return (0, nil, false).
func (r *Runtime) tryReceiveSignal(inst *instance, cases []stateroutine.Case) (int, any, bool) {
	select {
	case sig := <-inst.signalChan:
		for i, c := range cases {
			if c.SendName() != "" && c.HandlerKey() == sig.key {
				return i, sig.msg, true
			}
		}
		// Buffer for future states.
		inst.buffered[sig.key] = append(inst.buffered[sig.key], sig.msg)
		return 0, nil, false
	default:
		return 0, nil, false
	}
}

// blockForCase does a blocking select on signalChan, callChan, and the nearest timer.
// Returns (caseIdx, msg, respCh) if a case fires, or (-1, nil, nil) if a signal was
// buffered and the caller should loop.
func (r *Runtime) blockForCase(inst *instance, cases []stateroutine.Case, timers []timerInfo) (int, any, chan callResp) {
	// Find nearest timer.
	var timerChan <-chan time.Time
	nearestTimerIdx := -1
	var nearestDeadline time.Time
	for _, t := range timers {
		if nearestTimerIdx == -1 || t.deadline.Before(nearestDeadline) {
			nearestDeadline = t.deadline
			nearestTimerIdx = t.idx
		}
	}
	if nearestTimerIdx >= 0 {
		d := time.Until(nearestDeadline)
		if d <= 0 {
			return nearestTimerIdx, nil, nil
		}
		timerChan = time.After(d)
	}

	if timerChan != nil {
		select {
		case sig := <-inst.signalChan:
			return r.matchSignal(inst, cases, sig)
		case req := <-inst.callChan:
			return r.matchCall(inst, cases, req)
		case <-timerChan:
			return nearestTimerIdx, nil, nil
		}
	} else {
		select {
		case sig := <-inst.signalChan:
			return r.matchSignal(inst, cases, sig)
		case req := <-inst.callChan:
			return r.matchCall(inst, cases, req)
		}
	}
}

// matchSignal tries to match a received signal against OnSend cases.
// Returns (-1, nil, nil) if the signal was buffered.
func (r *Runtime) matchSignal(inst *instance, cases []stateroutine.Case, sig signal) (int, any, chan callResp) {
	for i, c := range cases {
		if c.SendName() != "" && c.HandlerKey() == sig.key {
			return i, sig.msg, nil
		}
	}
	inst.buffered[sig.key] = append(inst.buffered[sig.key], sig.msg)
	return -1, nil, nil
}

// matchCall tries to match a received call against OnCall cases.
// Returns (-1, nil, nil) if no match (sends error response).
func (r *Runtime) matchCall(inst *instance, cases []stateroutine.Case, req callReq) (int, any, chan callResp) {
	for i, c := range cases {
		if c.CallName() != "" && req.callName == c.CallName() {
			return i, req.req, req.respCh
		}
	}
	req.respCh <- callResp{err: fmt.Errorf("no matching OnCall case for %s", req.handlerKey)}
	return -1, nil, nil
}

// timerInfo holds a timer case index and its absolute deadline.
type timerInfo struct {
	idx      int
	deadline time.Time
}
