package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/raymondji/durableroutine-go/internal/durablecore"
	"github.com/raymondji/durableroutine-go/durable"
)

// sendMsg is a message delivered to an instance's sendCh.
type sendMsg struct {
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

// instance is a running routine backed by its own goroutine.
type instance struct {
	id       string
	sendCh   chan sendMsg // buffered, receives sends
	callChan chan callReq // unbuffered, for synchronous calls
	doneChan chan struct{}

	mu           sync.Mutex
	queryResults map[string]any

	// Owned by instance goroutine, no external access needed.
	buffered map[string][]any // sends for future states

	// Only read after doneChan is closed.
	result any
	err    error
}

// Runtime is an in-memory routine runtime where each instance runs in its own goroutine.
type Runtime struct {
	handlers map[string]durable.HandlerEntry

	mu        sync.Mutex
	instances map[string]*instance
}

// NewRuntime creates a Runtime from a Worker's registered handlers.
func NewRuntime(w *durable.Worker) *Runtime {
	return &Runtime{
		handlers:  w.Handlers(),
		instances: make(map[string]*instance),
	}
}

// Client returns a durable.Client backed by this runtime.
func (r *Runtime) Client() durable.Client {
	return durable.NewClientFrom(&client{runtime: r})
}

// start creates a new instance and launches its goroutine. Caller must NOT hold r.mu.
func (r *Runtime) start(id string, kind string, resultKind string, input any) error {
	r.mu.Lock()
	if _, exists := r.instances[id]; exists {
		r.mu.Unlock()
		return fmt.Errorf("routine %s already exists", id)
	}
	inst := &instance{
		id:           id,
		sendCh:       make(chan sendMsg, 1024),
		callChan:     make(chan callReq),
		doneChan:     make(chan struct{}),
		queryResults: make(map[string]any),
		buffered:     make(map[string][]any),
	}
	r.instances[id] = inst
	r.mu.Unlock()

	handlerKey := durablecore.HandlerKey(kind, resultKind)
	go r.runInstance(inst, handlerKey, input, nil)
	return nil
}

// runInstance is the goroutine entry point for a routine instance.
func (r *Runtime) runInstance(inst *instance, handlerKey string, input any, msg any) {
	defer close(inst.doneChan)

	// respCh is non-nil when the current handler invocation is from a Call.
	var respCh chan callResp

	for {
		output, err := r.runHandler(inst, handlerKey, input, msg)
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
		input = c.Input()
		msg = caseMsg
	}
}

// runHandler invokes a handler, with terminal error handler fallback.
func (r *Runtime) runHandler(inst *instance, handlerKey string, input any, msg any) (*durable.RunOutput, error) {
	entry, ok := r.handlers[handlerKey]
	if !ok {
		return nil, fmt.Errorf("no handler registered for key: %s", handlerKey)
	}

	rawInput, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	var rawMsg json.RawMessage
	if msg != nil {
		rawMsg, err = json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal msg: %w", err)
		}
	}

	sctx := durable.NewContext(context.Background(), inst.id)
	output, err := entry.Runner(sctx, rawInput, rawMsg, "")
	if err != nil {
		// Check for a terminal error handler.
		teKey := entry.Options.WithTerminalErrorHandlerKey()
		if teKey != "" {
			if teEntry, ok := r.handlers[teKey]; ok {
				teSctx := durable.NewContext(context.Background(), inst.id)
				teOutput, teErr := teEntry.Runner(teSctx, rawInput, rawMsg, err.Error())
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
func (r *Runtime) applyContextEffects(inst *instance, sctx *durable.Context) {
	// Merge query results.
	qrs := sctx.QueryResults()
	if len(qrs) > 0 {
		inst.mu.Lock()
		for _, qr := range qrs {
			inst.queryResults[qr.QueryName] = qr.Result
		}
		inst.mu.Unlock()
	}

	// Start child routines.
	for _, sr := range sctx.StartRequests() {
		r.start(sr.RoutineID, sr.InputKind, sr.ResultKind, sr.Input)
	}

	// Send messages to other instances.
	for _, sr := range sctx.SendRequests() {
		sendKey := durablecore.SendKey(sr.InputKind, sr.ExternalInputKind, sr.ResultKind)
		r.mu.Lock()
		target, ok := r.instances[sr.RoutineID]
		r.mu.Unlock()
		if !ok {
			continue
		}
		target.sendCh <- sendMsg{key: sendKey, msg: sr.Msg}
	}
}

// waitForCase blocks until one of the cases fires. Returns the case index,
// message, and for calls the response channel (nil for non-call cases).
func (r *Runtime) waitForCase(inst *instance, cases []durable.Case) (int, any, chan callResp) {
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
		// 2. Check buffered sends.
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

		if idx, msg, ok := r.tryReceiveSend(inst, cases); ok {
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
		// idx < 0 means a send was buffered; loop back.
	}
}

// tryReceiveCall does a non-blocking receive on callChan and matches against ReceiveCall cases.
func (r *Runtime) tryReceiveCall(inst *instance, cases []durable.Case) (int, callReq, bool) {
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
		req.respCh <- callResp{err: fmt.Errorf("no matching ReceiveCall case for %s", req.handlerKey)}
		return 0, callReq{}, false
	default:
		return 0, callReq{}, false
	}
}

// tryReceiveSend does a non-blocking receive on sendCh and matches against ReceiveSend cases.
// If the send doesn't match, it's buffered for future states, and we return (0, nil, false).
func (r *Runtime) tryReceiveSend(inst *instance, cases []durable.Case) (int, any, bool) {
	select {
	case sm := <-inst.sendCh:
		for i, c := range cases {
			if c.SendName() != "" && c.HandlerKey() == sm.key {
				return i, sm.msg, true
			}
		}
		// Buffer for future states.
		inst.buffered[sm.key] = append(inst.buffered[sm.key], sm.msg)
		return 0, nil, false
	default:
		return 0, nil, false
	}
}

// blockForCase does a blocking select on sendCh, callChan, and the nearest timer.
// Returns (caseIdx, msg, respCh) if a case fires, or (-1, nil, nil) if a send was
// buffered and the caller should loop.
func (r *Runtime) blockForCase(inst *instance, cases []durable.Case, timers []timerInfo) (int, any, chan callResp) {
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
		case sm := <-inst.sendCh:
			return r.matchSend(inst, cases, sm)
		case req := <-inst.callChan:
			return r.matchCall(inst, cases, req)
		case <-timerChan:
			return nearestTimerIdx, nil, nil
		}
	} else {
		select {
		case sm := <-inst.sendCh:
			return r.matchSend(inst, cases, sm)
		case req := <-inst.callChan:
			return r.matchCall(inst, cases, req)
		}
	}
}

// matchSend tries to match a received send against ReceiveSend cases.
// Returns (-1, nil, nil) if the send was buffered.
func (r *Runtime) matchSend(inst *instance, cases []durable.Case, sm sendMsg) (int, any, chan callResp) {
	for i, c := range cases {
		if c.SendName() != "" && c.HandlerKey() == sm.key {
			return i, sm.msg, nil
		}
	}
	inst.buffered[sm.key] = append(inst.buffered[sm.key], sm.msg)
	return -1, nil, nil
}

// matchCall tries to match a received call against ReceiveCall cases.
// Returns (-1, nil, nil) if no match (sends error response).
func (r *Runtime) matchCall(inst *instance, cases []durable.Case, req callReq) (int, any, chan callResp) {
	for i, c := range cases {
		if c.CallName() != "" && req.callName == c.CallName() {
			return i, req.req, req.respCh
		}
	}
	req.respCh <- callResp{err: fmt.Errorf("no matching ReceiveCall case for %s", req.handlerKey)}
	return -1, nil, nil
}

// timerInfo holds a timer case index and its absolute deadline.
type timerInfo struct {
	idx      int
	deadline time.Time
}
