package temporalimpl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raymondji/stateroutine/stateroutine"
)

type registry struct {
	entries map[string]registryEntry
}

type registryEntry struct {
	runner  stateroutine.HandlerRunner
	options stateroutine.HandlerOptions
}

type handlerActivity struct {
	reg *registry
}

// RunHandler is the Temporal activity that executes user handlers.
func (a *handlerActivity) RunHandler(ctx context.Context, input ActivityInput) (ActivityOutput, error) {
	entry, ok := a.reg.entries[input.HandlerKey]
	if !ok {
		return ActivityOutput{}, fmt.Errorf("no handler registered for key: %s", input.HandlerKey)
	}

	sctx := stateroutine.NewContext(ctx, input.StateroutineID)

	runOut, err := entry.runner(sctx, input.State, input.Message, input.Error)
	if err != nil {
		return ActivityOutput{}, err
	}

	var output ActivityOutput
	if runOut.Done {
		output.Done = true
		output.Result = runOut.Result
	} else {
		suspend := &SerializedSuspend{Cases: make([]SerializedCase, len(runOut.Cases))}
		for i, c := range runOut.Cases {
			stateBytes, err := json.Marshal(c.State())
			if err != nil {
				return ActivityOutput{}, fmt.Errorf("marshal case state: %w", err)
			}
			suspend.Cases[i] = SerializedCase{
				TimerDuration: c.TimerDuration(),
				SendName:      c.SendName(),
				CallName:      c.CallName(),
				Immediate:     c.Immediate(),
				State:         stateBytes,
				HandlerKey:    c.HandlerKey(),
			}
		}
		output.Suspend = suspend
	}
	output.CallResponse = runOut.CallResponse

	// Capture side effects from the context.
	for _, qr := range sctx.QueryResults() {
		resultBytes, err := json.Marshal(qr.Result)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("marshal query result: %w", err)
		}
		output.QueryResults = append(output.QueryResults, QueryEntry{
			QueryName: qr.QueryName,
			Result:    resultBytes,
		})
	}
	for _, sr := range sctx.StartRequests() {
		stateBytes, err := json.Marshal(sr.State)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("marshal start state: %w", err)
		}
		output.StartRequests = append(output.StartRequests, StartEntry{
			StateroutineID: sr.StateroutineID,
			StateKind:      sr.StateKind,
			State:          stateBytes,
		})
	}
	for _, sr := range sctx.SendRequests() {
		msgBytes, err := json.Marshal(sr.Msg)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("marshal send msg: %w", err)
		}
		output.SendRequests = append(output.SendRequests, SendEntry{
			StateroutineID: sr.StateroutineID,
			StateKind:      sr.StateKind,
			MsgKind:        sr.MsgKind,
			Msg:            msgBytes,
		})
	}

	return output, nil
}
