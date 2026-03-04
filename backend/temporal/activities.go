package temporal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
)

type registry struct {
	entries map[string]registryEntry
}

type registryEntry struct {
	runner  durable.HandlerRunner
	options durable.HandlerOptions
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

	sctx := durable.NewContext(ctx, input.RoutineID)

	runOut, err := entry.runner(sctx, input.Input, input.Message, input.Error)
	if err != nil {
		return ActivityOutput{}, err
	}

	var output ActivityOutput
	if runOut.Done {
		output.Done = true
		output.Result = runOut.Result
	} else {
		cont := &SerializedContinuation{Cases: make([]SerializedCase, len(runOut.Cases))}
		for i, c := range runOut.Cases {
			stateBytes, err := json.Marshal(c.Input())
			if err != nil {
				return ActivityOutput{}, fmt.Errorf("marshal case state: %w", err)
			}
			cont.Cases[i] = SerializedCase{
				TimerDuration: c.TimerDuration(),
				SendName:      c.SendName(),
				CallName:      c.CallName(),
				Immediate:     c.Immediate(),
				Input:         stateBytes,
				HandlerKey:    c.HandlerKey(),
			}
		}
		output.Continuation = cont
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
		stateBytes, err := json.Marshal(sr.Input)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("marshal start input: %w", err)
		}
		output.StartRequests = append(output.StartRequests, StartEntry{
			RoutineID:  sr.RoutineID,
			InputKind:  sr.InputKind,
			ResultKind: sr.ResultKind,
			Input:      stateBytes,
		})
	}
	for _, sr := range sctx.SendRequests() {
		msgBytes, err := json.Marshal(sr.Msg)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("marshal send msg: %w", err)
		}
		output.SendRequests = append(output.SendRequests, SendEntry{
			RoutineID:        sr.RoutineID,
			InputKind:        sr.InputKind,
			ExternalInputKind: sr.ExternalInputKind,
			ResultKind:       sr.ResultKind,
			Msg:              msgBytes,
		})
	}

	return output, nil
}
