package temporalimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/raymondji/stateroutine/stateroutine"
)

// globalRegistry is set during worker creation and used by RunHandler.
var globalRegistry *registry

type registry struct {
	entries map[string]registryEntry
}

type registryEntry struct {
	handler   any
	options   stateroutine.HandlerOptions
	stateType reflect.Type
	msgType   reflect.Type // nil for HandlerFunc/TerminalErrorFunc
}

// RunHandler is the Temporal activity that executes user handlers.
func RunHandler(ctx context.Context, input ActivityInput) (ActivityOutput, error) {
	if globalRegistry == nil {
		return ActivityOutput{}, fmt.Errorf("handler registry not initialized")
	}

	entry, ok := globalRegistry.entries[input.HandlerKey]
	if !ok {
		return ActivityOutput{}, fmt.Errorf("no handler registered for key: %s", input.HandlerKey)
	}

	// Deserialize state into the concrete type.
	state, err := deserializeAs(input.State, entry.stateType)
	if err != nil {
		return ActivityOutput{}, fmt.Errorf("deserialize state for %s: %w", input.HandlerKey, err)
	}

	// Deserialize message if present.
	var msg any
	if input.Message != nil && entry.msgType != nil {
		msg, err = deserializeAs(input.Message, entry.msgType)
		if err != nil {
			return ActivityOutput{}, fmt.Errorf("deserialize message for %s: %w", input.HandlerKey, err)
		}
	}

	sctx := stateroutine.NewContext(ctx, input.StateroutineID)

	// Build arguments for the handler call.
	args := []reflect.Value{reflect.ValueOf(sctx), reflect.ValueOf(state)}
	if input.Error != "" {
		// Terminal error handler: (ctx, state, [msg,] err)
		if msg != nil {
			args = append(args, reflect.ValueOf(msg))
		}
		args = append(args, reflect.ValueOf(fmt.Errorf("%s", input.Error)))
	} else if msg != nil {
		// SendFunc or CallFunc: (ctx, state, msg)
		args = append(args, reflect.ValueOf(msg))
	}

	results := reflect.ValueOf(entry.handler).Call(args)

	// Parse results based on output count.
	var output ActivityOutput
	numOut := len(results)

	if numOut == 3 {
		// CallFunc / CallTerminalErrorFunc: (Resp, *Suspend, error)
		errVal := results[2]
		if !errVal.IsNil() {
			return ActivityOutput{}, errVal.Interface().(error)
		}

		output.CallResponse = results[0].Interface()
		suspendPtr := results[1] // *Suspend[T]

		if suspendPtr.IsNil() {
			return ActivityOutput{}, fmt.Errorf(
				"handler %s returned nil Suspend with nil error — must always return a Suspend when there is no error",
				input.HandlerKey)
		}

		extractSuspend(suspendPtr, &output)
	} else {
		// HandlerFunc / SendFunc / terminal error: (*Suspend, error)
		errVal := results[1]
		if !errVal.IsNil() {
			return ActivityOutput{}, errVal.Interface().(error)
		}

		suspendPtr := results[0]
		if suspendPtr.IsNil() {
			return ActivityOutput{}, fmt.Errorf(
				"handler %s returned nil Suspend with nil error",
				input.HandlerKey)
		}

		extractSuspend(suspendPtr, &output)
	}

	// Capture side effects from the context.
	for _, qr := range sctx.QueryResults() {
		output.QueryResults = append(output.QueryResults, QueryEntry{
			QueryName: qr.QueryName,
			Result:    qr.Result,
		})
	}
	for _, sr := range sctx.SpawnRequests() {
		output.SpawnRequests = append(output.SpawnRequests, SpawnEntry{
			StateroutineID: sr.StateroutineID,
			StateKind:      sr.StateKind,
			State:          sr.State,
		})
	}
	for _, sr := range sctx.SendRequests() {
		output.SendRequests = append(output.SendRequests, SendEntry{
			StateroutineID: sr.StateroutineID,
			StateKind:      sr.StateKind,
			MsgKind:        sr.MsgKind,
			Msg:            sr.Msg,
		})
	}

	return output, nil
}

// extractSuspend reads the *Suspend[T] via its exported accessors and populates output.
func extractSuspend(suspendPtr reflect.Value, output *ActivityOutput) {
	isDone := suspendPtr.MethodByName("IsDone").Call(nil)[0].Bool()
	if isDone {
		output.Done = true
		output.Result = suspendPtr.MethodByName("Result").Call(nil)[0].Interface()
		return
	}

	casesVal := suspendPtr.MethodByName("Cases").Call(nil)[0]
	numCases := casesVal.Len()
	suspend := &SerializedSuspend{Cases: make([]SerializedCase, numCases)}
	for i := 0; i < numCases; i++ {
		c := casesVal.Index(i).Interface().(stateroutine.Case)
		suspend.Cases[i] = SerializedCase{
			TimerDuration: c.TimerDuration(),
			SendName:      c.SendName(),
			CallName:      c.CallName(),
			Immediate:     c.Immediate(),
			State:         c.State(),
			HandlerKey:    c.HandlerKey(),
		}
	}
	output.Suspend = suspend
}

// deserializeAs converts a value (possibly map[string]interface{} from JSON)
// into the concrete target type via JSON round-trip.
func deserializeAs(value any, targetType reflect.Type) (any, error) {
	if reflect.TypeOf(value) == targetType {
		return value, nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	target := reflect.New(targetType).Interface()
	if err := json.Unmarshal(data, target); err != nil {
		return nil, fmt.Errorf("unmarshal into %v: %w", targetType, err)
	}

	return reflect.ValueOf(target).Elem().Interface(), nil
}
