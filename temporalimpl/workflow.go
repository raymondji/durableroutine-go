package temporalimpl

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

// StateroutineWorkflow is the single workflow function for all stateroutines.
func StateroutineWorkflow(ctx workflow.Context, input WorkflowInput) (any, error) {
	handlerKey := input.HandlerKey
	state := input.State

	// Restore query results from previous execution (continue-as-new).
	allQueryResults := append([]QueryEntry{}, input.QueryResults...)
	for _, qr := range input.QueryResults {
		name := qr.QueryName
		result := qr.Result
		if err := workflow.SetQueryHandler(ctx, name, func() (any, error) {
			return result, nil
		}); err != nil {
			return nil, fmt.Errorf("set query handler %s: %w", name, err)
		}
	}

	// Pending calls queue.
	type pendingCall struct {
		handlerKey string
		req        any
		responseCh workflow.Channel
	}
	var pendingCalls []pendingCall

	callNotifyCh := workflow.NewChannel(ctx)

	// message carries the received signal/call message into the next activity.
	var message any

	for {
		// 1. Run the handler as an activity.
		activityInput := ActivityInput{
			HandlerKey:     handlerKey,
			StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			State:          state,
			Message:        message,
		}
		message = nil // consumed

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		})

		var output ActivityOutput
		err := workflow.ExecuteActivity(activityCtx, RunHandler, activityInput).Get(ctx, &output)

		if err != nil {
			teKey := lookupTerminalErrorKey(handlerKey)
			if teKey != "" {
				teInput := ActivityInput{
					HandlerKey:     teKey,
					StateroutineID: activityInput.StateroutineID,
					State:          state,
					Error:          err.Error(),
				}
				err = workflow.ExecuteActivity(activityCtx, RunHandler, teInput).Get(ctx, &output)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		// 2. Apply query results.
		for _, qr := range output.QueryResults {
			name := qr.QueryName
			result := qr.Result
			if err := workflow.SetQueryHandler(ctx, name, func() (any, error) {
				return result, nil
			}); err != nil {
				return nil, fmt.Errorf("set query handler %s: %w", name, err)
			}
		}
		allQueryResults = mergeQueryResults(allQueryResults, output.QueryResults)

		// 3. Handle child spawns.
		for _, spawn := range output.SpawnRequests {
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:        spawn.StateroutineID,
				ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
			})
			childInput := WorkflowInput{
				HandlerKey: "handler:" + spawn.StateKind,
				State:      spawn.State,
			}
			workflow.ExecuteChildWorkflow(childCtx, StateroutineWorkflow, childInput)
		}

		// 4. Handle send requests.
		for _, sr := range output.SendRequests {
			signalName := "send:" + sr.StateKind + ":" + sr.MsgKind
			workflow.SignalExternalWorkflow(ctx, sr.StateroutineID, "", signalName, sr.Msg)
		}

		// 5. Check if done.
		if output.Done {
			return output.Result, nil
		}

		// 6. Interpret Suspend cases.
		suspend := output.Suspend
		if suspend == nil || len(suspend.Cases) == 0 {
			return nil, fmt.Errorf("handler %s returned no suspend cases and is not done", handlerKey)
		}

		// If the only case is immediate (Continue), skip the selector.
		if len(suspend.Cases) == 1 && suspend.Cases[0].Immediate {
			handlerKey = suspend.Cases[0].HandlerKey
			state = suspend.Cases[0].State
			goto continueAsNewCheck
		}

		// Register Update handlers for OnCall cases.
		for _, c := range suspend.Cases {
			if c.CallName == "" {
				continue
			}
			c := c
			workflow.SetUpdateHandler(ctx, c.CallName,
				func(ctx workflow.Context, req any) (any, error) {
					responseCh := workflow.NewChannel(ctx)
					pendingCalls = append(pendingCalls, pendingCall{
						handlerKey: c.HandlerKey,
						req:        req,
						responseCh: responseCh,
					})
					callNotifyCh.SendAsync(true)

					var resp any
					responseCh.Receive(ctx, &resp)
					if errResp, ok := resp.(error); ok {
						return nil, errResp
					}
					return resp, nil
				},
			)
		}

		{
			// Build a selector with priority: timers > calls > sends > default.
			sel := workflow.NewSelector(ctx)

			// Priority 1: Timers
			for _, c := range suspend.Cases {
				if c.TimerDuration == nil {
					continue
				}
				c := c
				timer := workflow.NewTimer(ctx, *c.TimerDuration)
				sel.AddFuture(timer, func(f workflow.Future) {
					handlerKey = c.HandlerKey
					state = c.State
				})
			}

			// Priority 2: Calls (via notification channel)
			hasCallCases := false
			for _, c := range suspend.Cases {
				if c.CallName != "" {
					hasCallCases = true
					break
				}
			}
			if hasCallCases {
				sel.AddReceive(callNotifyCh, func(ch workflow.ReceiveChannel, more bool) {
					var discard any
					ch.Receive(ctx, &discard)

					pc := pendingCalls[0]
					pendingCalls = pendingCalls[1:]

					callInput := ActivityInput{
						HandlerKey:     pc.handlerKey,
						StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
						State:          state,
						Message:        pc.req,
					}

					callActivityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
					})

					var callOutput ActivityOutput
					err := workflow.ExecuteActivity(callActivityCtx, RunHandler, callInput).Get(ctx, &callOutput)
					if err != nil {
						pc.responseCh.Send(ctx, err)
						return
					}

					pc.responseCh.Send(ctx, callOutput.CallResponse)

					if callOutput.Suspend != nil && len(callOutput.Suspend.Cases) > 0 {
						handlerKey = callOutput.Suspend.Cases[0].HandlerKey
						state = callOutput.Suspend.Cases[0].State
					}
				})
			}

			// Priority 3: Sends (via Temporal signal channels)
			for _, c := range suspend.Cases {
				if c.SendName == "" {
					continue
				}
				c := c
				// The composite signal name matches what the client sends.
				compositeName := "send:" + c.HandlerKey[len("send:"):]
				signalCh := workflow.GetSignalChannel(ctx, compositeName)
				sel.AddReceive(signalCh, func(ch workflow.ReceiveChannel, more bool) {
					var msg any
					ch.Receive(ctx, &msg)
					handlerKey = c.HandlerKey
					state = c.State
					message = msg
				})
			}

			// Priority 4: Default
			for _, c := range suspend.Cases {
				if !c.Immediate {
					continue
				}
				c := c
				sel.AddDefault(func() {
					handlerKey = c.HandlerKey
					state = c.State
				})
			}

			sel.Select(ctx)
		}

	continueAsNewCheck:
		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
			for len(pendingCalls) > 0 {
				pc := pendingCalls[0]
				pendingCalls = pendingCalls[1:]

				callInput := ActivityInput{
					HandlerKey:     pc.handlerKey,
					StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
					State:          state,
					Message:        pc.req,
				}
				callActivityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: 30 * time.Second,
				})
				var callOutput ActivityOutput
				err := workflow.ExecuteActivity(callActivityCtx, RunHandler, callInput).Get(ctx, &callOutput)
				if err != nil {
					pc.responseCh.Send(ctx, err)
					continue
				}
				pc.responseCh.Send(ctx, callOutput.CallResponse)

				if callOutput.Suspend != nil && len(callOutput.Suspend.Cases) > 0 {
					handlerKey = callOutput.Suspend.Cases[0].HandlerKey
					state = callOutput.Suspend.Cases[0].State
				}
			}

			return nil, workflow.NewContinueAsNewError(ctx, StateroutineWorkflow, WorkflowInput{
				HandlerKey:   handlerKey,
				State:        state,
				QueryResults: allQueryResults,
			})
		}
	}
}

// lookupTerminalErrorKey checks if a terminal error handler is registered.
func lookupTerminalErrorKey(handlerKey string) string {
	if globalRegistry == nil {
		return ""
	}
	entry, ok := globalRegistry.entries[handlerKey]
	if !ok {
		return ""
	}
	teKey := entry.options.OnTerminalErrorKey()
	if teKey == "" {
		return ""
	}
	if _, ok := globalRegistry.entries[teKey]; ok {
		return teKey
	}
	return ""
}

func mergeQueryResults(existing, newEntries []QueryEntry) []QueryEntry {
	result := append([]QueryEntry{}, existing...)
	for _, nqr := range newEntries {
		found := false
		for i, eqr := range result {
			if eqr.QueryName == nqr.QueryName {
				result[i] = nqr
				found = true
				break
			}
		}
		if !found {
			result = append(result, nqr)
		}
	}
	return result
}
