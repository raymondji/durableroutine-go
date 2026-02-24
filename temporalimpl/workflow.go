package temporalimpl

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
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
		state      any // state from the suspend case
		responseCh workflow.Channel
	}
	var pendingCalls []pendingCall

	callNotifyCh := workflow.NewChannel(ctx)

	// message carries the received signal/call message into the next activity.
	var message any

	// callHandlerOutput stores the output from a call handler processed in the
	// selector callback. When set, the main loop skips step 1 (running the
	// handler activity) and processes this output directly through steps 2-6.
	var callHandlerOutput *ActivityOutput

	for {
		var output ActivityOutput

		if callHandlerOutput != nil {
			// A call handler was processed in the previous iteration's selector.
			// Use its output directly instead of running a new activity.
			output = *callHandlerOutput
			callHandlerOutput = nil
		} else {
			// 1. Run the handler as an activity.
			activityInput := ActivityInput{
				HandlerKey:     handlerKey,
				StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				State:          state,
				Message:        message,
			}
			message = nil // consumed

			activityCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions(handlerKey))

			err := workflow.ExecuteActivity(activityCtx, RunHandler, activityInput).Get(ctx, &output)

			if err != nil {
				teKey := lookupTerminalErrorKey(handlerKey)
				if teKey != "" {
					teInput := ActivityInput{
						HandlerKey:     teKey,
						StateroutineID: activityInput.StateroutineID,
						State:          state,
						Message:        activityInput.Message,
						Error:          err.Error(),
					}
					teActivityCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions(teKey))
					err = workflow.ExecuteActivity(teActivityCtx, RunHandler, teInput).Get(ctx, &output)
					if err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
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

		// 4. Handle send requests — wait for each signal to be acknowledged.
		for _, sr := range output.SendRequests {
			signalName := "send:" + sr.StateKind + ":" + sr.MsgKind
			f := workflow.SignalExternalWorkflow(ctx, sr.StateroutineID, "", signalName, sr.Msg)
			if err := f.Get(ctx, nil); err != nil {
				// Signal delivery failed (e.g., target workflow not found).
				// Log but continue — best-effort delivery.
				workflow.GetLogger(ctx).Warn("signal delivery failed",
					"target", sr.StateroutineID, "signal", signalName, "error", err)
			}
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
			// Use HandlerKey as the update name — matches the client's "call:{stateKind}:{reqKind}".
			workflow.SetUpdateHandler(ctx, c.HandlerKey,
				func(ctx workflow.Context, req any) (any, error) {
					responseCh := workflow.NewChannel(ctx)
					pendingCalls = append(pendingCalls, pendingCall{
						handlerKey: c.HandlerKey,
						req:        req,
						state:      c.State,
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
						State:          pc.state,
						Message:        pc.req,
					}

					callActivityCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions(pc.handlerKey))

					var callOutput ActivityOutput
					err := workflow.ExecuteActivity(callActivityCtx, RunHandler, callInput).Get(ctx, &callOutput)
					if err != nil {
						teKey := lookupTerminalErrorKey(pc.handlerKey)
						if teKey != "" {
							teInput := ActivityInput{
								HandlerKey:     teKey,
								StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
								State:          pc.state,
								Message:        pc.req,
								Error:          err.Error(),
							}
							teActivityCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions(teKey))
							var teOutput ActivityOutput
							teErr := workflow.ExecuteActivity(teActivityCtx, RunHandler, teInput).Get(ctx, &teOutput)
							if teErr != nil {
								pc.responseCh.Send(ctx, teErr)
								// Re-use current suspend to re-establish the same wait.
								callHandlerOutput = &ActivityOutput{Suspend: suspend}
								return
							}
							pc.responseCh.Send(ctx, teOutput.CallResponse)
							callHandlerOutput = &teOutput
							return
						}
						pc.responseCh.Send(ctx, err)
						// Re-use current suspend to re-establish the same wait.
						callHandlerOutput = &ActivityOutput{Suspend: suspend}
						return
					}

					pc.responseCh.Send(ctx, callOutput.CallResponse)
					callHandlerOutput = &callOutput
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
		// Skip CAN when a call handler output is pending — the next loop
		// iteration must consume it first (it may complete the workflow or
		// establish the next suspend state).
		shouldCAN := callHandlerOutput == nil && workflow.GetInfo(ctx).GetContinueAsNewSuggested()
		if callHandlerOutput == nil && input.MaxHistoryLength > 0 && workflow.GetInfo(ctx).GetCurrentHistoryLength() > int(input.MaxHistoryLength) {
			shouldCAN = true
		}
		if shouldCAN {
			for len(pendingCalls) > 0 {
				pc := pendingCalls[0]
				pendingCalls = pendingCalls[1:]

				callInput := ActivityInput{
					HandlerKey:     pc.handlerKey,
					StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
					State:          pc.state,
					Message:        pc.req,
				}
				callActivityCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions(pc.handlerKey))
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
				HandlerKey:       handlerKey,
				State:            state,
				QueryResults:     allQueryResults,
				MaxHistoryLength: input.MaxHistoryLength,
			})
		}
	}
}

// lookupActivityOptions builds workflow.ActivityOptions from the handler's
// RetryPolicy stored in the global registry.
func lookupActivityOptions(handlerKey string) workflow.ActivityOptions {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	if globalRegistry == nil {
		return opts
	}
	entry, ok := globalRegistry.entries[handlerKey]
	if !ok {
		return opts
	}
	rp := entry.options.RetryPolicy
	if rp.StartToCloseTimeout > 0 {
		opts.StartToCloseTimeout = rp.StartToCloseTimeout
	}
	if rp.ScheduleToCloseTimeout > 0 {
		opts.ScheduleToCloseTimeout = rp.ScheduleToCloseTimeout
	}
	if rp.MaxAttempts > 0 || rp.InitialInterval > 0 || rp.MaxInterval > 0 || rp.BackoffCoefficient > 0 {
		trp := &temporal.RetryPolicy{}
		if rp.MaxAttempts > 0 {
			trp.MaximumAttempts = int32(rp.MaxAttempts)
		}
		if rp.InitialInterval > 0 {
			trp.InitialInterval = rp.InitialInterval
		}
		if rp.MaxInterval > 0 {
			trp.MaximumInterval = rp.MaxInterval
		}
		if rp.BackoffCoefficient > 0 {
			trp.BackoffCoefficient = rp.BackoffCoefficient
		}
		opts.RetryPolicy = trp
	}
	return opts
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
	teKey := entry.options.WithTerminalErrorHandlerKey()
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
