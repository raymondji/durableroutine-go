package temporal

import (
	"encoding/json"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/raymondji/durableroutine-go/internal/durablecore"
)

type workflowHandler struct {
	reg *registry
}

// RoutineWorkflow is the single workflow function for all stateroutines.
func (wh *workflowHandler) RoutineWorkflow(ctx workflow.Context, input WorkflowInput) (any, error) {
	var ha *handlerActivity

	handlerKey := input.HandlerKey
	var handlerInput json.RawMessage = input.Input

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
		req        json.RawMessage
		input      json.RawMessage
		responseCh workflow.Channel
	}
	var pendingCalls []pendingCall

	callNotifyCh := workflow.NewChannel(ctx)

	// message carries the received send/call message into the next activity.
	var message json.RawMessage

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
				HandlerKey: handlerKey,
				RoutineID:  workflow.GetInfo(ctx).WorkflowExecution.ID,
				Input:      handlerInput,
				Message:    message,
			}
			message = nil // consumed

			activityCtx := workflow.WithActivityOptions(ctx, wh.lookupActivityOptions(handlerKey))

			err := workflow.ExecuteActivity(activityCtx, ha.RunHandler, activityInput).Get(ctx, &output)

			if err != nil {
				teKey := wh.lookupTerminalErrorKey(handlerKey)
				if teKey != "" {
					teInput := ActivityInput{
						HandlerKey: teKey,
						RoutineID:  activityInput.RoutineID,
						Input:      handlerInput,
						Message:    activityInput.Message,
						Error:      err.Error(),
					}
					teActivityCtx := workflow.WithActivityOptions(ctx, wh.lookupActivityOptions(teKey))
					err = workflow.ExecuteActivity(teActivityCtx, ha.RunHandler, teInput).Get(ctx, &output)
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

		// 3. Handle child starts.
		for _, start := range output.StartRequests {
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:        start.RoutineID,
				ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
			})
			childInput := WorkflowInput{
				HandlerKey: durablecore.HandlerKey(start.InputKind, start.ResultKind),
				Input:      start.Input,
			}
			workflow.ExecuteChildWorkflow(childCtx, wh.RoutineWorkflow, childInput)
		}

		// 4. Handle send requests — wait for each signal to be acknowledged.
		for _, sr := range output.SendRequests {
			signalName := durablecore.SendKey(sr.InputKind, sr.ExternalInputKind, sr.ResultKind)
			f := workflow.SignalExternalWorkflow(ctx, sr.RoutineID, "", signalName, sr.Msg)
			if err := f.Get(ctx, nil); err != nil {
				// Signal delivery failed (e.g., target workflow not found).
				// Log but continue — best-effort delivery.
				workflow.GetLogger(ctx).Warn("signal delivery failed",
					"target", sr.RoutineID, "signal", signalName, "error", err)
			}
		}

		// 5. Check if done.
		if output.Done {
			return output.Result, nil
		}

		// 6. Interpret Continuation cases.
		cont := output.Continuation
		if cont == nil || len(cont.Cases) == 0 {
			return nil, fmt.Errorf("handler %s returned no continuation cases and is not done", handlerKey)
		}

		// If the only case is immediate (Continue), skip the selector.
		if len(cont.Cases) == 1 && cont.Cases[0].Immediate {
			handlerKey = cont.Cases[0].HandlerKey
			handlerInput = cont.Cases[0].Input
			goto continueAsNewCheck
		}

		// Register Update handlers for ReceiveCall cases.
		for _, c := range cont.Cases {
			if c.CallName == "" {
				continue
			}
			c := c
			// Use HandlerKey as the update name — matches the client's "call:{stateKind}:{reqKind}:{respKind}:{resultKind}".
			workflow.SetUpdateHandler(ctx, c.HandlerKey,
				func(ctx workflow.Context, req json.RawMessage) (any, error) {
					responseCh := workflow.NewChannel(ctx)
					pendingCalls = append(pendingCalls, pendingCall{
						handlerKey: c.HandlerKey,
						req:        req,
						input:      c.Input,
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
			for _, c := range cont.Cases {
				if c.TimerDuration == nil {
					continue
				}
				c := c
				timer := workflow.NewTimer(ctx, *c.TimerDuration)
				sel.AddFuture(timer, func(f workflow.Future) {
					handlerKey = c.HandlerKey
					handlerInput = c.Input
				})
			}

			// Priority 2: Calls (via notification channel)
			hasCallCases := false
			for _, c := range cont.Cases {
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
						HandlerKey: pc.handlerKey,
						RoutineID:  workflow.GetInfo(ctx).WorkflowExecution.ID,
						Input:      pc.input,
						Message:    pc.req,
					}

					callActivityCtx := workflow.WithActivityOptions(ctx, wh.lookupActivityOptions(pc.handlerKey))

					var callOutput ActivityOutput
					err := workflow.ExecuteActivity(callActivityCtx, ha.RunHandler, callInput).Get(ctx, &callOutput)
					if err != nil {
						teKey := wh.lookupTerminalErrorKey(pc.handlerKey)
						if teKey != "" {
							teInput := ActivityInput{
								HandlerKey: teKey,
								RoutineID:  workflow.GetInfo(ctx).WorkflowExecution.ID,
								Input:      pc.input,
								Message:    pc.req,
								Error:      err.Error(),
							}
							teActivityCtx := workflow.WithActivityOptions(ctx, wh.lookupActivityOptions(teKey))
							var teOutput ActivityOutput
							teErr := workflow.ExecuteActivity(teActivityCtx, ha.RunHandler, teInput).Get(ctx, &teOutput)
							if teErr != nil {
								pc.responseCh.Send(ctx, teErr)
								// Re-use current continuation to re-establish the same wait.
								callHandlerOutput = &ActivityOutput{Continuation: cont}
								return
							}
							pc.responseCh.Send(ctx, teOutput.CallResponse)
							callHandlerOutput = &teOutput
							return
						}
						pc.responseCh.Send(ctx, err)
						// Re-use current continuation to re-establish the same wait.
						callHandlerOutput = &ActivityOutput{Continuation: cont}
						return
					}

					pc.responseCh.Send(ctx, callOutput.CallResponse)
					callHandlerOutput = &callOutput
				})
			}

			// Priority 3: Sends (via Temporal signal channels)
			for _, c := range cont.Cases {
				if c.SendName == "" {
					continue
				}
				c := c
				// The composite signal name matches what the client sends.
				compositeName := "send:" + c.HandlerKey[len("send:"):]
				signalCh := workflow.GetSignalChannel(ctx, compositeName)
				sel.AddReceive(signalCh, func(ch workflow.ReceiveChannel, more bool) {
					var msg json.RawMessage
					ch.Receive(ctx, &msg)
					handlerKey = c.HandlerKey
					handlerInput = c.Input
					message = msg
				})
			}

			// Priority 4: Default
			for _, c := range cont.Cases {
				if !c.Immediate {
					continue
				}
				c := c
				sel.AddDefault(func() {
					handlerKey = c.HandlerKey
					handlerInput = c.Input
				})
			}

			sel.Select(ctx)
		}

	continueAsNewCheck:
		// Skip CAN when a call handler output is pending — the next loop
		// iteration must consume it first (it may complete the workflow or
		// establish the next continuation state).
		shouldCAN := callHandlerOutput == nil && workflow.GetInfo(ctx).GetContinueAsNewSuggested()
		if callHandlerOutput == nil && input.MaxHistoryLength > 0 && workflow.GetInfo(ctx).GetCurrentHistoryLength() > int(input.MaxHistoryLength) {
			shouldCAN = true
		}
		if shouldCAN {
			for len(pendingCalls) > 0 {
				pc := pendingCalls[0]
				pendingCalls = pendingCalls[1:]

				callInput := ActivityInput{
					HandlerKey: pc.handlerKey,
					RoutineID:  workflow.GetInfo(ctx).WorkflowExecution.ID,
					Input:      pc.input,
					Message:    pc.req,
				}
				callActivityCtx := workflow.WithActivityOptions(ctx, wh.lookupActivityOptions(pc.handlerKey))
				var callOutput ActivityOutput
				err := workflow.ExecuteActivity(callActivityCtx, ha.RunHandler, callInput).Get(ctx, &callOutput)
				if err != nil {
					pc.responseCh.Send(ctx, err)
					continue
				}
				pc.responseCh.Send(ctx, callOutput.CallResponse)

				if callOutput.Continuation != nil && len(callOutput.Continuation.Cases) > 0 {
					handlerKey = callOutput.Continuation.Cases[0].HandlerKey
					handlerInput = callOutput.Continuation.Cases[0].Input
				}
			}

			return nil, workflow.NewContinueAsNewError(ctx, wh.RoutineWorkflow, WorkflowInput{
				HandlerKey:       handlerKey,
				Input:            handlerInput,
				QueryResults:     allQueryResults,
				MaxHistoryLength: input.MaxHistoryLength,
			})
		}
	}
}

// lookupActivityOptions builds workflow.ActivityOptions from the handler's
// options stored in the registry.
func (wh *workflowHandler) lookupActivityOptions(handlerKey string) workflow.ActivityOptions {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	entry, ok := wh.reg.entries[handlerKey]
	if !ok {
		return opts
	}
	if entry.options.StartToCloseTimeout > 0 {
		opts.StartToCloseTimeout = entry.options.StartToCloseTimeout
	}
	if entry.options.ScheduleToCloseTimeout > 0 {
		opts.ScheduleToCloseTimeout = entry.options.ScheduleToCloseTimeout
	}
	rp := entry.options.RetryPolicy
	if rp.MaxAttempts > 0 || rp.InitialInterval > 0 || rp.MaxInterval > 0 || rp.BackoffCoefficient > 0 {
		trp := &temporalsdk.RetryPolicy{}
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
func (wh *workflowHandler) lookupTerminalErrorKey(handlerKey string) string {
	entry, ok := wh.reg.entries[handlerKey]
	if !ok {
		return ""
	}
	teKey := entry.options.WithTerminalErrorHandlerKey()
	if teKey == "" {
		return ""
	}
	if _, ok := wh.reg.entries[teKey]; ok {
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
