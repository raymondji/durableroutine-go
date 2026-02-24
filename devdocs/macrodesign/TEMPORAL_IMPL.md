# Design Doc: Temporal Implementation

This document describes how the stateroutine API maps to Temporal primitives. The implementation lives in a separate `temporalimpl/` package, keeping the core `stateroutine/` package free of Temporal dependencies.

## Package Structure

```
temporalimpl/
    client.go       — implements stateroutine.Client
    worker.go       — wraps Temporal worker, registers workflows + activities
    workflow.go     — StateroutineWorkflow: the library-owned workflow loop
    activities.go   — activity wrappers for user handlers
```

## Client

The `temporalimpl.Client` implements `stateroutine.Client` by mapping each method to a Temporal SDK call:

| stateroutine.Client method | Temporal SDK call |
|---|---|
| `start(ctx, id, kind, state)` | `client.ExecuteWorkflow(ctx, options, StateroutineWorkflow, input)` |
| `send(ctx, id, stateKind, msgKind, msg)` | `client.SignalWorkflow(ctx, id, "", signalName, msg)` where `signalName = "send:" + stateKind + ":" + msgKind` |
| `call(ctx, id, methodName, req)` | `client.UpdateWorkflow(ctx, id, "", updateName, req)` where `updateName = "call:" + stateKind + ":" + reqKind` |
| `query(ctx, id, queryName)` | `client.QueryWorkflow(ctx, id, "", queryName)` |
| `get(ctx, id)` | `client.GetWorkflow(ctx, id, "").Get(ctx, &result)` |

The `start` method constructs `WorkflowOptions` with:
- `ID`: the stateroutine ID
- `TaskQueue`: from the worker configuration
- `WorkflowIDReusePolicy`: allow duplicate if terminated/completed

## Workflow Loop

`StateroutineWorkflow` is the single workflow function registered for all stateroutines. It is entirely library-owned — user code never runs inside it.

```go
func StateroutineWorkflow(ctx workflow.Context, input WorkflowInput) (any, error) {
    handlerKey := input.HandlerKey  // e.g. "handler:booking"
    state := input.State            // serialized handler state

    for {
        // 1. Run the handler as an activity
        activityInput := ActivityInput{
            HandlerKey:     handlerKey,
            StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
            State:          state,
        }
        var output ActivityOutput
        err := workflow.ExecuteActivity(ctx, RunHandler, activityInput).Get(ctx, &output)

        if err != nil {
            // Check for terminal error handler
            if teKey := lookupTerminalErrorKey(handlerKey); teKey != "" {
                teInput := ActivityInput{
                    HandlerKey:     teKey,
                    StateroutineID: activityInput.StateroutineID,
                    State:          state,
                    Error:          err.Error(),
                }
                err = workflow.ExecuteActivity(ctx, RunHandler, teInput).Get(ctx, &output)
                if err != nil {
                    return nil, err
                }
            } else {
                return nil, err
            }
        }

        // 2. Apply query results (persist across state transitions)
        for _, qr := range output.QueryResults {
            name := qr.QueryName
            result := qr.Result
            workflow.SetQueryHandler(ctx, name, func() (any, error) {
                return result, nil
            })
        }

        // 3. Handle child spawns
        for _, spawn := range output.SpawnRequests {
            childInput := WorkflowInput{
                HandlerKey: "handler:" + spawn.StateKind,
                State:      spawn.State,
            }
            workflow.ExecuteChildWorkflow(ctx, StateroutineWorkflow, childInput)
        }

        // 4. Handle send requests (buffered by Send() during handler execution)
        for _, sr := range output.SendRequests {
            signalName := "send:" + sr.StateKind + ":" + sr.MsgKind
            workflow.SignalExternalWorkflow(ctx, sr.StateroutineID, "", signalName, sr.Msg)
        }

        // 5. Check if done
        if output.Done {
            return output.Result, nil
        }

        // 6. Interpret Suspend cases
        suspend := output.Suspend

        // If the only case is immediate (Continue), skip the selector
        if len(suspend.Cases) == 1 && suspend.Cases[0].Immediate {
            handlerKey = suspend.Cases[0].HandlerKey
            state = suspend.Cases[0].State
            goto continueAsNewCheck
        }

        // Build a selector
        sel := workflow.NewSelector(ctx)

        for _, c := range suspend.Cases {
            c := c // capture for closure
            if c.Immediate {
                // Default case
                sel.AddDefault(func() {
                    handlerKey = c.HandlerKey
                    state = c.State
                })
            } else if c.TimerDuration != nil {
                // Timer case
                sel.AddFuture(workflow.NewTimer(ctx, *c.TimerDuration), func(f workflow.Future) {
                    handlerKey = c.HandlerKey
                    state = c.State
                })
            } else if c.SendName != "" {
                // OnSend case — listen for a signal
                signalName := c.SendName
                ch := workflow.GetSignalChannel(ctx, signalName)
                sel.AddReceive(ch, func(ch workflow.ReceiveChannel, more bool) {
                    var msg any
                    ch.Receive(ctx, &msg)
                    handlerKey = c.HandlerKey
                    state = c.State
                    // msg is bound into the activity input for the next handler
                })
            } else if c.CallName != "" {
                // OnCall case — register an update handler
                updateName := c.CallName
                workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context, req any) (any, error) {
                    callInput := ActivityInput{
                        HandlerKey:     c.HandlerKey,
                        StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
                        State:          c.State,
                        Message:        req,
                    }
                    var callOutput ActivityOutput
                    err := workflow.ExecuteActivity(ctx, RunHandler, callInput).Get(ctx, &callOutput)
                    if err != nil {
                        return nil, err
                    }
                    // The call handler's suspend becomes the new state
                    handlerKey = callOutput.Suspend.Cases[0].HandlerKey
                    state = callOutput.Suspend.Cases[0].State
                    return callOutput.CallResponse, nil
                })
            }
        }

        sel.Select(ctx)

    continueAsNewCheck:
        // 7. Check continue-as-new
        if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
            return nil, workflow.NewContinueAsNewError(ctx, StateroutineWorkflow, WorkflowInput{
                HandlerKey: handlerKey,
                State:      state,
            })
        }
    }
}
```

## Activity Model

Each user handler runs as a Temporal activity. This is the key design choice that eliminates replay-safety constraints — activities can use `time.Now()`, make network calls, use goroutines, etc.

```go
type ActivityInput struct {
    HandlerKey     string // e.g. "handler:booking", "send:booking.reserved:payment"
    StateroutineID string
    State          any    // serialized handler state
    Message        any    // for SendFunc/CallFunc handlers (nil for HandlerFunc)
    Error          string // for terminal error handlers (empty otherwise)
}

type ActivityOutput struct {
    Done          bool
    Result        any
    Suspend       SerializedSuspend
    QueryResults  []QueryEntry
    SpawnRequests []SpawnEntry
    SendRequests  []SendEntry
    CallResponse  any // for CallFunc handlers
}
```

The `RunHandler` activity:
1. Looks up the handler function by key from the worker's handler registry
2. Constructs a `stateroutine.Context` with the stateroutine ID
3. Calls the handler with the deserialized state (and message, if applicable)
4. Captures the `Suspend`, query results, spawn requests, and send requests from the context
5. Returns `ActivityOutput`

Activity options (retry policy, timeouts) are configured from the `HandlerOptions` registered with each handler.

## Worker

The `temporalimpl.Worker` wraps a Temporal worker:

```go
type Worker struct {
    inner      worker.Worker
    taskQueue  string
    handlers   map[string]handlerEntry // from stateroutine.Worker
}

func New(c client.Client, w *stateroutine.Worker) *Worker {
    // Create Temporal worker
    tw := worker.New(c, w.TaskQueue(), worker.Options{})

    // Register the single workflow
    tw.RegisterWorkflow(StateroutineWorkflow)

    // Register one activity per handler key
    for key, entry := range w.Handlers() {
        tw.RegisterActivityWithOptions(
            makeActivity(key, entry),
            activity.RegisterOptions{Name: key},
        )
    }

    return &Worker{inner: tw, taskQueue: w.TaskQueue(), handlers: w.Handlers()}
}
```

### Exposed Getters Needed

The `stateroutine.Worker` must expose:
- `TaskQueue() string` — returns the task queue name
- `Handlers() map[string]handlerEntry` — returns the handler registry

These are currently unexported fields. The Temporal implementation package needs read access to construct the Temporal worker and register activities.

## Serialization

All state, messages, and results use JSON serialization via Temporal's default data converter (`converter.GetDefaultDataConverter()`). This works out of the box for Go structs with exported fields.

The `Kind()` string is used as the routing key, not the Go type name, so serialization is stable across refactors as long as `Kind()` values are preserved.

## Continue-As-New

Continue-as-new is straightforward because the workflow loop carries minimal state:

1. After each iteration, check `workflow.GetInfo(ctx).GetCurrentHistoryLength()`
2. When the history exceeds the threshold (e.g., 10,000 events), trigger continue-as-new
3. Serialize the current `handlerKey` and `state` into a new `WorkflowInput`
4. Call `workflow.NewContinueAsNewError(ctx, StateroutineWorkflow, newInput)`
5. Query results are re-registered from the activity output on the next iteration

The new execution resumes the loop with the same handler key and state, as if nothing happened. This is invisible to user code.

## Signal Buffering

Temporal automatically buffers signals. When a stateroutine is suspended waiting for an `OnSend` case, signals that arrive for that inbox name are queued by Temporal. When the selector fires, the signal is consumed from the buffer.

For `ClientSend`, signals arrive via `client.SignalWorkflow`. For `Send` (stateroutine-to-stateroutine), the runtime calls `workflow.SignalExternalWorkflow` during the workflow loop after the handler activity completes.

## Error Handling

- **Handler errors**: Activities are retried according to the `RetryPolicy` in `HandlerOptions`. After all retries are exhausted, the workflow checks for a terminal error handler registered under `"error:" + handlerKey`.
- **Terminal error handlers**: Run as a separate activity. If the terminal error handler also fails, the workflow fails.
- **Workflow errors**: If no terminal error handler is registered and retries are exhausted, the workflow fails with the activity error.
