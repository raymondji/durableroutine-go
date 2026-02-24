# Design Doc: Temporal Implementation

This document describes how the stateroutine API maps to Temporal primitives. The implementation lives in a separate `backend/temporal/` package, keeping the core `stateroutine/` package free of Temporal dependencies.

## Package Structure

```
backend/temporal/
    client.go       — implements durable.Client
    worker.go       — wraps Temporal worker, registers workflows + activities
    workflow.go     — StateroutineWorkflow: the library-owned workflow loop
    activities.go   — activity wrappers for user handlers
```

## Client

The `temporal.Client` implements `durable.Client` by mapping each method to a Temporal SDK call:

| durable.Client method | Temporal SDK call |
|---|---|
| `start(ctx, id, kind, state)` | `client.ExecuteWorkflow(ctx, options, StateroutineWorkflow, input)` |
| `send(ctx, id, stateKind, msgKind, msg)` | `client.SignalWorkflow(ctx, id, "", signalName, msg)` where `signalName = "send:" + stateKind + ":" + msgKind` |
| `call(ctx, id, stateKind, reqKind, req)` | `client.UpdateWorkflow(ctx, id, "", updateName, req)` where `updateName = "call:" + stateKind + ":" + reqKind` |
| `query(ctx, id, queryName)` | `client.QueryWorkflow(ctx, id, "", queryName)` |
| `get(ctx, id)` | `client.GetWorkflow(ctx, id, "").Get(ctx, &result)` |

The `start` method constructs `WorkflowOptions` with:
- `ID`: the stateroutine ID
- `TaskQueue`: from the worker configuration
- `WorkflowIDReusePolicy`: allow duplicate if terminated/completed

## WorkflowInput

```go
type WorkflowInput struct {
    HandlerKey   string         // e.g. "handler:booking"
    State        any            // serialized handler state
    QueryResults []QueryEntry   // preserved across continue-as-new
    SignalBuffer map[string][]any // self-managed signal buffer, keyed by composite name
    PendingCalls []PendingCall  // calls received but not yet processed
}
```

On continue-as-new, all fields are carried forward so the new execution resumes without data loss.

## Workflow Loop

`StateroutineWorkflow` is the single workflow function registered for all stateroutines. It is entirely library-owned — user code never runs inside it.

### Key design decisions

**Priority order**: Timers > Calls > Sends > Default. Timers represent deadlines and cancellation — if a timer fires at the same moment a message arrives, the timer wins. Calls are processed before sends because the caller is blocking.

**Self-managed signal buffer**: Instead of relying on Temporal's signal channel naming, we buffer incoming signals in a local `map[string][]any` keyed by composite signal name (`"send:" + stateKind + ":" + msgKind`). This eliminates naming mismatches and makes continue-as-new behavior explicit.

**Queue+Await for ReceiveCall**: Temporal Updates don't participate in Selectors. Update handlers fire independently — registering `SetUpdateHandler` inside a Selector case creates a race with timers/signals. Instead, Update handlers enqueue requests into a local queue and block via `workflow.Await`. The main Selector receives a notification and processes the call in the main loop.

### Pseudocode

```go
func StateroutineWorkflow(ctx workflow.Context, input WorkflowInput) (any, error) {
    handlerKey := input.HandlerKey
    state := input.State
    signalBuffer := input.SignalBuffer // map[string][]any
    if signalBuffer == nil {
        signalBuffer = make(map[string][]any)
    }

    // Restore query results from previous execution (continue-as-new)
    for _, qr := range input.QueryResults {
        name := qr.QueryName
        result := qr.Result
        workflow.SetQueryHandler(ctx, name, func() (any, error) {
            return result, nil
        })
    }

    // Pending calls queue: Update handlers enqueue here, main loop dequeues
    type pendingCall struct {
        handlerKey string
        req        any
        responseCh workflow.Channel // Update handler blocks reading from this
    }
    var pendingCalls []pendingCall

    // Restore pending calls from previous execution (continue-as-new)
    for _, pc := range input.PendingCalls {
        ch := workflow.NewChannel(ctx)
        pendingCalls = append(pendingCalls, pendingCall{
            handlerKey: pc.HandlerKey,
            req:        pc.Req,
            responseCh: ch,
        })
        // Note: the original Update handler is gone after continue-as-new,
        // so these calls will be processed but the caller will see a
        // "workflow continued as new" error from the Temporal SDK.
    }

    // Notification channel: Update handlers signal that a call arrived
    callNotifyCh := workflow.NewChannel(ctx)

    // Register a catch-all signal handler that buffers into signalBuffer.
    // Temporal delivers all signals to this handler regardless of name.
    workflow.SetSignalHandler(ctx, func(signalName string, msg any) {
        signalBuffer[signalName] = append(signalBuffer[signalName], msg)
    })

    // Track all registered query results for continue-as-new
    allQueryResults := append([]QueryEntry{}, input.QueryResults...)

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
        allQueryResults = mergeQueryResults(allQueryResults, output.QueryResults)

        // 3. Handle child starts (with ABANDON policy)
        for _, start := range output.StartRequests {
            childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
                ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
            })
            childInput := WorkflowInput{
                HandlerKey: "handler:" + start.StateKind,
                State:      start.State,
            }
            workflow.ExecuteChildWorkflow(childCtx, StateroutineWorkflow, childInput)
        }

        // 4. Handle send requests
        for _, sr := range output.SendRequests {
            signalName := "send:" + sr.StateKind + ":" + sr.MsgKind
            workflow.SignalExternalWorkflow(ctx, sr.StateroutineID, "", signalName, sr.Msg)
        }

        // 5. Check if done
        if output.Done {
            return output.Result, nil
        }

        // 6. Interpret Continuation cases
        cont := output.Continuation

        // If the only case is immediate (Continue), skip the selector
        if len(cont.Cases) == 1 && cont.Cases[0].Immediate {
            handlerKey = cont.Cases[0].HandlerKey
            state = cont.Cases[0].State
            goto continueAsNewCheck
        }

        // Register Update handlers for ReceiveCall cases.
        // Each handler enqueues the request and blocks until the main loop
        // processes it and writes a response.
        for _, c := range cont.Cases {
            if c.CallName == "" {
                continue
            }
            c := c // capture for closure
            workflow.SetUpdateHandlerWithOptions(ctx, c.CallName,
                // Handler: enqueue and block until response
                func(ctx workflow.Context, req any) (any, error) {
                    responseCh := workflow.NewChannel(ctx)
                    pendingCalls = append(pendingCalls, pendingCall{
                        handlerKey: c.HandlerKey,
                        req:        req,
                        responseCh: responseCh,
                    })
                    callNotifyCh.SendAsync(true)

                    // Block until main loop writes the response
                    var resp any
                    responseCh.Receive(ctx, &resp)
                    // If resp is an error wrapper, return it as error
                    if errResp, ok := resp.(error); ok {
                        return nil, errResp
                    }
                    return resp, nil
                },
                // Validator: reject calls not in the current suspension's ReceiveCall cases
                workflow.UpdateHandlerOptions{
                    Validator: func(ctx workflow.Context, req any) error {
                        // Only accept if this update name is in the current cases
                        return nil // validated by registration — only registered names are accepted
                    },
                },
            )
        }

        // Build a selector with priority: timers > calls > sends > default
        //
        // Temporal's Selector fires the first ready callback in registration order.
        // By registering timers first, then the call notification channel, then
        // signal checks, then default, we enforce the priority.
        sel := workflow.NewSelector(ctx)

        // Priority 1: Timers
        for _, c := range cont.Cases {
            if c.TimerDuration == nil {
                continue
            }
            c := c
            sel.AddFuture(workflow.NewTimer(ctx, *c.TimerDuration), func(f workflow.Future) {
                handlerKey = c.HandlerKey
                state = c.State
            })
        }

        // Priority 2: Calls (via notification channel)
        sel.AddReceive(callNotifyCh, func(ch workflow.ReceiveChannel, more bool) {
            var discard any
            ch.Receive(ctx, &discard)

            // Dequeue and process the first pending call
            pc := pendingCalls[0]
            pendingCalls = pendingCalls[1:]

            callInput := ActivityInput{
                HandlerKey:     pc.handlerKey,
                StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
                State:          state,
                Message:        pc.req,
            }
            var callOutput ActivityOutput
            err := workflow.ExecuteActivity(ctx, RunHandler, callInput).Get(ctx, &callOutput)
            if err != nil {
                pc.responseCh.Send(ctx, err)
                // Don't update handlerKey/state — re-enter loop with same cases
                return
            }

            // Send response back to the Update handler
            pc.responseCh.Send(ctx, callOutput.CallResponse)

            // Transition to the call handler's returned Continuation
            handlerKey = callOutput.Continuation.Cases[0].HandlerKey
            state = callOutput.Continuation.Cases[0].State
        })

        // Priority 3: Sends (check self-managed signal buffer)
        for _, c := range cont.Cases {
            if c.SendName == "" {
                continue
            }
            c := c
            compositeName := c.SendName // already "send:" + stateKind + ":" + msgKind from HandlerKey
            // Check if a signal is already buffered
            if msgs, ok := signalBuffer[compositeName]; ok && len(msgs) > 0 {
                sel.AddDefault(func() {
                    msg := msgs[0]
                    signalBuffer[compositeName] = msgs[1:]
                    handlerKey = c.HandlerKey
                    state = c.State
                    _ = msg // msg is bound into next handler's activity input
                })
            } else {
                // Wait for a signal to arrive (Await on buffer having an entry)
                waitCh := workflow.NewChannel(ctx)
                workflow.Go(ctx, func(gCtx workflow.Context) {
                    workflow.Await(gCtx, func() bool {
                        msgs, ok := signalBuffer[compositeName]
                        return ok && len(msgs) > 0
                    })
                    waitCh.Send(gCtx, true)
                })
                sel.AddReceive(waitCh, func(ch workflow.ReceiveChannel, more bool) {
                    var discard any
                    ch.Receive(ctx, &discard)
                    msg := signalBuffer[compositeName][0]
                    signalBuffer[compositeName] = signalBuffer[compositeName][1:]
                    handlerKey = c.HandlerKey
                    state = c.State
                    _ = msg
                })
            }
        }

        // Priority 4: Default (immediate/Continue case)
        for _, c := range cont.Cases {
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

    continueAsNewCheck:
        // 7. Check continue-as-new
        if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
            // Drain all pending calls before continuing as new.
            // Temporal blocks continue-as-new while Update handlers are awaiting.
            for len(pendingCalls) > 0 {
                pc := pendingCalls[0]
                pendingCalls = pendingCalls[1:]

                callInput := ActivityInput{
                    HandlerKey:     pc.handlerKey,
                    StateroutineID: workflow.GetInfo(ctx).WorkflowExecution.ID,
                    State:          state,
                    Message:        pc.req,
                }
                var callOutput ActivityOutput
                err := workflow.ExecuteActivity(ctx, RunHandler, callInput).Get(ctx, &callOutput)
                if err != nil {
                    pc.responseCh.Send(ctx, err)
                    continue
                }
                pc.responseCh.Send(ctx, callOutput.CallResponse)

                handlerKey = callOutput.Continuation.Cases[0].HandlerKey
                state = callOutput.Continuation.Cases[0].State
            }

            return nil, workflow.NewContinueAsNewError(ctx, StateroutineWorkflow, WorkflowInput{
                HandlerKey:   handlerKey,
                State:        state,
                QueryResults: allQueryResults,
                SignalBuffer: signalBuffer,
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
    Continuation  SerializedContinuation  // always set for successful CallFunc handlers
    QueryResults  []QueryEntry
    StartRequests []StartEntry
    SendRequests  []SendEntry
    CallResponse  any // for CallFunc handlers
}
```

The `RunHandler` activity:
1. Looks up the handler function by key from the worker's handler registry
2. Uses the registry's type information to deserialize `State` and `Message` into their concrete Go types (the registry stores `reflect.Type` for each handler's state and message types)
3. Constructs a `durable.Context` with the stateroutine ID
4. Calls the handler with the deserialized state (and message, if applicable)
5. **Validates**: For `CallFunc` handlers, if the handler returned `err == nil` but `Continuation == nil`, the activity returns an error — handlers must always return a `Continuation` when there is no error. Returning `(resp, nil, err)` with a non-nil error is valid because the error triggers retry/terminal-error-handler logic.
6. Captures the `Continuation`, query results, start requests, and send requests from the context
7. Returns `ActivityOutput`

Activity options (retry policy, timeouts) are configured from the `HandlerOptions` registered with each handler.

## Worker

The `temporal.Worker` wraps a Temporal worker:

```go
type Worker struct {
    inner      worker.Worker
    taskQueue  string
    handlers   map[string]handlerEntry // from durable.Worker
}

func New(c client.Client, w *durable.Worker) *Worker {
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

The `durable.Worker` must expose:
- `TaskQueue() string` — returns the task queue name
- `Handlers() map[string]handlerEntry` — returns the handler registry

These are currently unexported fields. The Temporal implementation package needs read access to construct the Temporal worker and register activities.

## Serialization

All state, messages, and results use JSON serialization via Temporal's default data converter (`converter.GetDefaultDataConverter()`). This works out of the box for Go structs with exported fields.

The `Kind()` string is used as the routing key, not the Go type name, so serialization is stable across refactors as long as `Kind()` values are preserved.

The handler registry stores `reflect.Type` for each handler's state and message types. When the `RunHandler` activity receives an `ActivityInput`, it looks up the handler by key, allocates a new instance of the concrete type via reflection, and deserializes into it. This avoids the problem of deserializing into `any` (which would produce `map[string]interface{}` instead of the expected struct).

## Continue-As-New

Continue-as-new must preserve all workflow-local state:

1. After each iteration, check `workflow.GetInfo(ctx).GetCurrentHistoryLength()`
2. When the history exceeds the threshold (e.g., 10,000 events), **drain all pending calls first** — Temporal blocks continue-as-new while Update handlers are in an awaiting state
3. Serialize into a new `WorkflowInput` carrying:
   - `HandlerKey` and `State` — current position in the state machine
   - `QueryResults` — so queries keep working on the new execution
   - `SignalBuffer` — unprocessed signals that arrived but haven't been consumed
4. Call `workflow.NewContinueAsNewError(ctx, StateroutineWorkflow, newInput)`
5. On the new execution, restore query handlers, signal buffer, and pending calls from `WorkflowInput` before entering the loop

The new execution resumes the loop with the same handler key and state, as if nothing happened. This is invisible to user code.

## Child Workflow Lifecycle

Child workflows are started with `ParentClosePolicy: ABANDON`. This ensures children survive when the parent triggers continue-as-new (which Temporal treats as a close+reopen). Without this, children would be terminated on every continue-as-new.

```go
childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
    ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
})
```

## Signal Buffering

Instead of relying on Temporal's per-name signal channels, the workflow manages its own signal buffer:

1. A catch-all signal handler (`workflow.SetSignalHandler`) writes every incoming signal into a local `map[string][]any`, keyed by the composite signal name (`"send:" + stateKind + ":" + msgKind`)
2. When the Selector needs to check for a send case, it reads from the buffer
3. If a signal is already buffered, it's consumed immediately (AddDefault)
4. If not, a goroutine Awaits on the buffer entry appearing and notifies the Selector via a channel

This approach:
- Eliminates signal name mismatches — the buffer key and the client's signal name both use the same composite format
- Makes continue-as-new explicit — the buffer is carried in `WorkflowInput`
- Gives the workflow full control over signal consumption order

For `ClientSend`, signals arrive via `client.SignalWorkflow`. For `Send` (stateroutine-to-stateroutine), the runtime calls `workflow.SignalExternalWorkflow` during the workflow loop after the handler activity completes.

## Error Handling

- **Handler errors**: Activities are retried according to the `RetryPolicy` in `HandlerOptions`. After all retries are exhausted, the workflow checks for a terminal error handler registered under `"error:" + handlerKey`.
- **Terminal error handlers**: Run as a separate activity. If the terminal error handler also fails, the workflow fails.
- **Workflow errors**: If no terminal error handler is registered and retries are exhausted, the workflow fails with the activity error.
- **Call handler errors**: If a call handler activity fails, the error is sent back to the Update handler's response channel. The Update handler returns the error to the client. The workflow does not transition state — it re-enters the select loop with the same cases. If retries are exhausted and a terminal error handler is registered, it runs and must return a valid `Continuation` (along with the error response to the caller).
