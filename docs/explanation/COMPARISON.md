# Comparison: Durable Routines vs Temporal vs iWF

Three frameworks for durable execution, three different programming models. **Temporal** provides a general-purpose replay-based workflow engine with language-specific SDKs. **iWF** (Indeed Workflow Framework) adds a two-phase state model on top of Temporal, simplifying common patterns while running as a separate server that proxies to Temporal underneath. **Durable Routines** is a Go library that compiles directly into your worker binary — no separate server, no replay, no determinism constraints — using typed handler functions and explicit continuations to model state machines.

This doc compares the three frameworks pattern-by-pattern, using code from the actual Durable Routines codebase alongside examples from [Long Quanzheng's iWF overview for Temporal users](https://medium.com/@qlong/iwf-overview-for-temporal-users-part1-programming-model-difference-9f58e4793cfa).

## Concept mapping

| Concept | Temporal | iWF | Durable Routines |
|---|---|---|---|
| Unit of work | Workflow | Workflow (WorkflowStates) | Routine (handler chain) |
| Step / phase | Activity | WorkflowState (waitUntil + execute) | Handler function returning a Continuation |
| Fire-and-forget message | Signal | SignalChannel | `Send` / `ClientSend` |
| Request-response message | Update | RPC | `Call` / `ClientCall` |
| Read-only query | Query | DataAttributes / RPC | `Query` / `ClientQuery` |
| Timer / sleep | `workflow.Sleep` / `workflow.NewTimer` | `TimerCommand` | `durable.After` |
| Wait for first of N | `workflow.Select` (Go) / `Workflow.await` (Java) | `CommandRequest.forAnyOf` | `durable.Select` |
| Failure compensation | try/catch + manual rollback | `ProceedToStateWhenExecuteRetryExhausted` | Recovery handlers |
| Parallel execution | `workflow.Go` / `Async.function` | `multiNextStates` + InternalChannel | Child routines via `BufferStart` |
| Shared mutable state | Workflow variables (replay-safe) | Persistence (DataAttributes) | Handler input structs (explicit per-step state) |
| Determinism requirement | Yes (strict) | No (handled by interpreter) | No (no replay) |

## Pattern comparisons

### 1. Resettable timers

Implementing a durable timer that can be reset by an external event is notoriously difficult in Temporal because of its replay model. The naive approach doesn't work — you need a loop with a condition variable.

**Temporal** (the naive version is wrong; this is the correct one):

```java
// ~25 lines of non-obvious control flow
public class UpdatableTimerWorkflowImpl implements UpdatableTimerWorkflow {
    private boolean operationalSignal;
    private boolean shouldStayOpen = true;

    @Override
    public void start() {
        while (shouldStayOpen) {
            boolean timerFired = !Workflow.await(
                Duration.ofDays(7),
                () -> operationalSignal
            );
            if (!timerFired) {
                operationalSignal = false;  // reset and loop
            } else {
                shouldStayOpen = false;
            }
        }
        activities.sendEmail();
    }

    @Override
    public void updateTimer() {
        operationalSignal = true;
    }
}
```

**iWF** — the two-phase model makes this cleaner:

```java
class UpdatableTimerState implements WorkflowState<Void> {
    @Override
    public CommandRequest waitUntil(Context context, ...) {
        return CommandRequest.forAnyCommandCompleted(
            TimerCommand.createByDuration(TIMEOUT_DURATION),
            InternalChannelCommand.create(CHANNEL_RESET_TIMER)
        );
    }

    @Override
    public StateDecision execute(Context context, Void input, CommandResults commandResults, ...) {
        if (commandResults.getTimerResult(0).getStatus() == FIRED) {
            svc.sendEmail(...);
            return StateDecision.completeWorkflow();
        }
        // Channel received — reset by re-entering this state
        return StateDecision.singleNextState(UpdatableTimerState.class);
    }
}
```

**Durable Routines** — the auction example shows the same pattern. Each bid resets the deadline by returning a new `Select` with a fresh `After`:

```go
// From docs/howto/auction/auction.go
func (s *AuctionService) PlaceBid(ctx *durable.Context, input BiddingInput, req PlaceBidReq) (PlaceBidResp, *durable.Continuation[AuctionResult], error) {
    // ... validate and update bid ...

    // Timer resets naturally: returning a new Select creates a fresh timer
    return PlaceBidResp{Accepted: true, ...},
        durable.Select(
            durable.ReceiveCall(s.PlaceBid, input),
            durable.After(time.Until(input.Deadline), s.CloseAuction, input),
        ), nil
}
```

The booking example uses the same pattern — a reservation timeout that races against payment:

```go
// From docs/howto/booking/booking.go
func (s *BookingService) ReserveItem(ctx *durable.Context, input BookingInput) (*durable.Continuation[BookingResult], error) {
    reserved := ReservedInput{UserID: input.UserID, ItemID: input.ItemID}

    return durable.Select(
        durable.ReceiveSend(s.ProcessPayment, reserved),
        durable.ReceiveCall(s.CancelBooking, reserved),
        durable.After(15*time.Minute, s.ExpireReservation, reserved),
    ), nil
}
```

No boolean flags, no while loops, no condition variables. Each handler returns the next set of things to wait for.

### 2. Polling

**Temporal:**

```java
@Override
public void execute(MyWorkflowInput input) {
    boolean conditionMet = false;
    while (!conditionMet) {
        Workflow.sleep(Duration.ofSeconds(30));
        conditionMet = activities.checkCondition(input.getResourceId());
    }
}
```

**iWF:**

```java
public class PollingState implements WorkflowState<Input> {
    @Override
    public CommandRequest waitUntil(Context context, Input input, ...) {
        return CommandRequest.forTimer(TimerCommand.createByDuration(30));
    }

    @Override
    public StateDecision execute(Context context, Input input, ...) {
        if (checkExternalService()) {
            return StateDecision.singleNextState(StateMovement.create(COMPLETION_STATE));
        }
        return StateDecision.singleNextState(StateMovement.create(PollingState.class));
    }
}
```

**Durable Routines** — polling is a handler that continues back to itself:

```go
func (s *Svc) Poll(ctx *durable.Context, input PollInput) (*durable.Continuation[Result], error) {
    if done, err := checkExternalService(input.ResourceID); done {
        return durable.Done(Result{Status: "ready"}), nil
    }
    // Continue back to self after 30s — same as iWF's self-transition
    return durable.After(30*time.Second, s.Poll, input), nil
}
```

All three are similar here. Temporal requires a deterministic while loop. iWF and Durable Routines both use self-transitions.

### 3. Interactive workflows (signals / queries / updates)

Temporal splits external interaction across three separate APIs:
- `@SignalMethod` — fire-and-forget
- `@QueryMethod` — read-only
- `@UpdateMethod` — synchronous request-response

iWF unifies these into `@RPC`, which can read/write persistence and publish to internal channels.

Durable Routines provides three typed handler categories that each map cleanly:

| Interaction | Temporal | iWF | Durable Routines |
|---|---|---|---|
| Fire-and-forget | `@SignalMethod` | `SignalChannel` | `Send` handler (`ReceiveSend`) |
| Request-response | `@UpdateMethod` | `@RPC` | `Call` handler (`ReceiveCall`) |
| Read-only | `@QueryMethod` | `@RPC` / DataAttributes | `Query` (`SetQueryResult`) |

**Durable Routines** — all three in one workflow:

```go
// From docs/howto/booking/booking.go

// Send handler: fire-and-forget, advances state machine
func (s *BookingService) ProcessPayment(ctx *durable.Context, input ReservedInput, payment PaymentInfo) (*durable.Continuation[BookingResult], error) {
    fmt.Printf("charging card ending in %s\n", payment.CardNumber[len(payment.CardNumber)-4:])
    paid := PaidInput{UserID: input.UserID, ItemID: input.ItemID, PaymentID: "PAY-123"}
    return durable.Select(
        durable.ReceiveSend(s.ProcessShipping, paid),
        durable.After(24*time.Hour, s.ExpireShipping, paid),
    ), nil
}

// Call handler: synchronous request-response, returns value to caller AND advances state
func (s *BookingService) CancelBooking(ctx *durable.Context, _ ReservedInput, _ CancelReq) (CancelResp, *durable.Continuation[BookingResult], error) {
    return CancelResp{Confirmed: true}, durable.Done(BookingResult{Status: "cancelled"}), nil
}

// Query: read-only, set at any point during the routine
durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})
```

### 4. SAGA / failure recovery

**Temporal** — manual try/catch with compensation tracking:

```java
try {
    activities.bookFlight(flightDetails);
    activities.bookHotel(hotelDetails);
    activities.bookCar(carDetails);
} catch (ActivityFailure e) {
    if (flightBooked) activities.cancelFlight(flightDetails);
    if (hotelBooked) activities.cancelHotel(hotelDetails);
}
```

**iWF** — each state configures a recovery state via `ProceedToStateWhenExecuteRetryExhausted`:

```java
@Override
public StateOptions getStateOptions() {
    return new StateOptions()
        .setProceedToStateWhenExecuteRetryExhausted(RecoveryState.class)
        .setExecuteApiRetryPolicy(new RetryPolicy().maximumAttempts(5));
}
```

**Durable Routines** — recovery handlers are attached per-step at registration time. Each step carries exactly the state it needs for compensation:

```go
// From docs/howto/saga/saga.go

func (s *TripService) BookHotel(ctx *durable.Context, input FlightBookedInput) (*durable.Continuation[TripResult], error) {
    hotelConf, err := s.bookHotel(ctx, input.HotelID)
    if err != nil {
        return nil, fmt.Errorf("book hotel: %w", err)
    }
    return durable.Continue(s.BookCar, HotelBookedInput{
        TripID:             input.TripID,
        CarRentalID:        input.CarRentalID,
        FlightConfirmation: input.FlightConfirmation,
        HotelConfirmation:  hotelConf,
    }), nil
}

// Recovery handler — receives the same input as the failed step, plus the error
func (s *TripService) CompensateHotel(ctx *durable.Context, input FlightBookedInput, err error) (*durable.Continuation[TripResult], error) {
    s.cancelFlight(ctx, input.FlightConfirmation)
    return nil, fmt.Errorf("book hotel failed, compensated flight: %w", err)
}

// Registration wires recovery handlers to their steps
durable.RegisterHandler(w, svc.BookHotel, durable.HandlerOptions{
    RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
}).WithRecoveryHandler(svc.CompensateHotel, durable.HandlerOptions{
    RetryPolicy: durable.RetryPolicy{MaxAttempts: 1},
})
```

Key difference: in Temporal you track what's been booked with boolean flags and write compensation in a catch block. In iWF and Durable Routines, each step's input struct already contains the confirmations from prior steps — the recovery handler knows exactly what to compensate.

### 5. Parallel execution

**Temporal:**

```go
// Go SDK
future1 := workflow.GoNamed("task1", func() { activity1.DoSomething() })
future2 := workflow.GoNamed("task2", func() { activity2.DoSomethingElse() })
future1.Get(ctx, nil)
future2.Get(ctx, nil)
```

**iWF** — fork into multiple states, synchronize via InternalChannel:

```java
// Fork
return StateDecision.multiNextStates(
    StateMovement.create(PROCESS_A_STATE),
    StateMovement.create(PROCESS_B_STATE)
);

// Join — wait for both
return CommandRequest.forAllOf(
    InternalChannelCommand.create(PROCESS_A_COMPLETE_CHANNEL),
    InternalChannelCommand.create(PROCESS_B_COMPLETE_CHANNEL)
);
```

**Durable Routines** — each child is its own durable routine with independent retries and event history. Children send results back to the parent via `BufferSend`:

```go
// From docs/howto/fanout/fanout.go

// Parent: start children
func (s *FanoutService) StartItems(ctx *durable.Context, input FanoutInput) (*durable.Continuation[FanoutResult], error) {
    parentID := ctx.RoutineID()
    var itemStub *ItemService
    for _, item := range input.Items {
        durable.BufferStart(ctx, fmt.Sprintf("item-%s", item.ID),
            itemStub.ProcessItem, ItemInput{ID: item.ID, Data: item.Data, ParentID: parentID})
    }
    return durable.ReceiveSend(s.CollectResult, CollectingInput{Pending: len(input.Items)}), nil
}

// Child: process and send result back
func (s *ItemService) ProcessItem(ctx *durable.Context, input ItemInput) (*durable.Continuation[durable.Unit], error) {
    result := ItemResult{ID: input.ID, Output: fmt.Sprintf("processed: %s", input.Data)}
    var stub *FanoutService
    durable.BufferSend(ctx, input.ParentID, stub.CollectResult, result)
    return durable.Done(durable.Unit{}), nil
}

// Parent: collect results one at a time
func (s *FanoutService) CollectResult(ctx *durable.Context, input CollectingInput, result ItemResult) (*durable.Continuation[FanoutResult], error) {
    input.Results = append(input.Results, result)
    input.Pending--
    if input.Pending > 0 {
        return durable.ReceiveSend(s.CollectResult, input), nil
    }
    return durable.Done(FanoutResult{Results: input.Results}), nil
}
```

Temporal's approach is the most concise for simple fork/join. iWF requires explicit state definitions and channel coordination. Durable Routines gives each child full routine isolation (independent retries, timeouts, event history) at the cost of more explicit plumbing.

### 6. Event loop / GenServer pattern

Long-lived routines that manage concurrent state — like a provider managing multiple reservation slots — follow an event-loop pattern similar to Elixir's GenServer. The routine processes messages one at a time, updating state and looping back to wait for the next message.

**Durable Routines** — use a helper method that defines the `Select` once, and have every handler return it:

```go
type ProviderState struct {
    ProviderID   string
    Reservations map[string]Reservation
}

type Reservation struct {
    UserID string
    SlotID string
}

// steadyStateSelect defines the event loop — the set of messages this routine handles.
// Defined once, returned by every handler.
func (s *Svc) steadyStateSelect(state ProviderState) *durable.Continuation[durable.Unit] {
    return durable.Select(
        durable.ReceiveSend(s.HandleReserve, state),
        durable.ReceiveSend(s.HandleConfirmAndPay, state),
        durable.ReceiveSend(s.HandleExpire, state),
    )
}

// Init runs once, then enters the event loop.
func (s *Svc) Init(ctx *durable.Context, input ProviderInput) (*durable.Continuation[durable.Unit], error) {
    state := ProviderState{ProviderID: input.ProviderID, Reservations: map[string]Reservation{}}
    return s.steadyStateSelect(state), nil
}

// Each handler does its work and loops back to the same Select.
func (s *Svc) HandleReserve(ctx *durable.Context, state ProviderState, msg ReserveSlot) (*durable.Continuation[durable.Unit], error) {
    state.Reservations[msg.SlotID] = Reservation{UserID: msg.UserID, SlotID: msg.SlotID}
    return s.steadyStateSelect(state), nil
}

func (s *Svc) HandleConfirmAndPay(ctx *durable.Context, state ProviderState, msg ConfirmAndPay) (*durable.Continuation[durable.Unit], error) {
    res, ok := state.Reservations[msg.SlotID]
    if !ok || res.UserID != msg.UserID {
        return s.steadyStateSelect(state), nil // reject stale/invalid
    }
    state.BookSlot(msg.SlotID, msg.UserID, msg.PaymentID)
    delete(state.Reservations, msg.SlotID)
    return s.steadyStateSelect(state), nil
}

func (s *Svc) HandleExpire(ctx *durable.Context, state ProviderState, msg ExpireMsg) (*durable.Continuation[durable.Unit], error) {
    if _, ok := state.Reservations[msg.SlotID]; ok {
        state.ReleaseSlot(msg.SlotID)
        delete(state.Reservations, msg.SlotID)
    }
    return s.steadyStateSelect(state), nil
}
```

The `steadyStateSelect` helper keeps the Select definition in one place. Adding a new message type means updating the helper and adding one handler — every existing handler automatically picks up the change because they all call the same method.

This is analogous to a Go goroutine running a `for { select { ... } }` loop, or an Elixir GenServer processing messages sequentially with `handle_cast`/`handle_call`/`handle_info`. Each message is handled in order against the current state.

**Why not use an intermediate handler?** You could write a `WaitForActivity` handler that returns the Select, and have each handler `Continue(WaitForActivity, state)`. This works but costs an extra Temporal activity per message (the `WaitForActivity` handler runs as an activity even though it does no real work). Returning the Select directly from each handler via the helper method avoids this overhead — one activity per message.

## What Durable Routines don't have (yet)

| Feature | iWF | Durable Routines |
|---|---|---|
| Persistence layer (DataAttributes) | Built-in key-value store accessible from any state or RPC | No built-in persistence — state is carried in handler input structs |
| Persistence locking | `@RPC(dataAttributesLocking = {...})` for atomic read-modify-write | Not applicable (no shared mutable persistence) |
| `forAllOf` / wait-for-all commands | `CommandRequest.forAllOf(...)` | Fan-out uses child routines + `BufferSend` to collect |
| `waitForStateExecutionCompletion` | `client.waitForStateExecutionCompletion(wfId, State.class)` | No equivalent — use `ClientCall` or `ClientQuery` to check progress |
| Internal channels | `InternalChannel` for communication between states within a workflow | Not applicable — handlers communicate via continuations and input structs |
| Channel draining | Atomic check on channel emptiness before completing | Not applicable (no channel/signal queue model) |
| Search attributes | Built-in, queryable via Temporal's visibility API | Not built-in |

## Architecture differences

| | Temporal | iWF | Durable Routines |
|---|---|---|---|
| Deployment | Temporal Server + Worker processes | iWF Server + Temporal Server + Worker processes | Worker binary only (library) |
| Execution model | Replay-based (deterministic re-execution) | Activity-based (no replay, iWF interpreter handles it) | Checkpoint-based (no replay, state persisted between steps) |
| Determinism constraints | Yes — no random, no system time, no non-deterministic I/O in workflow code | No — iWF abstracts this away | No — plain Go code, call anything |
| Language support | Go, Java, Python, TypeScript, .NET | Go, Java, Python | Go |
| Infrastructure | Requires Temporal Server (self-hosted or Cloud) | Requires iWF Server + Temporal Server | Requires a backing store (e.g., database) |
