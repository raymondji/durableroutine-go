# Design Doc: In-Memory Implementation

This document describes the in-memory implementation of the stateroutine API, designed for deterministic unit testing without a Temporal server. The implementation lives in a separate `backend/inmemory/` package.

## Package Structure

```
backend/inmemory/
    client.go    — implements durable.Client
    runtime.go   — event loop, instance tracking, step execution
    clock.go     — controllable clock for deterministic timer testing
```

## Runtime

The `Runtime` is the central coordinator. It owns a map of stateroutine instances and drives execution step-by-step.

```go
type Runtime struct {
    worker    *durable.Worker
    clock     *Clock
    instances map[string]*instance
}

type instance struct {
    id           string
    handlerKey   string
    state        any
    cont         *serializedContinuation  // current continuation cases
    queryResults map[string]any      // persisted across transitions
    signals      map[string][]any    // buffered signals keyed by "send:{stateKind}:{msgKind}"
    done         bool
    result       any
    err          error
    waiters      []chan struct{}      // unblocked when done
}
```

### Creating a Runtime

```go
func NewRuntime(w *durable.Worker) *Runtime {
    return &Runtime{
        worker:    w,
        clock:     NewClock(),
        instances: make(map[string]*instance),
    }
}
```

The `Runtime` reads the handler registry from `durable.Worker` via the exposed `Handlers()` getter (same getter needed by the Temporal implementation).

## Client

The `inmemory.Client` implements `durable.Client` and holds a reference to the `Runtime`:

```go
type Client struct {
    runtime *Runtime
}
```

### Method Mappings

| durable.Client method | In-memory behavior |
|---|---|
| `start(ctx, id, kind, state)` | Create a new `instance` with `handlerKey = "handler:" + kind`. Run the handler immediately. Store the resulting continuation cases. |
| `send(ctx, id, stateKind, msgKind, msg)` | Look up instance. If a matching `ReceiveSend` case case is waiting, fire it immediately. Otherwise, buffer the signal in `instance.signals["send:" + stateKind + ":" + msgKind]`. |
| `call(ctx, id, methodName, req)` | Look up instance. Find the matching `ReceiveCall` case. Run the call handler synchronously. Return the response. |
| `query(ctx, id, queryName)` | Look up instance. Return `instance.queryResults[queryName]`. |
| `get(ctx, id)` | If instance is done, return result immediately. Otherwise, block on a channel until the instance completes. |

### Start Behavior

When `start` is called:
1. Create the instance
2. Look up the handler by `"handler:" + kind`
3. Run the handler synchronously
4. Capture query results, start requests, and send requests from the context
5. Store the continuation cases on the instance
6. Process start requests (recursively start children)
7. Process send requests (deliver signals to target instances)

## Step-by-Step Execution

The runtime provides explicit control over execution for deterministic testing:

```go
// Step processes one pending event (signal delivery, timer fire, default case).
// Returns true if an event was processed, false if nothing is pending.
func (r *Runtime) Step() bool

// StepAll processes all pending events until quiescence (no more work).
func (r *Runtime) StepAll()

// AdvanceTime advances the clock by d, firing any timers that expire.
// Then processes all resulting events until quiescence.
func (r *Runtime) AdvanceTime(d time.Duration)
```

### Step Logic

`Step()` evaluates all instances looking for one that can make progress:

1. **Default cases**: If an instance has a `Default` continuation case, fire it (process the next handler).
2. **Buffered signals**: If an instance has a `ReceiveSend` case and a matching signal is buffered, consume the signal and fire the handler.
3. **Expired timers**: If an instance has an `After` case and the clock has passed the timer deadline, fire the handler.

When a handler fires:
1. Run the handler synchronously
2. Capture query results, start requests, send requests
3. If the handler returns `Done`, mark the instance as done and notify waiters
4. Otherwise, store the new continuation cases
5. Process start requests and send requests

## Timer Simulation

The `Clock` provides a controllable time source:

```go
type Clock struct {
    now time.Time
}

func NewClock() *Clock {
    return &Clock{now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *Clock) Now() time.Time {
    return c.now
}

func (c *Clock) Advance(d time.Duration) {
    c.now = c.now.Add(d)
}
```

When `Runtime.AdvanceTime(d)` is called:
1. Advance the clock by `d`
2. Call `StepAll()` to fire any expired timers and process cascading events

Timer deadlines are stored as absolute times (`clock.Now().Add(duration)`) when the `After` continuation case is created. During `Step()`, the runtime checks `clock.Now() >= deadline` to decide whether to fire.

No real sleeping occurs — all time is simulated.

## Signal Buffering

Each instance maintains a per-inbox signal buffer:

```go
signals map[string][]any  // key: "send:{stateKind}:{msgKind}", value: ordered queue
```

When `send` is called on the client:
1. Build the signal key: `"send:" + stateKind + ":" + msgKind`
2. Check if the instance is currently waiting with a matching `ReceiveSend` case
3. If yes: consume the signal immediately, fire the handler, process results
4. If no: append to `instance.signals[key]` for later delivery

When an instance enters a new continuation state (after a handler returns):
- Check if any buffered signals match the new `ReceiveSend` cases
- If so, they can be consumed on the next `Step()` call

This matches Temporal's signal buffering behavior: signals that arrive before the workflow is ready to receive them are queued.

## Concurrency Model

**Sequential (deterministic) by default.** All handler execution is synchronous within `Step()` / `StepAll()` / `AdvanceTime()`. This makes tests fully deterministic — the same inputs always produce the same outputs in the same order.

This is the primary mode for unit testing. The test controls exactly when things happen:

```go
// Example test flow
runtime.StepAll()                           // Process initial handlers
runtime.Client().Send(ctx, id, handler, msg) // Deliver a signal
runtime.StepAll()                           // Process the signal handler
runtime.AdvanceTime(24 * time.Hour)         // Fire timers
```

## Exposed Getters Needed

The `durable.Worker` must expose (same as Temporal impl):
- `TaskQueue() string` — returns the task queue name
- `Handlers() map[string]handlerEntry` — returns the handler registry

Additionally, the `Case` struct fields are currently unexported. The in-memory runtime needs read access to:
- `Case.TimerDuration`
- `Case.SendName`
- `Case.CallName`
- `Case.Immediate`
- `Case.State`
- `Case.HandlerKey`

These can be exposed via getter methods on `Case` or by making the `Continuation` struct expose its cases via a method.

## Example Test Usage

```go
func TestBookingHappyPath(t *testing.T) {
    svc := &BookingService{}
    w := durable.NewWorker("test-queue")
    booking.RegisterHandlers(w, svc)

    rt := inmemory.NewRuntime(w)
    client := rt.Client()
    ctx := context.Background()

    // Start a booking
    h, err := durable.Start(client, ctx, "booking-1", svc.ReserveItem,
        BookingState{UserID: "user-1", ItemID: "item-1"})
    require.NoError(t, err)

    // Verify status is "reserved"
    status, err := durable.ClientQuery(client, ctx, "booking-1", StatusResp{})
    require.NoError(t, err)
    assert.Equal(t, "reserved", status.Status)

    // Send payment
    err = durable.ClientSend(client, ctx, "booking-1", svc.ProcessPayment,
        PaymentInfo{CardNumber: "4111111111111234", Expiry: "12/27"})
    require.NoError(t, err)
    rt.StepAll()

    // Send shipping
    err = durable.ClientSend(client, ctx, "booking-1", svc.ProcessShipping,
        ShippingInfo{Address: "123 Main St", City: "Springfield", Zip: "62704"})
    require.NoError(t, err)
    rt.StepAll()

    // Verify completion
    result, err := h.Get(ctx)
    require.NoError(t, err)
    assert.Equal(t, "shipped", result.Status)
}
```

## Differences from Temporal Implementation

| Aspect | Temporal | In-Memory |
|---|---|---|
| Durability | Persistent across restarts | In-process only |
| Concurrency | Real concurrent execution | Sequential by default |
| Timers | Real wall-clock timers | Simulated via `Clock.Advance` |
| Signals | Temporal signal infrastructure | In-process buffer |
| Continue-as-new | Triggered by history size | Not needed (no history) |
| Error retries | Temporal activity retry | Configurable (immediate retry or skip) |
