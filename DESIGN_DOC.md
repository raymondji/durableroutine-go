# Design Doc: Durable Routines — A Replay-Safe Alternative API for Temporal

## Problem Statement

Temporal workflows achieve durability by replaying workflow code against a stored event history. This requires workflow code to be **deterministic and replay-safe**: you cannot use `time.Now()`, `rand.Int()`, native goroutines, channels, or any I/O directly in workflow code. Instead, you must use Temporal-specific equivalents (`workflow.Now`, `workflow.Go`, `workflow.Channel`, etc.).

More critically, **evolving workflow code is painful**. When you change the sequence of commands a workflow issues (add a step, remove a step, reorder steps), running workflows whose event history was recorded under the old code will hit non-determinism errors and become stuck. Temporal provides `workflow.GetVersion` / patching to handle this, but it leads to accumulating conditional branches that must be preserved until all in-flight workflows from the old version complete — which for long-running workflows can be weeks or months.

Additionally, **Temporal requires users to manage `continue-as-new` for long-running workflows**. Temporal enforces a hard limit of ~50,000 events per workflow execution (with warnings at ~10,000). Every activity, timer, signal, and update adds events to the history. Long-running workflows — the kind that sit idle waiting for messages, process them over days or weeks — will inevitably hit this limit. Users must manually call `workflow.NewContinueAsNewError` to reset the history, which requires careful handling: draining pending signals/updates, serializing state, and ensuring nothing is lost in the transition. This is error-prone boilerplate that every long-running workflow must implement.

### Goal

Provide a Go library that builds on Temporal's durability guarantees but **eliminates the replay-safety and continue-as-new constraints** from the user's programming model. Users should be able to write normal Go code in their handlers and deploy new versions without worrying about compatibility with in-flight workflow histories or event history size limits.

---

## Approach: Suspension-Based Model

### Core Insight

The replay-safety problem stems from Temporal encoding the workflow's control flow in its event history. If user code runs inline with the workflow, any change to the control flow breaks running executions.

The suspension-based model solves this cleanly: **user handler functions run as Temporal activities** (no determinism constraints at all). When a handler needs to wait — for a timer, for an incoming message — it **returns a declarative `Suspend` value** instead of blocking. The library-owned workflow code interprets the `Suspend` to set up the appropriate Temporal primitives (timers, signal waits), and when one fires, it invokes the next handler as another activity.

This means:
- User code is always an activity — write normal Go, call databases, use `time.Now()`, whatever you want
- The workflow code is 100% library-owned and never changes, so replay is never broken
- State is explicit and serialisable — trivial to carry across continue-as-new boundaries

### Actor Model Semantics

Each durable routine is an actor with a mailbox. Communication follows actor model conventions:

| Concept | Temporal primitive | Blocks caller? | Mutates state? | Advances state machine? |
|---|---|---|---|---|
| `OnCast` | Signal | No (fire-and-forget) | Yes | Yes |
| `OnCall` | Update | Yes (waits for response) | Yes | Yes |
| `SetQueryHandler` | Query | Yes (instant response) | No (value semantics) | No |
| `OnTimer` | Timer | N/A | Yes | Yes |

**Rules:**
- Cast (Signal): fire-and-forget, never blocks. Clients and routines can Cast.
- Call (Update): synchronous request-response. Only clients can Call.
- Query (Query): synchronous read-only. Only clients can Query.
- Routines can only Cast to each other — no cross-workflow blocking.

### Per-Handler State

There is **no shared state type `S`** across handlers. Each handler declares its own state type. State flows forward explicitly through Suspend cases:

```go
type BookingState struct { UserID, ItemID string }
func (BookingState) Kind() string { return "booking" }

type ReservedState struct { UserID, ItemID string }
func (ReservedState) Kind() string { return "booking.reserved" }

type PaymentInfo struct { CardNumber, Expiry string }
func (PaymentInfo) Kind() string { return "payment" }

type BookingService struct { /* injected deps */ }

type BookingResult struct { Status string }

func (s *BookingService) ReserveItem(ctx *durable.Context, state BookingState) (*durable.Suspend[BookingResult], error) {
    reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}
    durable.SetQueryHandler(ctx, s.GetStatus, reserved)
    return durable.Select[BookingResult](
        durable.OnCast(s.ProcessPayment, reserved),
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Suspend[BookingResult], error) {
    paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
    return durable.Select[BookingResult](
        durable.OnCast(s.ProcessShipping, paid),
    ), nil
}

func (s *BookingService) ProcessShipping(ctx *durable.Context, state PaidState, msg ShippingInfo) (*durable.Suspend[BookingResult], error) {
    return durable.Done(BookingResult{Status: "shipped"}), nil
}
```

This eliminates the "bloated shared struct" problem where every step's fields accumulate in a single type.

### API

The API is defined in the [`durable/`](durable/) package:

#### Interfaces

- **`HandlerState`** — interface with `Kind() string`, implemented by all handler state types. There is no separate "routine args" interface — any `HandlerState` can serve as a routine entry point via `Start`.
- **`Message`** — interface with `Kind() string`, implemented by all message and request types. The `Kind()` string is used as the Temporal signal/update/query name and as part of the handler registration key.

#### Options

- **`RetryPolicy`** — configures retry behavior (max attempts, intervals, backoff, timeouts)
- **`WithRetryPolicy(p)`** — option for handler registration and suspend cases

#### Handler Registration

Because Go does not allow type parameters on methods, these are package-level functions that take `*Worker`. All accept `...HandlerOption`:

- **`AddHandler[S](w, handler)`** — registers a HandlerFunc keyed by `handler:{state.Kind()}`. Any handler registered this way can serve as both a routine entry point (via `Start`) and a continuation target (via `After`, `Continue`, `Default`, `OnTimer`).
- **`AddCastHandler[S, M](w, handler)`** — registers a CastFunc keyed by `cast:{state.Kind()}:{msg.Kind()}`
- **`AddCallHandler[S, Req, Resp](w, handler)`** — registers a CallFunc keyed by `call:{state.Kind()}:{req.Kind()}`
- **`AddQueryHandler[S, Req, Resp](w, handler)`** — registers a QueryFunc keyed by `query:{state.Kind()}:{req.Kind()}`

#### Handler Signatures

`State` is constrained to `HandlerState`. `M` and `Req` are constrained to `Message`. `Result` is the routine's result type, returned via `Done(result)` and retrieved via `Handle[T].Get`:

- **`HandlerFunc[State, Result]`** — `func(ctx *Context, state State) (*Suspend[Result], error)`
- **`CastFunc[State, M, Result]`** — `func(ctx *Context, state State, msg M) (*Suspend[Result], error)`
- **`CallFunc[State, Req, Resp, Result]`** — `func(ctx *Context, state State, req Req) (Resp, *Suspend[Result], error)`
- **`QueryFunc[State, Req, Resp]`** — `func(ctx *Context, state State, req Req) (Resp, error)` (no Result — queries don't advance state)

#### Suspend and Constructors

`Suspend[T]` is generic over the routine's result type `T`. All handlers in a routine return `*Suspend[T]`. Use `Unit` for routines with no meaningful result.

- **`Done[T](result T)`** — complete the routine with a typed result. Clients retrieve it via `Handle[T].Get`.
- **`After(duration, handler, state, ...CaseOption)`** — suspend until a timer fires. `T` inferred from handler.
- **`Select[T](cases...)`** — wait for the first of several events (OnCast, OnCall, OnTimer, Default). `T` must be specified explicitly.
- **`Continue(handler, state, ...CaseOption)`** — checkpoint state and immediately invoke handler (no waiting). `T` inferred from handler.
- **`OnTimer(duration, handler, state, ...CaseOption)`** — a timer case for use inside Select
- **`OnCast(handler, state, ...CaseOption)`** — fires when a Cast message arrives (inbox name = `M.Kind()`)
- **`OnCall(handler, state, ...CaseOption)`** — fires when a client calls a method (method name = `Req.Kind()`)
- **`Default(handler, state, ...CaseOption)`** — a case for use inside Select that fires immediately if no other cases are ready (maps to `sel.AddDefault()`)

#### Context

- **`Context`** — wraps `context.Context` with routine capabilities
- **`ctx.RoutineID()`** — get the current routine's ID (reply address for children)
- **`ctx.Spawn(id, state)`** — spawn a child routine (state.Kind() determines handler)
- **`Cast[M](ctx, routineID, msg)`** — send a fire-and-forget message to another routine (inbox name = `msg.Kind()`)
- **`SetQueryHandler[S, Req, Resp](ctx, handler, state)`** — register a query handler that persists across state transitions. Takes effect after the current handler returns its Suspend. Replaces any existing handler for the same `Req.Kind()`. Query handlers run concurrently with the routine (read-only, no state machine advancement). Maps to a Temporal Query handler.

#### Client (typed top-level functions)

- **`Start[S, T](client, ctx, id, handler, state)`** — start a new routine instance (state.Kind() determines handler). The handler function is passed for Go type inference of the result type `T`. If `id` is empty, a random UUID is generated. Returns a typed `Handle[T]`.
- **`Handle[T].Get(ctx)`** — blocks until the routine completes and returns the typed result. Maps to Temporal's `WorkflowRun.Get`.
- **`ClientCast[M](client, ctx, id, msg)`** — fire-and-forget message to a routine (inbox name = `msg.Kind()`)
- **`ClientCall[S, Req, Resp, T](client, ctx, id, handler, req)`** — synchronous request-response. The handler function is passed for Go type inference of `Resp` (not called).
- **`ClientQuery[S, Req, Resp](client, ctx, id, handler, req)`** — synchronous read-only query. The handler function is passed for Go type inference of `Resp` (not called).
- **`ClientGet[T](client, ctx, id)`** — blocks until the routine completes and returns the typed result. Useful when you only have a routine ID (e.g., from config or database) and not a `Handle`.

#### Worker

- **`NewWorker(taskQueue)`** — creates a worker; register handlers with `Add*` functions before calling `Start`

### Examples

See the [`examples/`](examples/) directory:

- **[`examples/reminder/`](examples/reminder/main.go)** — Timer chain with per-handler state. Demonstrates `After` for simple timer-based progression.
- **[`examples/order/`](examples/order/main.go)** — Order lifecycle with Cast + timer + Query. Demonstrates `OnCast`, `SetQueryHandler`, `OnTimer`, and `Select`.
- **[`examples/booking/`](examples/booking/main.go)** — Multi-step client-driven routine with Cast + Call + Query. Client sends payment/shipping info via `ClientCast`, can cancel via `ClientCall`, and check status via `ClientQuery`.
- **[`examples/fanout/`](examples/fanout/main.go)** — Fan-out/fan-in using child routines and routine-to-routine Cast. Parent spawns children via `ctx.Spawn`, children send results back via `durable.Cast`. Parent collects via `OnCast`.
- **[`examples/pipeline/`](examples/pipeline/main.go)** — Producer-consumer pipeline. Producer sends items to consumer via `durable.Cast`. Consumer processes items one at a time via `OnCast`.
- **[`examples/saga/`](examples/saga/main.go)** — SAGA compensation pattern. Sequential service calls with compensation on failure — just normal Go error handling.
- **[`examples/batch/`](examples/batch/main.go)** — Chunked batch processing with cancellation. Processes a large dataset in chunks using `Select` + `Default`, checking for a cancel signal between chunks. Like a GenServer that checks its mailbox between batches.

### How Common Patterns Map

| Pattern | Suspension-based approach |
|---|---|
| **Do something, sleep, do something** | Handler does work, returns `After(duration, nextHandler, state)`. Each handler is an activity. See [`examples/reminder/`](examples/reminder/main.go). |
| **Wait for one of several events** | Handler returns `Select(OnCast(...), OnCall(...), OnTimer(...))`. The runtime sets up a Temporal selector. Query handlers are registered separately via `SetQueryHandler`. See [`examples/order/`](examples/order/main.go). |
| **Fan-out / fan-in** | Parent spawns children via `ctx.Spawn(id, state)`. Each child calls `durable.Cast(ctx, parentID, result)` to send results back. Parent collects via `OnCast`, one at a time. See [`examples/fanout/`](examples/fanout/main.go). |
| **Producer-consumer** | Producer calls `durable.Cast` in a loop to send items. Consumer uses `Select(OnCast(receiveItem, state), OnCast(receiveDone, state))` to process items and detect completion. See [`examples/pipeline/`](examples/pipeline/main.go). |
| **SAGA compensation** | Handler calls services sequentially; on error, calls compensation. All normal Go error handling. See [`examples/saga/`](examples/saga/main.go). |
| **Checkpoint and continue** | Handler does expensive work, returns `Continue(nextHandler, state)`. The runtime checkpoints state (continue-as-new boundary) and immediately invokes the next handler without waiting. |
| **Cancellable batch processing** | Process items in chunks. Between chunks, return `Select(OnCast(cancelHandler, state), Default(nextChunkHandler, state))`. If a cancel signal is pending it fires; otherwise Default continues to the next chunk. See [`examples/batch/`](examples/batch/main.go). |
| **Drain buffered signals** | Handler returns `Select(OnCast(handler, state), Default(doneHandler, state))`. Processes pending signals one at a time; when none are buffered, the default case fires. |
| **Request-response** | Client uses `ClientCall(client, ctx, id, handler, req)` to invoke a method that returns a typed response. |
| **Read-only status check** | Client uses `ClientQuery(client, ctx, id, handler, req)` for instant, non-mutating reads. |

### Implementation Sketch

Each routine maps to a single Temporal workflow. The workflow function is entirely library-generated:

```
RoutineWorkflow(ctx, routineID):
    state = initial state (deserialized from workflow input)
    handler = registered handler for state.Kind()

    loop:
        // Run the handler as an activity
        suspend, err = executeActivity(handler, state)
        if err != nil: fail workflow
        if suspend.done: complete workflow

        // Interpret the Suspend declaratively

        // If the only case is immediate (Continue), skip the selector entirely
        if len(suspend.cases) == 1 && suspend.cases[0].immediate:
            handler = suspend.cases[0].handler
            state = suspend.cases[0].state
            continue

        for each case in suspend.cases:
            if case.immediate:
                // Default case — fires if no other cases are ready
                sel.AddDefault(func() {
                    handler = case.handler
                    state = case.state
                })
            if case.timer:
                sel.AddTimer(d, func() {
                    handler = case.handler
                    state = case.state
                })
            if case.inbox (OnCast):
                // Register a Signal handler
                signalChan = workflow.GetSignalChannel(ctx, case.inboxName)
                sel.AddReceive(signalChan, func(msg) {
                    handler = case.handler  // with msg bound
                    state = case.state
                })
            if case.method (OnCall):
                // Register an Update handler
                workflow.SetUpdateHandler(ctx, case.methodName, func(req) (resp, error) {
                    resp, suspend, err = executeActivity(case.handler, case.state, req)
                    return resp, err
                })
        sel.Select(ctx)

        // Apply query handlers registered via SetQueryHandler during the activity.
        // These persist across state transitions until overridden.
        for each qh in ctx.queryHandlers:
            workflow.SetQueryHandler(ctx, qh.queryName, func(req) (resp, error) {
                return qh.handler(qh.state, req)
            })

        // Handle child spawns from ctx.Spawn calls
        for each spawn in ctx.spawnRequests:
            workflow.ExecuteChildWorkflow(ctx, spawn.state.Kind(), spawn.state)

        // Check continue-as-new
        if shouldContinueAsNew():
            return continueAsNew(ctx, state, handler)
```

Key implementation details:
- **Handlers run as activities** — no replay-safety concerns for user code
- **State is per-handler and serialisable** — each case carries its own state, trivially serialized
- **The workflow loop is library code** — it never changes, so replay is never broken by user code changes
- **Continue-as-new is trivial** — serialize current state and handler identifier, restart the loop
- **Signal → OnCast**: buffered Temporal signals dispatched to the matching inbox handler
- **Update → OnCall**: Temporal update handler runs the call handler as an activity, returns response
- **Query → SetQueryHandler**: Temporal query handler runs synchronously in workflow context (read-only). Registered via `SetQueryHandler` on Context, persists across state transitions until overridden.

#### Automatic Continue-As-New

Because the workflow is a simple loop (run activity → interpret suspend → repeat), continue-as-new is straightforward:

1. After each iteration, check `workflow.GetInfo(ctx).GetContinueAsNewSuggested()`
2. If suggested, serialize the current state and a handler identifier
3. Call `workflow.NewContinueAsNewError` with the serialized state
4. The new execution resumes the loop with the same state and next handler

### Tradeoffs

**Advantages:**
- User code has zero replay-safety constraints — handlers are pure activities
- Per-handler state eliminates bloated shared structs — each handler only carries what it needs
- The workflow code is 100% library-owned — user code changes never break replay
- Simple mental model: handler runs → returns what to wait for → runtime waits → next handler runs
- Type-safe messages — message types self-identify via `Kind()`, handler functions provide type inference for `ClientCall`/`ClientQuery`
- Type-safe results — `Suspend[T]` enforces that all handlers in a routine agree on the result type at compile time. `Start` returns a typed `Handle[T]` so `Get` requires no manual type specification. `Done(result)` and `Handle[T].Get` are both typed.
- Call/Cast/Query maps cleanly to Temporal's Update/Signal/Query primitives
- Uniform handler model — no distinction between "routine entry point" and "continuation handler". Any `HandlerFunc` registered with `AddHandler` can serve as either.
- River-style registration: state types self-identify via `Kind()`, handlers registered at startup

**Disadvantages:**
- No linear top-to-bottom code for multi-step sequences — each step is a separate handler function connected via `After`. This is more verbose than `sleep(); doNext()` but eliminates the checkpointing problem entirely.
- Each handler invocation is a separate activity execution — slightly more overhead than inline workflow code, but this is the price of replay-safety freedom

---

## Open Questions

1. ~~**Error handling and retries**: Handlers run as activities, so Temporal's activity retry policy applies. How should we expose retry configuration?~~ **Resolved**: `RetryPolicy` struct with `WithRetryPolicy` option at both handler registration and per-case level. Priority: case-level > handler registration-level > Temporal default.
2. **State size limits**: Temporal has payload size limits (~2MB default). Large state may need external storage.
3. **Testing**: Should support a local/in-memory mode for unit testing without a Temporal server.
4. **Observability**: How do we expose Temporal's native visibility (search attributes, workflow status) through the abstraction?
5. ~~**Handler identification for continue-as-new**: Need a strategy for identifying handler functions across continue-as-new boundaries (function names, registration, etc.).~~ **Resolved**: All state types implement `HandlerState` (`Kind() string`). Handlers are registered in a flat map with composite keys (`handler:{kind}`, `cast:{state.Kind()}:{msg.Kind()}`, `call:{state.Kind()}:{req.Kind()}`, `query:{state.Kind()}:{req.Kind()}`). Each `Case` carries a `handlerKey` for runtime lookup after continue-as-new.
