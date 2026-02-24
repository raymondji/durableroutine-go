# Design Doc: Stateroutines — A Replay-Safe Alternative API for Temporal

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

Each durable stateroutine is an actor with a mailbox. Communication follows actor model conventions:

| Concept | Temporal primitive | Blocks caller? | Mutates state? | Advances state machine? |
|---|---|---|---|---|
| `OnSend` | Signal | No (fire-and-forget) | Yes | Yes |
| `OnCall` | Update | Yes (waits for response) | Yes | Yes |
| `SetQueryResult` | Query | Yes (instant response) | No (static value) | No |
| `OnTimer` | Timer | N/A | Yes | Yes |

**Rules:**
- Send (Signal): fire-and-forget, never blocks. Clients and stateroutines can Send.
- Call (Update): synchronous request-response. Only clients can Call.
- Query (Query): synchronous read-only. Only clients can Query. Returns a static value set via `SetQueryResult`.
- Stateroutines can only Send to each other — no cross-workflow blocking.

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

type StatusResp struct { Status string }
func (StatusResp) Kind() string { return "get-status" }

func (s *BookingService) ReserveItem(ctx *stateroutine.Context, state BookingState) (*stateroutine.Suspend[BookingResult], error) {
    reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}
    stateroutine.SetQueryResult(ctx, StatusResp{Status: "reserved"})
    return stateroutine.Select[BookingResult](
        stateroutine.OnSend(s.ProcessPayment, reserved),
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *stateroutine.Context, state ReservedState, msg PaymentInfo) (*stateroutine.Suspend[BookingResult], error) {
    paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
    return stateroutine.Select[BookingResult](
        stateroutine.OnSend(s.ProcessShipping, paid),
    ), nil
}

func (s *BookingService) ProcessShipping(ctx *stateroutine.Context, state PaidState, msg ShippingInfo) (*stateroutine.Suspend[BookingResult], error) {
    return stateroutine.Done(BookingResult{Status: "shipped"}), nil
}
```

This eliminates the "bloated shared struct" problem where every step's fields accumulate in a single type.

### API

The API is defined in the [`stateroutine/`](stateroutine/) package:

#### Interfaces

- **`HandlerState`** — interface with `Kind() string`, implemented by all handler state types. There is no separate "stateroutine args" interface — any `HandlerState` can serve as a stateroutine entry point via `Start`.
- **`Message`** — interface with `Kind() string`, implemented by all message and request types. The `Kind()` string is used as the Temporal signal/update/query name and as part of the handler registration key.

#### HandlerOptions and RetryPolicy

- **`RetryPolicy`** — configures retry behavior (max attempts, intervals, backoff, timeouts).
- **`HandlerOptions`** — required parameter on all `Add*` registration functions. Contains a `RetryPolicy` field. Use `HandlerOptions{}` for Temporal defaults.
- **Terminal error handlers** — registered via the `.OnTerminalError()` builder method on the registration returned by each `Add*` function. This enforces at most one terminal error handler at compile time. Invoked only after all retries in the RetryPolicy are exhausted, instead of failing the stateroutine.

Terminal error handlers share the same generic type parameters as the main handler, so Go enforces type safety at compile time:
- A `TerminalErrorFunc[S, T]` chained on `AddHandler` must match the handler's `S` and `T`.
- A `SendTerminalErrorFunc[S, M, T]` chained on `AddSendHandler` must match the handler's `S`, `M`, and `T`.
- A `CallTerminalErrorFunc[S, Req, Resp, T]` chained on `AddCallHandler` must match the handler's `S`, `Req`, `Resp`, and `T`.

Passing the wrong kind of terminal error handler (e.g., a `SendTerminalErrorFunc` to `AddHandler`'s `.OnTerminalError()`) is a compile-time error because the function signatures are incompatible.

Retry policies are set at handler registration level only, not on individual suspend cases.

#### Handler Registration

Because Go does not allow type parameters on methods, these are package-level functions that take `*Worker`. All require `HandlerOptions` and return a typed registration struct with an `OnTerminalError` method:

- **`AddHandler[S, T](w, handler, opts) handlerReg[S, T]`** — registers a HandlerFunc keyed by `handler:{state.Kind()}`. Any handler registered this way can serve as both a stateroutine entry point (via `Start`) and a continuation target (via `After`, `Continue`, `Default`, `OnTimer`). Chain `.OnTerminalError(te)` to register a terminal error handler.
- **`AddSendHandler[S, M, T](w, handler, opts) sendHandlerReg[S, M, T]`** — registers a SendFunc keyed by `send:{state.Kind()}:{msg.Kind()}`. Chain `.OnTerminalError(te)` to register a terminal error handler.
- **`AddCallHandler[S, Req, Resp, T](w, handler, opts) callHandlerReg[S, Req, Resp, T]`** — registers a CallFunc keyed by `call:{state.Kind()}:{req.Kind()}`. Chain `.OnTerminalError(te)` to register a terminal error handler.

#### Handler Signatures

`State` is constrained to `HandlerState`. `M` and `Req` are constrained to `Message`. `Result` is the stateroutine's result type, returned via `Done(result)` and retrieved via `Handle[T].Get`:

- **`HandlerFunc[State, Result]`** — `func(ctx *Context, state State) (*Suspend[Result], error)`
- **`SendFunc[State, M, Result]`** — `func(ctx *Context, state State, msg M) (*Suspend[Result], error)`
- **`CallFunc[State, Req, Resp, Result]`** — `func(ctx *Context, state State, req Req) (Resp, *Suspend[Result], error)`

#### Terminal Error Handler Signatures

Terminal error handlers are invoked only after all retries configured in the RetryPolicy are exhausted, instead of failing the stateroutine. They receive the same inputs as the original handler plus the final error:

- **`TerminalErrorFunc[State, Result]`** — `func(ctx *Context, state State, err error) (*Suspend[Result], error)`
- **`SendTerminalErrorFunc[State, M, Result]`** — `func(ctx *Context, state State, msg M, err error) (*Suspend[Result], error)`
- **`CallTerminalErrorFunc[State, Req, Resp, Result]`** — `func(ctx *Context, state State, req Req, err error) (Resp, *Suspend[Result], error)`

#### Suspend and Constructors

`Suspend[T]` is generic over the stateroutine's result type `T`. All handlers in a stateroutine return `*Suspend[T]`. Use `Unit` for stateroutines with no meaningful result.

- **`Done[T](result T)`** — complete the stateroutine with a typed result. Clients retrieve it via `Handle[T].Get`.
- **`After(duration, handler, state)`** — suspend until a timer fires. `T` inferred from handler.
- **`Select[T](cases...)`** — wait for the first of several events (OnSend, OnCall, OnTimer, Default). `T` must be specified explicitly.
- **`Continue(handler, state)`** — checkpoint state and immediately invoke handler (no waiting). `T` inferred from handler.
- **`OnTimer(duration, handler, state)`** — a timer case for use inside Select
- **`OnSend(handler, state)`** — fires when a Send message arrives (inbox name = `M.Kind()`)
- **`OnCall(handler, state)`** — fires when a client calls a method (method name = `Req.Kind()`)
- **`Default(handler, state)`** — a case for use inside Select that fires immediately if no other cases are ready (maps to `sel.AddDefault()`)

#### Context

- **`Context`** — wraps `context.Context` with stateroutine capabilities
- **`ctx.StateroutineID()`** — get the current stateroutine's ID (reply address for children)
- **`ctx.Spawn(id, state)`** — spawn a child stateroutine (state.Kind() determines handler)
- **`Send[S, M, T](ctx, stateroutineID, handler, msg)`** — buffer a fire-and-forget message to another stateroutine (delivered after handler returns). The handler is passed for type inference; pass `nil` with explicit type params when the sender doesn't have the receiver's handler. Inbox name = `msg.Kind()`.
- **`SetQueryResult[Resp](ctx, resp)`** — store a static query result that persists across state transitions. Takes effect after the current handler returns its Suspend. Replaces any existing result for the same `Resp.Kind()`. Clients retrieve the value via `ClientQuery`. Maps to a Temporal Query handler that returns the stored value.

#### Client (typed top-level functions)

- **`Start[S, T](client, ctx, id, handler, state)`** — start a new stateroutine instance (state.Kind() determines handler). The handler function is passed for Go type inference of the result type `T`. If `id` is empty, a random UUID is generated. Returns a typed `Handle[T]`.
- **`Handle[T].Get(ctx)`** — blocks until the stateroutine completes and returns the typed result. Maps to Temporal's `WorkflowRun.Get`.
- **`ClientSend[S, M, T](client, ctx, id, handler, msg)`** — fire-and-forget message to a stateroutine. The handler is passed for type inference of the target state kind (not called). Inbox name = `msg.Kind()`.
- **`ClientCall[S, Req, Resp, T](client, ctx, id, handler, req)`** — synchronous request-response. The handler function is passed for Go type inference of `Resp` (not called).
- **`ClientQuery[Resp](client, ctx, id, resp)`** — synchronous read-only query. Pass a zero value of the response type for routing (via `Kind()`) and type inference.
- **`ClientGet[T](client, ctx, id)`** — blocks until the stateroutine completes and returns the typed result. Useful when you only have a stateroutine ID (e.g., from config or database) and not a `Handle`.

#### Worker

- **`NewWorker(taskQueue)`** — creates a worker; register handlers with `Add*` functions before calling `Start`

### Examples

See the [`docs/howto/`](docs/howto/) directory:

- **[`docs/howto/reminder/`](docs/howto/reminder/reminder.go)** — Timer chain with per-handler state. Demonstrates `After` for simple timer-based progression.
- **[`docs/howto/order/`](docs/howto/order/order.go)** — Order lifecycle with Send + timer + Query. Demonstrates `OnSend`, `SetQueryResult`, `OnTimer`, and `Select`.
- **[`docs/howto/booking/`](docs/howto/booking/booking.go)** — Multi-step client-driven stateroutine with Send + Call + Query. Client sends payment/shipping info via `ClientSend`, can cancel via `ClientCall`, and check status via `ClientQuery`. Demonstrates `OnSendTerminalError` to release the reservation if payment fails after all retries.
- **[`docs/howto/auction/`](docs/howto/auction/auction.go)** — Auction with synchronous bidding via `ClientCall`. Bidders place bids and immediately learn whether they were accepted or outbid. Demonstrates `OnCall` for request-response that advances state, `SetQueryResult` for live status, `OnTimer` for auction close, and `OnCallTerminalError` to return an error response to the blocked caller without crashing the auction.
- **[`docs/howto/fanout/`](docs/howto/fanout/fanout.go)** — Fan-out/fan-in using child stateroutines and stateroutine-to-stateroutine Send. Parent spawns children via `ctx.Spawn`, children send results back via `stateroutine.Send`. Parent collects via `OnSend`.
- **[`docs/howto/pipeline/`](docs/howto/pipeline/pipeline.go)** — Producer-consumer pipeline. Producer sends items to consumer via `stateroutine.Send`. Consumer processes items one at a time via `OnSend`.
- **[`docs/howto/saga/`](docs/howto/saga/saga.go)** — SAGA compensation pattern with terminal error handlers. Sequential service calls with compensation via `OnTerminalError` — when all retries are exhausted, the terminal error handler runs compensation logic instead of failing the stateroutine.
- **[`docs/howto/batch/`](docs/howto/batch/batch.go)** — Chunked batch processing with cancellation. Processes a large dataset in chunks using `Select` + `Default`, checking for a cancel signal between chunks. Like a GenServer that checks its mailbox between batches.

### How Common Patterns Map

| Pattern | stateroutine approach |
|---|---|
| **Do something, sleep, do something** | Handler does work, returns `After(duration, nextHandler, state)`. Each handler is an activity. See [`docs/howto/reminder/`](docs/howto/reminder/reminder.go). |
| **Wait for one of several events** | Handler returns `Select(OnSend(...), OnCall(...), OnTimer(...))`. The runtime sets up a Temporal selector. Query results are registered separately via `SetQueryResult`. See [`docs/howto/order/`](docs/howto/order/order.go). |
| **Fan-out / fan-in** | Parent spawns children via `ctx.Spawn(id, state)`. Each child calls `stateroutine.Send(ctx, parentID, result)` to send results back. Parent collects via `OnSend`, one at a time. See [`docs/howto/fanout/`](docs/howto/fanout/fanout.go). |
| **Producer-consumer** | Producer calls `stateroutine.Send` in a loop to send items. Consumer uses `Select(OnSend(receiveItem, state), OnSend(receiveDone, state))` to process items and detect completion. See [`docs/howto/pipeline/`](docs/howto/pipeline/pipeline.go). |
| **SAGA compensation** | Register terminal error handlers via `OnTerminalError` that run compensation logic when retries are exhausted. See [`docs/howto/saga/`](docs/howto/saga/saga.go). |
| **Checkpoint and continue** | Handler does expensive work, returns `Continue(nextHandler, state)`. The runtime checkpoints state (continue-as-new boundary) and immediately invokes the next handler without waiting. |
| **Cancellable batch processing** | Process items in chunks. Between chunks, return `Select(OnSend(cancelHandler, state), Default(nextChunkHandler, state))`. If a cancel signal is pending it fires; otherwise Default continues to the next chunk. See [`docs/howto/batch/`](docs/howto/batch/batch.go). |
| **Drain buffered signals** | Handler returns `Select(OnSend(handler, state), Default(doneHandler, state))`. Processes pending signals one at a time; when none are buffered, the default case fires. |
| **Request-response** | Client uses `ClientCall(client, ctx, id, handler, req)` to invoke a method that returns a typed response. |
| **Read-only status check** | Client uses `ClientQuery(client, ctx, id, resp)` for instant, non-mutating reads of static query results. |

### Implementation Sketch

Each stateroutine maps to a single Temporal workflow. The workflow function is entirely library-generated:

```
StateroutineWorkflow(ctx, stateroutineID):
    state = initial state (deserialized from workflow input)
    handler = registered handler for state.Kind()

    loop:
        // Run the handler as an activity
        suspend, err = executeActivity(handler, state)
        if err != nil:
            // Check if a terminal error handler is registered
            if handler has onTerminalErrorKey:
                errorHandler = lookup(onTerminalErrorKey)
                suspend, err = executeActivity(errorHandler, state, err)
                if err != nil: fail workflow
            else:
                fail workflow
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
            if case.inbox (OnSend):
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

        // Apply query results registered via SetQueryResult during the activity.
        // These persist across state transitions until overridden.
        for each qr in ctx.queryResults:
            workflow.SetQueryHandler(ctx, qr.queryName, func() (resp, error) {
                return qr.result, nil
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
- **Signal → OnSend**: buffered Temporal signals dispatched to the matching inbox handler
- **Update → OnCall**: Temporal update handler runs the call handler as an activity, returns response
- **Query → SetQueryResult**: Temporal query handler runs synchronously in workflow context (read-only), returning the stored static value. Registered via `SetQueryResult` on Context, persists across state transitions until overridden.
- **Terminal error handlers** — registered under `error:{originalKey}` in the worker map. When all retries are exhausted, the runtime invokes the terminal error handler instead of failing the stateroutine.

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
- Type-safe results — `Suspend[T]` enforces that all handlers in a stateroutine agree on the result type at compile time. `Start` returns a typed `Handle[T]` so `Get` requires no manual type specification. `Done(result)` and `Handle[T].Get` are both typed.
- Call/Send/Query maps cleanly to Temporal's Update/Signal/Query primitives
- Uniform handler model — no distinction between "stateroutine entry point" and "continuation handler". Any `HandlerFunc` registered with `AddHandler` can serve as either.
- River-style registration: state types self-identify via `Kind()`, handlers registered at startup
- Terminal error handlers enable compensation patterns (SAGA) — instead of failing the stateroutine when retries are exhausted, transition to a terminal error handler that can compensate and continue

**Disadvantages:**
- No linear top-to-bottom code for multi-step sequences — each step is a separate handler function connected via `After`. This is more verbose than `sleep(); doNext()` but eliminates the checkpointing problem entirely.
- Each handler invocation is a separate activity execution — slightly more overhead than inline workflow code, but this is the price of replay-safety freedom

---

## Inspirations and Comparisons

stateroutine draws from several systems. This section maps concepts across them to help users with existing familiarity quickly build intuition.

### Inspirations

- **[Temporal](https://temporal.io/)** — The durability engine underneath. stateroutine builds on Temporal's workflow/activity model, signals, updates, queries, and timers. The key difference is that stateroutine moves all user code into activities, eliminating replay-safety constraints.
- **[Elixir GenServer](https://hexdocs.pm/elixir/GenServer.html)** — The actor model semantics. Each stateroutine is an actor with a mailbox. `Send` maps to `GenServer.cast`, `ClientCall` maps to `GenServer.call`, and the handler → suspend → handler loop mirrors GenServer's callback model where each callback returns the next state.
- **Go goroutines & channels** — The mental model for concurrency. `ctx.Spawn` is like `go func()`, `Send` is like `ch <- msg`, and `OnSend` is like `<-ch`. Fan-out/fan-in patterns look nearly identical to their goroutine+channel counterparts, but with durability.
- **[River](https://riverqueue.com/)** — Type safety ergonomics. River's pattern of job types that self-identify via `Kind()` and are registered at startup inspired stateroutine's handler registration model.

### Concept Comparison

| Concept | stateroutine | Temporal | Elixir GenServer | Go goroutines |
|---|---|---|---|---|
| **Unit of execution** | Stateroutine | Workflow | GenServer process | Goroutine |
| **Start** | `Start(client, ctx, id, handler, state)` | `client.ExecuteWorkflow(...)` | `GenServer.start_link(mod, args)` | `go func()` |
| **Fire-and-forget message** | `ClientSend[S,M,T]` / `Send[S,M,T]` | Signal | `GenServer.cast` | `ch <- msg` |
| **Request-response** | `ClientCall` | Update | `GenServer.call` | (no direct equivalent) |
| **Read-only query** | `ClientQuery` + `SetQueryResult` | Query | `:sys.get_state` / custom call | (no direct equivalent) |
| **Get result** | `Handle.Get` / `ClientGet` | `WorkflowRun.Get` | (process exit value) | (no direct equivalent) |
| **Spawn child** | `ctx.Spawn` | Child Workflow | `DynamicSupervisor.start_child` | `go func()` |
| **Sleep/timer** | `After` / `OnTimer` | `workflow.Sleep` / Timer | `Process.send_after` + `handle_info` | `time.After` |
| **State machine** | Handler returns `Suspend` | Workflow code + signals | `handle_cast` / `handle_call` returns `{:noreply, new_state}` | Manual with select |
| **Retry + compensation** | `OnTerminalError` / `RetryPolicy` | Activity retry policy | Supervisor restart strategy | Manual |
| **Durability** | Temporal (automatic) | Event history replay | (not durable by default) | (not durable) |
| **Continue-as-new** | Automatic (library-managed) | Manual `workflow.NewContinueAsNewError` | (not needed) | (not applicable) |

---

## Open Questions

1. ~~**Error handling and retries**: Handlers run as activities, so Temporal's activity retry policy applies. How should we expose retry configuration?~~ **Resolved**: `HandlerOptions` (containing a `RetryPolicy`) is a required parameter on all `Add*` registration functions. Terminal error handlers are registered via the `.OnTerminalError()` builder method on the returned registration, sharing the same generic type parameters as the main handler for compile-time type safety. They are invoked only after all retries are exhausted. Retry policies are set at handler registration level only, not on individual suspend cases.
2. **State size limits**: Temporal has payload size limits (~2MB default). Large state may need external storage.
3. **Testing**: Should support a local/in-memory mode for unit testing without a Temporal server.
4. **Observability**: How do we expose Temporal's native visibility (search attributes, workflow status) through the abstraction?
5. ~~**Handler identification for continue-as-new**: Need a strategy for identifying handler functions across continue-as-new boundaries (function names, registration, etc.).~~ **Resolved**: All state types implement `HandlerState` (`Kind() string`). Handlers are registered in a flat map with composite keys (`handler:{kind}`, `send:{state.Kind()}:{msg.Kind()}`, `call:{state.Kind()}:{req.Kind()}`). Error handlers are registered under `error:{originalKey}`. Each `Case` carries a `handlerKey` for runtime lookup after continue-as-new.
