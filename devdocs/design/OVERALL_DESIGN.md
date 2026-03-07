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

The suspension-based model solves this cleanly: **user handler functions run as Temporal activities** (no determinism constraints at all). When a handler needs to wait — for a timer, for an incoming message — it **returns a declarative `Continuation` value** instead of blocking. The library-owned workflow code interprets the `Continuation` to set up the appropriate Temporal primitives (timers, signal waits), and when one fires, it invokes the next handler as another activity.

This means:
- User code is always an activity — write normal Go, call databases, use `time.Now()`, whatever you want
- The workflow code is 100% library-owned and never changes, so replay is never broken
- Input is explicit and serialisable — trivial to carry across continue-as-new boundaries

### Actor Model Semantics

Each durable routine is an actor with a mailbox. Communication follows actor model conventions:

| Concept | Temporal primitive | Blocks caller? | Mutates state? | Advances state machine? |
|---|---|---|---|---|
| `ReceiveSend` | Signal | No (fire-and-forget) | Yes | Yes |
| `ReceiveCall` | Update | Yes (waits for response) | Yes | Yes |
| `SetQueryResult` | Query | Yes (instant response) | No (static value) | No |
| `After` | Timer | N/A | Yes | Yes |

**Rules:**
- Send (Signal): fire-and-forget, never blocks. Clients and routines can Send.
- Call (Update): synchronous request-response. Only clients can Call.
- Query (Query): synchronous read-only. Only clients can Query. Returns a static value set via `SetQueryResult`.
- Routines can only Send to each other — no cross-workflow blocking.

### Per-Handler Input

There is **no shared input type `I`** across handlers. Each handler declares its own input type. Input flows forward explicitly through Continuation cases:

```go
type BookingInput struct { UserID, ItemID string }
func (BookingInput) DurableKind() string { return "booking" }

type ReservedInput struct { UserID, ItemID string }
func (ReservedInput) DurableKind() string { return "booking.reserved" }

type PaymentInfo struct { CardNumber, Expiry string }
func (PaymentInfo) DurableKind() string { return "payment" }

type BookingService struct { /* injected deps */ }

type BookingResult struct { Status string }

type StatusResp struct { Status string }
func (StatusResp) DurableKind() string { return "get-status" }

func (s *BookingService) ReserveItem(ctx *durable.Context, input BookingInput) (*durable.Continuation[BookingResult], error) {
    reserved := ReservedInput{UserID: input.UserID, ItemID: input.ItemID}
    durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})
    return durable.Select[BookingResult](
        durable.ReceiveSend(s.ProcessPayment, reserved),
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, input ReservedInput, externalInput PaymentInfo) (*durable.Continuation[BookingResult], error) {
    paid := PaidInput{UserID: input.UserID, ItemID: input.ItemID, PaymentID: "PAY-123"}
    return durable.Select[BookingResult](
        durable.ReceiveSend(s.ProcessShipping, paid),
    ), nil
}

func (s *BookingService) ProcessShipping(ctx *durable.Context, input PaidInput, externalInput ShippingInfo) (*durable.Continuation[BookingResult], error) {
    return durable.Done(BookingResult{Status: "shipped"}), nil
}
```

This eliminates the "bloated shared struct" problem where every step's fields accumulate in a single input type.

### API

The API is defined in the [`durable/`](durable/) package:

#### Interfaces

- **`Payload`** — interface with `DurableKind() string`, implemented by all handler input, message, result, and query response types. The `DurableKind()` string is used as part of the handler registration key and for serialization across continue-as-new boundaries.

#### HandlerOptions and RetryPolicy

- **`RetryPolicy`** — configures retry behavior (max attempts, intervals, backoff, timeouts).
- **`HandlerOptions`** — required parameter on all `Register*` registration functions. Contains a `RetryPolicy` field. Use `HandlerOptions{}` for Temporal defaults.
- **Recovery handlers** — registered via the `.WithRecoveryHandler()` builder method on the registration returned by each `Register*` function. This enforces at most one recovery handler at compile time. Invoked only after all retries in the RetryPolicy are exhausted, instead of failing the routine.

Recovery handlers share the same generic type parameters as the main handler, so Go enforces type safety at compile time:
- A `RecoveryHandler[I, T]` chained on `RegisterHandler` must match the handler's `I` and `T`.
- A `SendRecoveryHandler[I, E, T]` chained on `RegisterSendHandler` must match the handler's `I`, `E`, and `T`.
- A `CallRecoveryHandler[I, Req, Resp, T]` chained on `RegisterCallHandler` must match the handler's `I`, `Req`, `Resp`, and `T`.

Passing the wrong kind of recovery handler (e.g., a `SendRecoveryHandler` to `RegisterHandler`'s `.WithRecoveryHandler()`) is a compile-time error because the function signatures are incompatible.

Retry policies are set at handler registration level only, not on individual continuation cases.

#### Handler Registration

Because Go does not allow type parameters on methods, these are package-level functions that take `*Worker`. All require `HandlerOptions` and return a typed registration struct with an `WithRecoveryHandler` method:

- **`RegisterHandler[I, T](w, handler, opts) handlerReg[I, T]`** — registers a Handler keyed by `handler:{input.DurableKind()}:{result.DurableKind()}`. Any handler registered this way can serve as both a routine entry point (via `Go`) and a continuation target (via `ContinueAfter`, `Continue`, `Default`, `After`). Chain `.WithRecoveryHandler(te)` to register a recovery handler.
- **`RegisterSendHandler[I, E, T](w, handler, opts) sendHandlerReg[I, E, T]`** — registers a SendHandler keyed by `send:{input.DurableKind()}:{externalInput.DurableKind()}:{result.DurableKind()}`. Chain `.WithRecoveryHandler(te)` to register a recovery handler.
- **`RegisterCallHandler[I, Req, Resp, T](w, handler, opts) callHandlerReg[I, Req, Resp, T]`** — registers a CallHandler keyed by `call:{input.DurableKind()}:{externalReq.DurableKind()}:{externalResp.DurableKind()}:{result.DurableKind()}`. Chain `.WithRecoveryHandler(te)` to register a recovery handler.

#### Handler Signatures

All generic type params (`Input`, `ExternalInput`, `ExternalReq`, `ExternalResp`, `Result`) are constrained to `Payload`:

- **`Handler[Input, Result]`** — `func(ctx *Context, input Input) (*Continuation[Result], error)`
- **`SendHandler[Input, ExternalInput, Result]`** — `func(ctx *Context, input Input, externalInput ExternalInput) (*Continuation[Result], error)`
- **`CallHandler[Input, ExternalReq, ExternalResp, Result]`** — `func(ctx *Context, input Input, externalReq ExternalReq) (ExternalResp, *Continuation[Result], error)`

#### Recovery Handler Signatures

Recovery handlers are invoked only after all retries configured in the RetryPolicy are exhausted, instead of failing the routine. They receive the same inputs as the original handler plus the final error:

- **`RecoveryHandler[Input, Result]`** — `func(ctx *Context, input Input, err error) (*Continuation[Result], error)`
- **`SendRecoveryHandler[Input, ExternalInput, Result]`** — `func(ctx *Context, input Input, externalInput ExternalInput, err error) (*Continuation[Result], error)`
- **`CallRecoveryHandler[Input, ExternalReq, ExternalResp, Result]`** — `func(ctx *Context, input Input, externalReq ExternalReq, err error) (ExternalResp, *Continuation[Result], error)`

#### Continuation and Constructors

`Continuation[T]` is generic over the routine's result type `T`. All handlers in a routine return `*Continuation[T]`. Use `Unit` for routines with no meaningful result.

- **`Done[T](result T)`** — complete the routine with a typed result. Clients retrieve it via `Handle[T].Get`.
- **`ContinueAfter(duration, handler, input)`** — wait until a timer fires. `T` inferred from handler.
- **`Select[T](cases...)`** — wait for the first of several events (ReceiveSend, ReceiveCall, After, Default). `T` must be specified explicitly.
- **`Continue(handler, input)`** — checkpoint input and immediately invoke handler (no waiting). `T` inferred from handler.
- **`After(duration, handler, input)`** — a timer case for use inside Select
- **`ReceiveSend(handler, input)`** — fires when a Send message arrives (inbox name = `ExternalInput.DurableKind()`)
- **`ReceiveCall(handler, input)`** — fires when a client calls a method (method name = `ExternalReq.DurableKind()`)
- **`Default(handler, input)`** — a case for use inside Select that fires immediately if no other cases are ready (maps to `sel.AddDefault()`)

#### Context

- **`Context`** — wraps `context.Context` with routine capabilities
- **`ctx.RoutineID()`** — get the current routine's ID (reply address for children)
- **`BufferStart[I, T](ctx, routineID, handler, input)`** — start a child routine (input.DurableKind() determines handler). Executes after the current handler returns its Continuation.
- **`BufferSend[I, E, T](ctx, routineID, handler, externalInput)`** — buffer a fire-and-forget message to another routine (delivered after handler returns). The handler is passed for type inference; use a nil stub for type inference when the sender doesn't have the receiver's handler. Inbox name = `externalInput.DurableKind()`.
- **`SetQueryResult[Resp](ctx, resp)`** — store a static query result that persists across state transitions. Takes effect after the current handler returns its Continuation. Replaces any existing result for the same `Resp.DurableKind()`. Clients retrieve the value via `Query`. Maps to a Temporal Query handler that returns the stored value.

#### Client (typed top-level functions)

- **`Go[I, T](client, ctx, id, handler, input)`** — start a new routine instance (input.DurableKind() determines handler). The handler function is passed for Go type inference of the result type `T`. If `id` is empty, a random UUID is generated. Returns a typed `Handle[T]`.
- **`Handle[T].Get(ctx)`** — blocks until the routine completes and returns the typed result. Maps to Temporal's `WorkflowRun.Get`.
- **`Send[I, E, T](client, ctx, id, handler, externalInput)`** — fire-and-forget message to a routine. The handler is passed for type inference of the target input kind (not called). Inbox name = `externalInput.DurableKind()`.
- **`Call[I, Req, Resp, T](client, ctx, id, handler, req)`** — synchronous request-response. The handler function is passed for Go type inference of `Resp` (not called).
- **`Query[Resp](client, ctx, id, resp)`** — synchronous read-only query. Pass a zero value of the response type for routing (via `DurableKind()`) and type inference.
- **`Get[T](client, ctx, id)`** — blocks until the routine completes and returns the typed result. Useful when you only have a routine ID (e.g., from config or database) and not a `Handle`.

#### Worker

- **`NewWorker(taskQueue)`** — creates a worker; register handlers with `Register*` functions before starting

### Examples

See the [`docs/howto/`](docs/howto/) directory:

- **[`docs/howto/reminder/`](docs/howto/reminder/reminder.go)** — Timer chain with per-handler state. Demonstrates `ContinueAfter` for simple timer-based progression.
- **[`docs/howto/order/`](docs/howto/order/order.go)** — Order lifecycle with Send + timer + Query. Demonstrates `ReceiveSend`, `SetQueryResult`, `After`, and `Select`.
- **[`docs/howto/booking/`](docs/howto/booking/booking.go)** — Multi-step client-driven routine with Send + Call + Query. Client sends payment/shipping info via `Send`, can cancel via `Call`, and check status via `Query`. Demonstrates `SendRecoveryHandler` to release the reservation if payment fails after all retries.
- **[`docs/howto/auction/`](docs/howto/auction/auction.go)** — Auction with synchronous bidding via `Call`. Bidders place bids and immediately learn whether they were accepted or outbid. Demonstrates `ReceiveCall` for request-response that advances state, `SetQueryResult` for live status, `After` for auction close, and `CallRecoveryHandler` to return an error response to the blocked caller without crashing the auction.
- **[`docs/howto/fanout/`](docs/howto/fanout/fanout.go)** — Fan-out/fan-in using child routines and routine-to-routine BufferSend. Parent starts children via `BufferStart`, children send results back via `durable.BufferSend`. Parent collects via `ReceiveSend`.
- **[`docs/howto/pipeline/`](docs/howto/pipeline/pipeline.go)** — Producer-consumer pipeline. Producer sends items to consumer via `durable.BufferSend`. Consumer processes items one at a time via `ReceiveSend`.
- **[`docs/howto/saga/`](docs/howto/saga/saga.go)** — SAGA compensation pattern with recovery handlers. Sequential service calls with compensation via `WithRecoveryHandler` — when all retries are exhausted, the recovery handler runs compensation logic instead of failing the routine.
- **[`docs/howto/batch/`](docs/howto/batch/batch.go)** — Chunked batch processing with cancellation. Processes a large dataset in chunks using `Select` + `Default`, checking for a cancel signal between chunks. Like a GenServer that checks its mailbox between batches.

### How Common Patterns Map

| Pattern | durable routine approach |
|---|---|
| **Do something, sleep, do something** | Handler does work, returns `ContinueAfter(duration, nextHandler, input)`. Each handler is an activity. See [`docs/howto/reminder/`](docs/howto/reminder/reminder.go). |
| **Wait for one of several events** | Handler returns `Select(ReceiveSend(...), ReceiveCall(...), After(...))`. The runtime sets up a Temporal selector. Query results are registered separately via `SetQueryResult`. See [`docs/howto/order/`](docs/howto/order/order.go). |
| **Fan-out / fan-in** | Parent starts children via `BufferStart(ctx, id, handler, input)`. Each child calls `durable.BufferSend(ctx, parentID, handler, result)` to send results back. Parent collects via `ReceiveSend`, one at a time. See [`docs/howto/fanout/`](docs/howto/fanout/fanout.go). |
| **Producer-consumer** | Producer calls `durable.BufferSend` in a loop to send items. Consumer uses `Select(ReceiveSend(receiveItem, input), ReceiveSend(receiveDone, input))` to process items and detect completion. See [`docs/howto/pipeline/`](docs/howto/pipeline/pipeline.go). |
| **SAGA compensation** | Register recovery handlers via `WithRecoveryHandler` that run compensation logic when retries are exhausted. See [`docs/howto/saga/`](docs/howto/saga/saga.go). |
| **Checkpoint and continue** | Handler does expensive work, returns `Continue(nextHandler, input)`. The runtime checkpoints input (continue-as-new boundary) and immediately invokes the next handler without waiting. |
| **Cancellable batch processing** | Process items in chunks. Between chunks, return `Select(ReceiveSend(cancelHandler, input), Default(nextChunkHandler, input))`. If a cancel signal is pending it fires; otherwise Default continues to the next chunk. See [`docs/howto/batch/`](docs/howto/batch/batch.go). |
| **Drain buffered signals** | Handler returns `Select(ReceiveSend(handler, input), Default(doneHandler, input))`. Processes pending signals one at a time; when none are buffered, the default case fires. |
| **Request-response** | Client uses `Call(client, ctx, id, handler, req)` to invoke a method that returns a typed response. |
| **Read-only status check** | Client uses `Query(client, ctx, id, resp)` for instant, non-mutating reads of static query results. |

### Implementation Sketch

Each routine maps to a single Temporal workflow. The workflow function is entirely library-generated:

```
RoutineWorkflow(ctx, routineID):
    input = initial input (deserialized from workflow input)
    handler = registered handler for input.DurableKind()

    loop:
        // Run the handler as an activity
        cont, err = executeActivity(handler, input)
        if err != nil:
            // Check if a recovery handler is registered
            if handler has RecoveryHandlerKey:
                errorHandler = lookup(RecoveryHandlerKey)
                cont, err = executeActivity(errorHandler, input, err)
                if err != nil: fail workflow
            else:
                fail workflow
        if cont.done: complete workflow

        // Interpret the Continuation declaratively

        // If the only case is immediate (Continue), skip the selector entirely
        if len(cont.cases) == 1 && cont.cases[0].immediate:
            handler = cont.cases[0].handler
            input = cont.cases[0].input
            continue

        for each case in cont.cases:
            if case.immediate:
                // Default case — fires if no other cases are ready
                sel.AddDefault(func() {
                    handler = case.handler
                    input = case.input
                })
            if case.timer:
                sel.AddTimer(d, func() {
                    handler = case.handler
                    input = case.input
                })
            if case.inbox (ReceiveSend):
                // Register a Signal handler
                signalChan = workflow.GetSignalChannel(ctx, case.inboxName)
                sel.AddReceive(signalChan, func(msg) {
                    handler = case.handler  // with msg bound
                    input = case.input
                })
            if case.method (ReceiveCall):
                // Register an Update handler
                workflow.SetUpdateHandler(ctx, case.methodName, func(req) (resp, error) {
                    resp, cont, err = executeActivity(case.handler, case.input, req)
                    return resp, err
                })
        sel.Select(ctx)

        // Apply query results registered via SetQueryResult during the activity.
        // These persist across state transitions until overridden.
        for each qr in ctx.queryResults:
            workflow.SetQueryHandler(ctx, qr.queryName, func() (resp, error) {
                return qr.result, nil
            })

        // Handle child starts from BufferStart calls
        for each start in ctx.startRequests:
            workflow.ExecuteChildWorkflow(ctx, start.input.DurableKind(), start.input)

        // Check continue-as-new
        if shouldContinueAsNew():
            return continueAsNew(ctx, input, handler)
```

Key implementation details:
- **Handlers run as activities** — no replay-safety concerns for user code
- **Input is per-handler and serialisable** — each case carries its own input, trivially serialized
- **The workflow loop is library code** — it never changes, so replay is never broken by user code changes
- **Continue-as-new is trivial** — serialize current input and handler identifier, restart the loop
- **Signal → ReceiveSend**: buffered Temporal signals dispatched to the matching inbox handler
- **Update → ReceiveCall**: Temporal update handler runs the call handler as an activity, returns response
- **Query → SetQueryResult**: Temporal query handler runs synchronously in workflow context (read-only), returning the stored static value. Registered via `SetQueryResult` on Context, persists across state transitions until overridden.
- **Recovery handlers** — registered under `error:{originalKey}` in the worker map. When all retries are exhausted, the runtime invokes the recovery handler instead of failing the routine.

#### Automatic Continue-As-New

Because the workflow is a simple loop (run activity → interpret continuation → repeat), continue-as-new is straightforward:

1. After each iteration, check `workflow.GetInfo(ctx).GetContinueAsNewSuggested()`
2. If suggested, serialize the current input and a handler identifier
3. Call `workflow.NewContinueAsNewError` with the serialized input
4. The new execution resumes the loop with the same input and next handler

### Tradeoffs

**Advantages:**
- User code has zero replay-safety constraints — handlers are pure activities
- Per-handler input eliminates bloated shared structs — each handler only carries what it needs
- The workflow code is 100% library-owned — user code changes never break replay
- Simple mental model: handler runs → returns what to wait for → runtime waits → next handler runs
- Type-safe messages — message types self-identify via `DurableKind()`, handler functions provide type inference for `Call`/`Query`
- Type-safe results — `Continuation[T]` enforces that all handlers in a routine agree on the result type at compile time. `Go` returns a typed `Handle[T]` so `Get` requires no manual type specification. `Done(result)` and `Handle[T].Get` are both typed.
- Call/Send/Query maps cleanly to Temporal's Update/Signal/Query primitives
- Uniform handler model — no distinction between "routine entry point" and "continuation handler". Any `Handler` registered with `RegisterHandler` can serve as either.
- River-style registration: input types self-identify via `DurableKind()`, handlers registered at startup
- Recovery handlers enable compensation patterns (SAGA) — instead of failing the routine when retries are exhausted, transition to a recovery handler that can compensate and continue

**Disadvantages:**
- No linear top-to-bottom code for multi-step sequences — each step is a separate handler function connected via `ContinueAfter`. This is more verbose than `sleep(); doNext()` but eliminates the checkpointing problem entirely.
- Each handler invocation is a separate activity execution — slightly more overhead than inline workflow code, but this is the price of replay-safety freedom

---

## Inspirations and Comparisons

Durable routine draws from several systems. This section maps concepts across them to help users with existing familiarity quickly build intuition.

### Inspirations

- **[Temporal](https://temporal.io/)** — The durability engine underneath. Durable routine builds on Temporal's workflow/activity model, signals, updates, queries, and timers. The key difference is that durable routine moves all user code into activities, eliminating replay-safety constraints.
- **[Elixir GenServer](https://hexdocs.pm/elixir/GenServer.html)** — The actor model semantics. Each routine is an actor with a mailbox. `BufferSend` maps to `GenServer.cast`, `Call` maps to `GenServer.call`, and the handler → continuation → handler loop mirrors GenServer's callback model where each callback returns the next state.
- **Go goroutines & channels** — The mental model for concurrency. `BufferStart` is like `go func()`, `BufferSend` is like `ch <- msg`, and `ReceiveSend` is like `<-ch`. Fan-out/fan-in patterns look nearly identical to their goroutine+channel counterparts, but with durability.
- **[River](https://riverqueue.com/)** — Type safety ergonomics. River's pattern of job types that self-identify via `Kind()` and are registered at startup inspired the handler registration model (our interface uses `DurableKind()`).

### Concept Comparison

| Concept | durable routine | Temporal | Elixir GenServer | Go goroutines |
|---|---|---|---|---|
| **Unit of execution** | Routine | Workflow | GenServer process | Goroutine |
| **Start** | `Go(client, ctx, id, handler, input)` | `client.ExecuteWorkflow(...)` | `GenServer.start_link(mod, args)` | `go func()` |
| **Fire-and-forget message** | `Send[I,E,T]` / `BufferSend[I,E,T]` | Signal | `GenServer.cast` | `ch <- msg` |
| **Request-response** | `Call` | Update | `GenServer.call` | (no direct equivalent) |
| **Read-only query** | `Query` + `SetQueryResult` | Query | `:sys.get_state` / custom call | (no direct equivalent) |
| **Get result** | `Handle.Get` / `Get` | `WorkflowRun.Get` | (process exit value) | (no direct equivalent) |
| **Start child** | `BufferStart` | Child Workflow | `DynamicSupervisor.start_child` | `go func()` |
| **Sleep/timer** | `ContinueAfter` / `After` | `workflow.Sleep` / Timer | `Process.send_after` + `handle_info` | `time.After` |
| **State machine** | Handler returns `Continuation` | Workflow code + signals | `handle_cast` / `handle_call` returns `{:noreply, new_state}` | Manual with select |
| **Retry + compensation** | `WithRecoveryHandler` / `RetryPolicy` | Activity retry policy | Supervisor restart strategy | Manual |
| **Durability** | Temporal (automatic) | Event history replay | (not durable by default) | (not durable) |
| **Continue-as-new** | Automatic (library-managed) | Manual `workflow.NewContinueAsNewError` | (not needed) | (not applicable) |

---

## Open Questions

1. ~~**Error handling and retries**: Handlers run as activities, so Temporal's activity retry policy applies. How should we expose retry configuration?~~ **Resolved**: `HandlerOptions` (containing a `RetryPolicy`) is a required parameter on all `Register*` registration functions. Recovery handlers are registered via the `.WithRecoveryHandler()` builder method on the returned registration, sharing the same generic type parameters as the main handler for compile-time type safety. They are invoked only after all retries are exhausted. Retry policies are set at handler registration level only, not on individual continuation cases.
2. **State size limits**: Temporal has payload size limits (~2MB default). Large state may need external storage.
3. **Testing**: Should support a local/in-memory mode for unit testing without a Temporal server.
4. **Observability**: How do we expose Temporal's native visibility (search attributes, workflow status) through the abstraction?
5. ~~**Handler identification for continue-as-new**: Need a strategy for identifying handler functions across continue-as-new boundaries (function names, registration, etc.).~~ **Resolved**: All types implement `Payload` (`DurableKind() string`). Handlers are registered in a flat map with composite keys (`handler:{input.DurableKind()}:{result.DurableKind()}`, `send:{input.DurableKind()}:{externalInput.DurableKind()}:{result.DurableKind()}`, `call:{input.DurableKind()}:{externalReq.DurableKind()}:{externalResp.DurableKind()}:{result.DurableKind()}`). Error handlers are registered under `error:{originalKey}`. Each `Case` carries a `handlerKey` for runtime lookup after continue-as-new.
