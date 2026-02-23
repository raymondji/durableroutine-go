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
| `OnCast` (Inbox) | Signal | No (fire-and-forget) | Yes | Yes |
| `OnCall` (Method) | Update | Yes (waits for response) | Yes | Yes |
| `OnQuery` (Query) | Query | Yes (instant response) | No (value semantics) | No |
| `AfterFunc` | Timer | N/A | Yes | Yes |

**Rules:**
- Inboxes belong to a routine. Only the owning routine receives from its inboxes.
- Cast (Signal): fire-and-forget, never blocks. Clients and routines can Cast.
- Call (Update): synchronous request-response. Only clients can Call.
- Query (Query): synchronous read-only. Only clients can Query.
- Routines can only Cast to each other — no cross-workflow blocking.

### Per-Handler State

There is **no shared state type `S`** across handlers. Each handler declares its own state type. State flows forward explicitly through Suspend cases:

```go
func reserveItem(ctx *durable.Context, args BookingArgs) (*durable.Suspend, error) {
    reserved := ReservedState{UserID: args.UserID, ItemID: args.ItemID}
    return durable.Select(
        durable.OnCast(PaymentInbox, processPayment, reserved),
    ), nil
}

func processPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Suspend, error) {
    paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
    return durable.Select(
        durable.OnCast(ShippingInbox, processShipping, paid),
    ), nil
}
```

This eliminates the "bloated shared struct" problem where every step's fields accumulate in a single type.

### API

The API is defined in the [`durable/`](durable/) package:

#### Routine Identity (River-style)

Routine types are identified by their args implementing `RoutineArgs`:

- **`RoutineArgs`** — interface with `Kind() string`, identifies the routine type (Temporal workflow type)
- **`Workers`** / **`NewWorkers()`** — type-safe handler registry
- **`AddRoutine[Args](workers, handler)`** — registers a handler for a routine kind

#### Descriptors (compile-time type safety)

- **`Inbox[M]`** — typed descriptor for Cast messages (maps to Signal)
- **`Method[Req, Resp]`** — typed descriptor for Call (maps to Update)
- **`Query[Req, Resp]`** — typed descriptor for Query (maps to Query)

#### Handler Signatures

- **`HandlerFunc[State]`** — `func(ctx *Context, state State) (*Suspend, error)`
- **`CastFunc[State, M]`** — `func(ctx *Context, state State, msg M) (*Suspend, error)`
- **`CallFunc[State, Req, Resp]`** — `func(ctx *Context, state State, req Req) (Resp, *Suspend, error)`
- **`QueryFunc[State, Req, Resp]`** — `func(ctx *Context, state State, req Req) (Resp, error)`

#### Suspend Constructors

- **`After(duration, handler, state)`** — suspend until a timer fires
- **`Select(cases...)`** — wait for the first of several events
- **`AfterFunc(duration, handler, state)`** — a timer case for use inside Select
- **`OnCast(inbox, handler, state)`** — fires when a Cast message arrives
- **`OnCall(method, handler, state)`** — fires when a client calls a method
- **`OnQuery(query, handler, state)`** — fires when a client queries

#### Context

- **`Context`** — wraps `context.Context` with routine capabilities
- **`ctx.RoutineID()`** — get the current routine's ID (reply address for children)
- **`ctx.Spawn(id, args)`** — spawn a child routine (args.Kind() determines handler)
- **`Cast[M](ctx, routineID, inbox, msg)`** — send a fire-and-forget message to another routine

#### Client (typed top-level functions)

- **`Start[Args](client, ctx, id, args)`** — start a new routine instance (args.Kind() determines handler)
- **`ClientCast[M](client, ctx, id, inbox, msg)`** — fire-and-forget message to a routine
- **`ClientCall[Req, Resp](client, ctx, id, method, req)`** — synchronous request-response
- **`ClientQuery[Req, Resp](client, ctx, id, query, req)`** — synchronous read-only query

#### Worker

- **`NewWorker(taskQueue, workers)`** — creates a worker using a handler registry

### Examples

See the [`examples/`](examples/) directory:

- **[`examples/reminder/`](examples/reminder/main.go)** — Timer chain with per-handler state. Demonstrates `After` for simple timer-based progression.
- **[`examples/order/`](examples/order/main.go)** — Order lifecycle with Cast + timer + Query. Demonstrates `OnCast`, `OnQuery`, `AfterFunc`, and `Select`.
- **[`examples/booking/`](examples/booking/main.go)** — Multi-step client-driven routine with Cast + Call + Query. Client sends payment/shipping info via `ClientCast`, can cancel via `ClientCall`, and check status via `ClientQuery`.
- **[`examples/fanout/`](examples/fanout/main.go)** — Fan-out/fan-in using child routines and routine-to-routine Cast. Parent spawns children via `ctx.Spawn`, children send results back via `durable.Cast`. Parent collects via `OnCast`.
- **[`examples/pipeline/`](examples/pipeline/main.go)** — Producer-consumer pipeline. Producer sends items to consumer via `durable.Cast`. Consumer processes items one at a time via `OnCast`.
- **[`examples/saga/`](examples/saga/main.go)** — SAGA compensation pattern. Sequential service calls with compensation on failure — just normal Go error handling.

### How Common Patterns Map

| Pattern | Suspension-based approach |
|---|---|
| **Do something, sleep, do something** | Handler does work, returns `After(duration, nextHandler, state)`. Each handler is an activity. See [`examples/reminder/`](examples/reminder/main.go). |
| **Wait for one of several events** | Handler returns `Select(OnCast(...), OnCall(...), OnQuery(...), AfterFunc(...))`. The runtime sets up a Temporal selector. See [`examples/order/`](examples/order/main.go). |
| **Fan-out / fan-in** | Parent spawns children via `ctx.Spawn(id, args)`. Each child calls `durable.Cast(ctx, parentID, inbox, result)` to send results back. Parent collects via `OnCast`, one at a time. See [`examples/fanout/`](examples/fanout/main.go). |
| **Producer-consumer** | Producer calls `durable.Cast` in a loop to send items. Consumer uses `Select(OnCast(ItemsInbox, ...), OnCast(DoneInbox, ...))` to process items and detect completion. See [`examples/pipeline/`](examples/pipeline/main.go). |
| **SAGA compensation** | Handler calls services sequentially; on error, calls compensation. All normal Go error handling. See [`examples/saga/`](examples/saga/main.go). |
| **Request-response** | Client uses `ClientCall(client, ctx, id, method, req)` to invoke a method that returns a typed response. |
| **Read-only status check** | Client uses `ClientQuery(client, ctx, id, query, req)` for instant, non-mutating reads. |

### Implementation Sketch

Each routine maps to a single Temporal workflow. The workflow function is entirely library-generated:

```
RoutineWorkflow(ctx, routineID):
    state = args (deserialized from workflow input)
    handler = registered handler for args.Kind()

    loop:
        // Run the handler as an activity
        suspend, err = executeActivity(handler, state)
        if err != nil: fail workflow
        if suspend == nil: complete workflow

        // Interpret the Suspend declaratively
        for each case in suspend.cases:
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
            if case.query (OnQuery):
                // Register a Query handler (read-only, no activity needed)
                workflow.SetQueryHandler(ctx, case.queryName, func(req) (resp, error) {
                    return case.handler(case.state, req)
                })
        sel.Select(ctx)

        // Handle child spawns from ctx.Spawn calls
        for each spawn in ctx.spawnRequests:
            workflow.ExecuteChildWorkflow(ctx, spawn.args.Kind(), spawn.args)

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
- **Query → OnQuery**: Temporal query handler runs synchronously in workflow context (read-only)

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
- Type-safe messages via `Inbox[M]`, `Method[Req, Resp]`, `Query[Req, Resp]` with generics
- Call/Cast/Query maps cleanly to Temporal's Update/Signal/Query primitives
- River-style registration: args types self-identify via `Kind()`, handlers registered at startup

**Disadvantages:**
- No linear top-to-bottom code for multi-step sequences — each step is a separate handler function connected via `After`. This is more verbose than `sleep(); doNext()` but eliminates the checkpointing problem entirely.
- Each handler invocation is a separate activity execution — slightly more overhead than inline workflow code, but this is the price of replay-safety freedom

---

## Open Questions

1. **Error handling and retries**: Handlers run as activities, so Temporal's activity retry policy applies. How should we expose retry configuration?
2. **State size limits**: Temporal has payload size limits (~2MB default). Large state may need external storage.
3. **Testing**: Should support a local/in-memory mode for unit testing without a Temporal server.
4. **Observability**: How do we expose Temporal's native visibility (search attributes, workflow status) through the abstraction?
5. **Handler identification for continue-as-new**: Need a strategy for identifying handler functions across continue-as-new boundaries (function names, registration, etc.).
