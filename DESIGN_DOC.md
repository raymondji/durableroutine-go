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
- State is an explicit, serialisable struct — trivial to carry across continue-as-new boundaries

### API

The API is defined in the [`durable/`](durable/) package. The key types are:

- **`Process[S]`** ([`durable/process.go`](durable/process.go)) — Defines a process with a name, state initialiser, and initial handler
- **`HandlerFunc[S]`** — `func(ctx, *S) (*Suspend[S], error)` — a handler that receives state and returns the next suspension
- **`MessageHandlerFunc[S, M]`** — like `HandlerFunc` but also receives a typed message
- **`Suspend[S]`** ([`durable/suspend.go`](durable/suspend.go)) — Describes what to wait for next
- **`After(duration, handler)`** — suspend until a timer fires
- **`Select(cases...)`** — wait for the first of several events
- **`Receive(channel, handler)`** — a case that fires when a message arrives
- **`AfterFunc(duration, handler)`** — a timer case for use inside Select
- **`Spawn`** — describes a child process to start (ProcessID, Process, Args)
- **`Suspend.WithSpawns(...)`** — attach child process spawns to any Suspend; children start when the Suspend is entered
- **`SendMessage(ctx, processID, channel, msg)`** ([`durable/send.go`](durable/send.go)) — send a message to another process's channel from within a handler, like `ch <- msg`
- **`ProcessID(ctx)`** ([`durable/send.go`](durable/send.go)) — get the current process's ID, so you can pass it to children as a "reply address"
- **`Client`** ([`durable/client.go`](durable/client.go)) — Starts processes and sends messages
- **`Worker`** ([`durable/worker.go`](durable/worker.go)) — Registers processes and polls for work

### Examples

See the [`examples/`](examples/) directory:

- **[`examples/reminder/`](examples/reminder/main.go)** — A process that sends a sequence of emails with durable sleeps between them. Demonstrates `After` for simple timer-based progression.
- **[`examples/order/`](examples/order/main.go)** — An order lifecycle process that waits for placement, handles cancellation or auto-ships after a window. Demonstrates `Select`, `Receive`, and `AfterFunc`.
- **[`examples/fanout/`](examples/fanout/main.go)** — Fan-out/fan-in mirroring Go's goroutine+channel pattern. A parent spawns child processes via `WithSpawns`, passes its own process ID as a reply address, and each child sends its result back via `durable.SendMessage`. The parent collects results one at a time via `Receive`.
- **[`examples/pipeline/`](examples/pipeline/main.go)** — Producer-consumer pipeline. A producer process sends many items to a consumer's channel via `durable.SendMessage`, then signals completion. The consumer receives items one at a time and processes each.
- **[`examples/saga/`](examples/saga/main.go)** — SAGA compensation pattern for a trip booking (flight + hotel + car). Sequential calls with compensation on failure — just normal Go error handling.
- **[`examples/booking/`](examples/booking/main.go)** — Multi-step client-driven process. Client starts a process with args, then drives each step by sending typed messages (`PaymentInfo`, `ShippingInfo`). Each step suspends with its own `Receive`, so State stays lean.

### How Common Patterns Map

| Pattern | Suspension-based approach |
|---|---|
| **Do something, sleep, do something** | Handler does work, returns `After(duration, nextHandler)`. Each handler is an activity. See [`examples/reminder/`](examples/reminder/main.go). |
| **Wait for one of several events** | Handler returns `Select(Receive(...), AfterFunc(...))`. The runtime sets up a Temporal selector. See [`examples/order/`](examples/order/main.go). |
| **Fan-out / fan-in** | Parent spawns children via `WithSpawns`, passing its own process ID as a reply address. Each child calls `durable.SendMessage` to send its result back. Parent collects via `Receive`, one at a time — like reading from a Go channel. See [`examples/fanout/`](examples/fanout/main.go). |
| **Producer-consumer** | Producer process calls `durable.SendMessage` in a loop to send items to a consumer's channel. Consumer uses `Select(Receive("items", ...), Receive("done", ...))` to process items and detect completion. See [`examples/pipeline/`](examples/pipeline/main.go). |
| **SAGA compensation** | Handler calls services sequentially; on error, calls compensation. All normal Go error handling. See [`examples/saga/`](examples/saga/main.go). |
| **Start and wait for result** | `Client.Start` then `Client.GetResult` blocks until the process completes and returns the final state. See [`examples/booking/`](examples/booking/main.go). |
| **Async fire-and-forget** | `Client.SendMessage` to a channel; the process receives it when it reaches a `Receive` case. |

### Implementation Sketch

Each `Process[S]` maps to a single Temporal workflow. The workflow function is entirely library-generated:

```
ProcessWorkflow(ctx, processID):
    state = initState()
    handler = process.Initial

    loop:
        // Run the handler as an activity
        suspend, err = executeActivity(handler, state)
        if err != nil: fail workflow
        if suspend == nil: complete workflow

        // Interpret the Suspend declaratively
        switch suspend:
            case After(d, next):
                workflow.Sleep(ctx, d)
                handler = next
            case Select(cases...):
                sel = workflow.NewSelector(ctx)
                for each case:
                    if case.timer:
                        sel.AddTimer(d, func() { handler = case.handler })
                    if case.channel:
                        sel.AddReceive(signalChan(case.channel), func(msg) {
                            handler = case.handler  // with msg bound
                        })
                sel.Select(ctx)
        // If suspend has child spawns, start them first
        if suspend.childSpawns:
            for each spawn in suspend.childSpawns:
                childFuture = workflow.ExecuteChildWorkflow(ctx, spawn.Process, spawn.Args)
                // When child completes, deliver its final state to the parent
                // as a signal on spawn.ResultChannel
                go onChildComplete(childFuture, spawn.ResultChannel)

        // Check continue-as-new
        if shouldContinueAsNew():
            return continueAsNew(ctx, state, handler)
```

Key implementation details:
- **Handlers run as activities** — no replay-safety concerns for user code
- **State is explicit and serialisable** — the `*S` struct is the single source of truth, trivially carried across continue-as-new
- **The workflow loop is library code** — it never changes, so replay is never broken by user code changes
- **Continue-as-new is trivial** — serialize `state` and a handler identifier, restart the loop

#### Automatic Continue-As-New

Because the workflow is a simple loop (run activity → interpret suspend → repeat), continue-as-new is straightforward:

1. After each iteration, check `workflow.GetInfo(ctx).GetContinueAsNewSuggested()`
2. If suggested, serialize the current state and a handler identifier
3. Call `workflow.NewContinueAsNewError` with the serialized state
4. The new execution resumes the loop with the same state and next handler

This is dramatically simpler than the previous approach because:
- State is a single explicit struct (no closure serialization)
- There is no "program counter" problem — the next handler is an explicit function reference
- No child workflows or goroutine futures to re-attach

### Tradeoffs

**Advantages:**
- User code has zero replay-safety constraints — handlers are pure activities
- State is explicit and serialisable — makes continue-as-new trivial
- The workflow code is 100% library-owned — user code changes never break replay
- Simple mental model: handler runs → returns what to wait for → runtime waits → next handler runs
- Type-safe messages via `Receive[S, M]` with generics

**Disadvantages:**
- No linear top-to-bottom code for multi-step sequences — each step is a separate handler function connected via `After`. This is more verbose than `sleep(); doNext()` but eliminates the checkpointing problem entirely.
- Each handler invocation is a separate activity execution — slightly more overhead than inline workflow code, but this is the price of replay-safety freedom

---

## Open Questions

1. **Error handling and retries**: Handlers run as activities, so Temporal's activity retry policy applies. How should we expose retry configuration?
3. **State size limits**: Temporal has payload size limits (~2MB default). Large state may need external storage.
4. **Testing**: Should support a local/in-memory mode for unit testing without a Temporal server.
5. **Observability**: How do we expose Temporal's native visibility (search attributes, workflow status) through the abstraction?
6. **Handler identification for continue-as-new**: Need a strategy for identifying handler functions across continue-as-new boundaries (function names, registration, etc.).
7. **Reply mechanism**: How should a handler send a response back to a waiting client? Could use a built-in reply channel or a separate mechanism.
