# Design: Patching Running Routines

## Problem

When a routine is blocked on a `Select` (waiting for a timer, send, or call), there's no way to change what it's waiting for. This matters in two scenarios:

1. **Bug fix replay**: A handler returned a bad continuation (e.g., 10-year timer instead of 10-day). The developer deploys a fix and wants to re-run the handler to produce a corrected continuation.
2. **Operational patch**: An operator needs to replace the current continuation with something entirely different (e.g., skip a step, change state, force a timeout).

## Background: What the workflow stores

When a handler runs and returns a `Select(...)`, the workflow stores a `SerializedContinuation` with a list of `SerializedCase` entries. Each case has:
- Wait condition: `TimerDuration`, `SendName`, `CallName`, or `Immediate`
- `HandlerKey`: the *next* handler to invoke when the case fires
- `Input`: the state to pass to that next handler

Crucially, the continuation stores only **forward-looking** data — which handlers to run next — not **backward-looking** data — which handler produced this continuation. To support replaying the producer, we need to also store the producer's identity.

## Proposal

Add two features:

### Feature 1: Replay (Approach D)

Re-run the handler that produced the current continuation, using the same input it originally received. If the developer has deployed a fix, the handler now returns the corrected continuation.

### Feature 2: Patch (Approach C)

Run a registered "patch handler" that receives the current state (the full serialized continuation with all case inputs) and returns a replacement continuation. This is for cases where replay isn't sufficient — schema migrations, manual data corrections, skipping steps, etc.

### Feature 3: List routines by producer handler key

To find which routines are affected by a bug, expose a query that returns the producer handler key. This lets operators use Temporal's visibility/list APIs (or the in-memory equivalent) to find all routines currently waiting on a continuation produced by a specific handler.

## Detailed Design

### 1. Store the producer in SerializedContinuation

Add two fields to `SerializedContinuation`:

```go
// backend/temporal/types.go
type SerializedContinuation struct {
    Cases              []SerializedCase `json:"cases"`
    ProducerHandlerKey string           `json:"producerHandlerKey,omitempty"`
    ProducerInput      json.RawMessage  `json:"producerInput,omitempty"`
    ProducerMessage    json.RawMessage  `json:"producerMessage,omitempty"`
}
```

The workflow populates these after each activity completes, before entering the selector:
- `ProducerHandlerKey` = the handler key that just ran
- `ProducerInput` = the input that was passed to it
- `ProducerMessage` = the message (for send/call handlers), nil for plain handlers

For the in-memory backend, add the same fields to `RunOutput` (or store them alongside the current cases in the instance loop).

### 2. Auto-register a `__producer` query

The workflow automatically registers a Temporal query handler named `"__producer"` that returns the current `ProducerHandlerKey`. This is set after every activity completes and preserved across continue-as-new.

This enables:
```go
// Find all routines produced by the buggy handler
// Using Temporal CLI or visibility API:
// temporal workflow list --query "ProducerHandlerKey = 'handler:reserved:booking-result'"
```

However, Temporal queries return data from the workflow, not from search attributes. To support `list` queries, we should also set a **custom search attribute** `ProducerHandlerKey` on the workflow. This requires:
- The user to create the search attribute in their Temporal cluster (one-time setup)
- The workflow to upsert the search attribute after every activity

We'll make search attribute upsert optional (best-effort; skip if the attribute isn't registered) so users who don't need listing don't need to set anything up.

**Alternative for in-memory**: The `Runtime` can expose a `ListByProducer(handlerKey string) []string` method that iterates instances and checks the stored producer key.

### 3. Replay API

#### Client interface

Add to `ClientImpl`:
```go
Replay(ctx context.Context, id string) error
```

Add typed wrapper in `durable/client.go`:
```go
func Replay(c Client, ctx context.Context, id string) error
```

Note: Replay is intentionally untyped — it doesn't need type parameters because it re-runs whatever handler is stored in `ProducerHandlerKey` with `ProducerInput`.

#### Temporal implementation

Replay is delivered as a Temporal Update named `"__replay"`. The workflow:

1. Registers an `"__replay"` Update handler when entering the selector (alongside call handlers).
2. When `__replay` fires:
   a. Cancel the current selector (by sending on a dedicated `replayCh`).
   b. Read `ProducerHandlerKey` and `ProducerInput` from the stored continuation.
   c. Set `handlerKey = ProducerHandlerKey`, `handlerInput = ProducerInput`, `message = ProducerMessage`.
   d. Loop back to run the handler as an activity.
   e. The Update returns success/error after the replay activity completes and the new continuation is established.

**Edge cases**:
- If `ProducerHandlerKey` is empty (pre-migration routines): return an error from the Update handler.
- If the replayed handler has side effects: user responsibility. Handlers should be idempotent, or the user should use Patch instead.
- If a call handler produced the continuation: replay re-runs the call handler with the original request. The call response is discarded (the original caller already received it).

#### In-memory implementation

The `Runtime` adds a `Replay(id string) error` method. It:
1. Looks up the instance.
2. Interrupts the `waitForCase` loop (via a new `replayCh` channel on the instance).
3. The runInstance loop reads the stored producer info and re-runs the handler.

### 4. Patch API

#### Handler registration

```go
// durable/routine.go
func RegisterPatchHandler[I Payload, T Payload](
    w *Worker,
    h func(ctx *Context, input I, currentContinuation PatchInfo) (*Continuation[T], error),
    opts HandlerOptions,
) patchHandlerReg[I, T]
```

Where `PatchInfo` provides typed access to the current continuation state:

```go
// durable/patch.go
type PatchInfo struct {
    ProducerHandlerKey string
    ProducerInput      json.RawMessage
    Cases              []PatchCase
}

type PatchCase struct {
    HandlerKey    string
    Input         json.RawMessage
    TimerDuration *time.Duration
    SendName      string
    CallName      string
}
```

The patch handler receives:
- `input`: fresh input provided by the Patch call (e.g., operator-specified parameters)
- `currentContinuation`: the full state of what the routine is currently waiting on

The patch handler is registered with a key like `"patch:{inputKind}:{resultKind}"` so it can be looked up by the workflow.

#### Client interface

Add to `ClientImpl`:
```go
Patch(ctx context.Context, id string, inputKind string, resultKind string, input any) error
```

Add typed wrapper:
```go
func Patch[I Payload, T Payload](
    c Client,
    ctx context.Context,
    id string,
    handler func(ctx *Context, input I, info PatchInfo) (*Continuation[T], error),
    input I,
) error
```

The `handler` parameter is used only for type inference (same pattern as `Go`, `Send`, etc.).

#### Temporal implementation

Patch is delivered as a Temporal Update named `"__patch"`. The Update carries:
- `PatchHandlerKey`: the registered patch handler key
- `Input`: the operator-provided input

The workflow:
1. Registers a `"__patch"` Update handler when entering the selector.
2. When `__patch` fires:
   a. Cancel the current selector.
   b. Run the patch handler as an activity, passing it the operator input + `PatchInfo` (built from the current `SerializedContinuation`).
   c. The patch handler returns a new `Continuation`.
   d. Replace the current continuation and re-enter the selector loop.
   e. The Update returns success/error.

#### In-memory implementation

Similar to Replay — interrupt `waitForCase`, run the patch handler, replace continuation.

### 5. Wire format for Replay/Patch Updates

```go
// backend/temporal/types.go

// ReplayRequest is the Update input for __replay.
type ReplayRequest struct{}

// PatchRequest is the Update input for __patch.
type PatchRequest struct {
    PatchHandlerKey string          `json:"patchHandlerKey"`
    Input           json.RawMessage `json:"input"`
}
```

### 6. Workflow changes (workflow.go)

The main changes to the workflow loop:

```
// Before entering the selector (after step 5, "check if done"):

// Always register __replay and __patch Update handlers.
// These use a shared mechanism: set a "patchOverride" variable
// and send on replayCh to break the selector.

var patchOverride *patchAction // nil = no patch pending
replayCh := workflow.NewChannel(ctx)

workflow.SetUpdateHandler(ctx, "__replay", func(ctx workflow.Context, req ReplayRequest) (any, error) {
    if cont.ProducerHandlerKey == "" {
        return nil, fmt.Errorf("no producer info stored; cannot replay")
    }
    patchOverride = &patchAction{
        handlerKey: cont.ProducerHandlerKey,
        input:      cont.ProducerInput,
        message:    cont.ProducerMessage,
    }
    replayCh.SendAsync(true)
    // Block until the replay completes
    ...
})

workflow.SetUpdateHandler(ctx, "__patch", func(ctx workflow.Context, req PatchRequest) (any, error) {
    patchOverride = &patchAction{
        handlerKey: req.PatchHandlerKey,
        input:      req.Input,
        // Pass PatchInfo as the message field
        message:    marshaledPatchInfo,
    }
    replayCh.SendAsync(true)
    ...
})

// Add replayCh to the selector at highest priority (before timers):
sel.AddReceive(replayCh, func(ch workflow.ReceiveChannel, more bool) {
    var discard any
    ch.Receive(ctx, &discard)
    // patchOverride is already set by the Update handler
    handlerKey = patchOverride.handlerKey
    handlerInput = patchOverride.input
    message = patchOverride.message
})
```

**Update response timing**: The `__replay` and `__patch` Update handlers need to return a result after the replay/patch activity completes. This is similar to how call handlers work — the Update handler blocks on a response channel. The selector callback sets up the activity, and when it completes, sends the result back to the Update handler's response channel.

### 7. Finding affected routines

#### Option A: Custom search attribute (Temporal)

The workflow upserts a `ProducerHandlerKey` search attribute after every activity. Users can then query:

```
temporal workflow list --query "ProducerHandlerKey = 'handler:reserved:booking-result'"
```

This requires one-time cluster setup:
```
temporal operator search-attribute create --name ProducerHandlerKey --type Text
```

The library sets the search attribute via `workflow.UpsertSearchAttributes()`. If the attribute doesn't exist, the upsert is a no-op (we catch and ignore the error).

#### Option B: Query + iterate (any backend)

The `__producer` query lets you check individual routines:
```go
result, _ := durable.Query(client, ctx, "booking-123", ProducerInfo{})
// result.HandlerKey == "handler:reserved:booking-result"
```

For bulk discovery, iterate over known routine IDs (from your database) and query each one. Less efficient than search attributes but works without cluster setup.

#### Option C: In-memory backend

```go
// backend/inmemory/runtime.go
func (r *Runtime) ListByProducer(handlerKey string) []string
```

Iterates all instances, returns IDs where the stored producer matches.

### 8. Continue-as-new preservation

The `WorkflowInput` already carries `HandlerKey` and `Input` across continue-as-new boundaries. We need to also carry the producer info. Two options:

**Option A**: Add `ProducerHandlerKey`, `ProducerInput`, `ProducerMessage` to `WorkflowInput`. The workflow restores these on the other side.

**Option B**: Since CAN already stores the next handler key/input (which become the new first activity), the producer info after CAN is just the handler that ran right before CAN triggered. This is already captured — the CAN input `HandlerKey`/`Input` are set from the last call handler's output continuation. So we just need to also carry the producer fields through.

Go with Option A for clarity:

```go
type WorkflowInput struct {
    HandlerKey         string           `json:"handlerKey"`
    Input              json.RawMessage  `json:"input"`
    QueryResults       []QueryEntry     `json:"queryResults,omitempty"`
    MaxHistoryLength   int32            `json:"maxHistoryLength,omitempty"`
    ProducerHandlerKey string           `json:"producerHandlerKey,omitempty"`
    ProducerInput      json.RawMessage  `json:"producerInput,omitempty"`
    ProducerMessage    json.RawMessage  `json:"producerMessage,omitempty"`
}
```

### 9. Patch handler signature design

The patch handler receives `PatchInfo` as a separate argument (not via the `message` field) because:
- It's structurally different from send/call messages
- It's always present for patch handlers, never present for regular handlers
- It contains multiple fields (cases array, producer info)

This means the patch handler runner needs a different invocation path in the activity. The `ActivityInput` gains an optional `PatchInfo` field:

```go
type ActivityInput struct {
    HandlerKey string          `json:"handlerKey"`
    RoutineID  string          `json:"routineID"`
    Input      json.RawMessage `json:"input"`
    Message    json.RawMessage `json:"message,omitempty"`
    Error      string          `json:"error,omitempty"`
    PatchInfo  *PatchInfo      `json:"patchInfo,omitempty"` // NEW
}
```

The patch handler runner reads `PatchInfo` from the activity input and passes it to the handler function.

## Implementation Plan

### Phase 1: Store producer info
1. Add `ProducerHandlerKey`, `ProducerInput`, `ProducerMessage` to `SerializedContinuation`.
2. Populate these in the workflow after each activity completes.
3. Carry them through continue-as-new via `WorkflowInput`.
4. In-memory: store equivalent fields in the instance run loop.
5. Register `__producer` query handler.

### Phase 2: Replay
1. Add `Replay` to `ClientImpl` interface.
2. Add `__replay` Update handler in workflow.
3. Implement replay in in-memory backend.
4. Add `durable.Replay()` typed wrapper.
5. Write tests using the booking example (fix 10-year timer to 10-day).

### Phase 3: Patch
1. Add `PatchInfo` type and `RegisterPatchHandler` registration function.
2. Add `Patch` to `ClientImpl` interface.
3. Add `__patch` Update handler in workflow.
4. Implement patch in in-memory backend.
5. Add `durable.Patch()` typed wrapper.
6. Write tests.

### Phase 4: Discoverability
1. Add optional `ProducerHandlerKey` search attribute upsert in workflow.
2. Add `ListByProducer` to in-memory runtime.
3. Document search attribute setup for Temporal users.

## Open Questions

1. **Should Replay return the new continuation state?** Currently proposed as `error`-only. Returning state info could be useful for verification but adds complexity.

2. **Concurrent replay/patch with pending calls**: If a call Update arrives while a replay is in progress, what happens? Proposed: reject calls while replay/patch is active (the Update handlers are re-registered after the new continuation is established). Temporal's Update handler replacement semantics may handle this naturally.

3. **Patch handler retry policy**: Should patch handlers have their own retry config, or always use a default? Proposed: same `HandlerOptions` pattern as regular handlers.

4. **Should we support replaying send/call handlers?** A send handler needs a message, a call handler needs a request. The producer info stores these in `ProducerMessage`. This means replay of a send handler re-processes the original message. Is that desirable, or should we restrict replay to plain handlers only? Proposed: allow it — the producer info stores everything needed, and the user decides whether re-processing is safe.
