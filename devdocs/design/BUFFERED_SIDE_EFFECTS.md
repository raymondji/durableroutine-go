# Buffered Side Effects (BufferStart / BufferSend)

The core issue: `ctx.BufferStart(...)` and `BufferSend(ctx, ...)` buffer requests that only take effect after the handler returns its `Continuation` value, not immediately. The naming should make this clear.

**Option A: Prefix with Buffer** (`BufferStart`, `BufferSend`)
- Signals intent in the name itself — "Buffer" is the closest Go idiom for "collect now, flush later".
- Works naturally in loops, conditionals, and error handling.

**Option B: Fold into Continuation return value** — e.g. `Continuation(Select(...), Start(...), Send(...))`
- Consistent with the declarative model — everything the handler wants to happen is expressed in the return value.
- But fanout and pipeline examples call `BufferSend`/`BufferStart` in loops (variable number of calls), which doesn't fit a single return value cleanly. Would need a builder or variadic approach that's more awkward than imperative calls.

**Option C: Improved docs only**
- Zero API churn, zero migration cost.
- But relies on users reading docs — easy to miss, especially for experienced Go developers who expect side-effecting calls to execute immediately.

**Option D: Pending tokens** — return a value from `BufferStart`/`BufferSend` that is never used
- e.g. `_ = ctx.BufferStart(...)` or `token := BufferSend(ctx, ...)`
- Adds noise without real value. The token isn't useful since the operations are always deferred. Go's unused variable rules would force callers to assign to `_`, which doesn't communicate intent.

**Option E: Keep imperative calls with non-buffered names, make buffered nature prominent in docs**
- Use names like `ctx.Start(...)` and `Send(ctx, ...)` without prefix.
- Add prominent doc comments explaining the buffered execution model.
- Pragmatic, but risks confusing Go developers who expect immediate execution.

### Decision

**Option A.** The `Buffer` prefix makes the buffered execution model explicit in the API surface. Go developers will immediately understand that these operations collect requests that are flushed later by the runtime. The imperative call style still works naturally in loops and conditionals.
