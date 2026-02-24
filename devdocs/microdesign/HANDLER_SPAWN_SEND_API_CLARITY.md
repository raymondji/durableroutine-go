# Deferred Start/Send Analysis

The core issue: `ctx.Start(...)` and `Send(ctx, ...)` look like they execute immediately, but they actually buffer requests that only take effect after the handler returns its `Suspend` value.

**Option A: Rename methods** (`DeferStart`, `BufferSend`)
- Signals intent in the name itself.
- But no Go idiom maps cleanly: "Async" suggests goroutine-level concurrency, "Defer" collides with Go's `defer` keyword, "Buffer" is the closest but verbose.
- Adds naming friction for the most common operations.

**Option B: Fold into Suspend return value** — e.g. `Suspend(Select(...), Start(...), Send(...))`
- Consistent with the declarative model — everything the handler wants to happen is expressed in the return value.
- But fanout and pipeline examples call `Send`/`Start` in loops (variable number of calls), which doesn't fit a single return value cleanly. Would need a builder or variadic approach that's more awkward than imperative calls.

**Option C: Improved docs only**
- Zero API churn, zero migration cost.
- But relies on users reading docs — easy to miss, especially for experienced Go developers who expect side-effecting calls to execute immediately.

**Option D: Pending tokens** — return a value from `Start`/`Send` that is never used
- e.g. `_ = ctx.Start(...)` or `token := Send(ctx, ...)`
- Adds noise without real value. The token isn't useful since the operations are always deferred. Go's unused variable rules would force callers to assign to `_`, which doesn't communicate intent.

**Option E: Keep imperative calls, make deferred nature prominent in docs + naming convention**
- Keep the current imperative `ctx.Start(...)` and `Send(ctx, ...)` signatures.
- Add prominent doc comments: "Buffers a start/send request. The request is executed by the runtime after the handler returns its Suspend value, not immediately."
- Add a "Deferred Execution" section to `DESIGN_DOC.md` explaining the model.
- Pragmatic: loop-based usage in fanout/pipeline strongly favors imperative calls. The deferred nature is a runtime detail, not a caller concern.

### Recommendation

**Option E.** The current imperative call style is the right fit for Go — it works naturally in loops, conditionals, and error handling. The deferred execution is a runtime detail that should be documented clearly rather than encoded in awkward naming. Enhanced doc comments on `Start` and `Send`, plus a prominent section in the design doc, are sufficient. The risk of confusion is low because the operations have no return value that callers could depend on for ordering.
