# Handler Return Type: `(*Continuation, error)` vs `Continuation` with Error Clause

## Question

Should handlers return `(*Continuation[T], error)` (current) or a single `Continuation[T]` value type with a `Fail(err)` constructor that folds error into Continuation?

## Analysis

The current `(*Continuation[T], error)` pattern allows four combinations:

| Continuation | error | Meaning | Valid? |
|---------|-------|---------|--------|
| non-nil | nil | State transition | Yes |
| nil | non-nil | Retry / terminal error handler | Yes |
| nil | nil | ??? | No (runtime validation) |
| non-nil | non-nil | ??? | Ambiguous (unused) |

Two of four combinations are invalid or ambiguous.

### Alternative: single `Continuation` return with `Fail()` constructor

Add an `err` field to `Continuation` and a `Fail[T](err) Continuation[T]` constructor. Change all handler signatures from `(*Continuation[T], error)` to `Continuation[T]` (value type). CallFunc becomes `(Resp, Continuation[T])` instead of `(Resp, *Continuation[T], error)`.

**Pros of the alternative:**
- Value type eliminates nil Continuation entirely — can't return nil for a value type
- Removes trailing `, nil` on every success return
- Every return path produces exactly one value — fewer invalid states
- `Fail()` is explicit and self-documenting

**Cons of the alternative:**
- Breaking change to every handler signature and every example
- `Fail[T]()` requires explicit type parameter (Go can't infer it from context)
- Less Go-idiomatic — Go convention is `(value, error)`, not "error inside value"
- Zero-value `Continuation{}` still needs runtime validation (done=false, cases=nil, err=nil)

## Decision

**Keep `(*Continuation[T], error)`.** The Go `(value, error)` convention is well-understood, and errors naturally trigger Temporal activity retries (the activity returns the error directly). Folding error into Continuation would require the activity to extract the error and re-return it — an implementation leak that makes the API less idiomatic for no meaningful safety gain.

The `(nil, nil)` invalid state is caught by runtime validation in the `RunHandler` activity: if a `CallFunc` returns `err == nil && Continuation == nil`, the activity returns an error. Handlers must always return a Continuation when there is no error. Returning `(nil, err)` with a non-nil error is valid because the error triggers retry/terminal-error-handler logic.
