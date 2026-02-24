# Handler Return Type: `(*Suspend, error)` vs `Suspend` with Error Clause

## Question

Should handlers return `(*Suspend[T], error)` (current) or a single `Suspend[T]` value type with a `Fail(err)` constructor that folds error into Suspend?

## Analysis

The current `(*Suspend[T], error)` pattern allows four combinations:

| Suspend | error | Meaning | Valid? |
|---------|-------|---------|--------|
| non-nil | nil | State transition | Yes |
| nil | non-nil | Retry / terminal error handler | Yes |
| nil | nil | ??? | No (runtime validation) |
| non-nil | non-nil | ??? | Ambiguous (unused) |

Two of four combinations are invalid or ambiguous.

### Alternative: single `Suspend` return with `Fail()` constructor

Add an `err` field to `Suspend` and a `Fail[T](err) Suspend[T]` constructor. Change all handler signatures from `(*Suspend[T], error)` to `Suspend[T]` (value type). CallFunc becomes `(Resp, Suspend[T])` instead of `(Resp, *Suspend[T], error)`.

**Pros of the alternative:**
- Value type eliminates nil Suspend entirely — can't return nil for a value type
- Removes trailing `, nil` on every success return
- Every return path produces exactly one value — fewer invalid states
- `Fail()` is explicit and self-documenting

**Cons of the alternative:**
- Breaking change to every handler signature and every example
- `Fail[T]()` requires explicit type parameter (Go can't infer it from context)
- Less Go-idiomatic — Go convention is `(value, error)`, not "error inside value"
- Zero-value `Suspend{}` still needs runtime validation (done=false, cases=nil, err=nil)

## Decision

**Keep `(*Suspend[T], error)`.** The Go `(value, error)` convention is well-understood, and errors naturally trigger Temporal activity retries (the activity returns the error directly). Folding error into Suspend would require the activity to extract the error and re-return it — an implementation leak that makes the API less idiomatic for no meaningful safety gain.

The `(nil, nil)` invalid state is caught by runtime validation in the `RunHandler` activity: if a `CallFunc` returns `err == nil && Suspend == nil`, the activity returns an error. Handlers must always return a Suspend when there is no error. Returning `(nil, err)` with a non-nil error is valid because the error triggers retry/terminal-error-handler logic.
