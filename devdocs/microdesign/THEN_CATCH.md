# Then and TerminalError — Sequential Continuation Composition

## Problem

Every suspension point in a durable routine requires a separately named handler function and input type, all returning `*Continuation[T]` with the same final result type `T`. This couples intermediate steps to the routine's result type and prevents reusing sub-chains across routines with different result types.

## Proposal

Two additions:

```go
// Then: sequential composition. When inner's chain reaches Done(result T),
// pass result as input to handler, which returns Continuation[U].
// If inner's chain fails (error), the routine fails — handler never runs.
func Then[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U]

// TerminalError wraps an error to signal it should not be retried.
// - From a regular handler: skip retries, go straight to RecoveryHandler.
// - From a RecoveryHandler: skip TE retries, routine fails immediately.
func TerminalError(err error) error
func IsTerminal(err error) bool
```

RecoveryHandler stays unchanged. It continues to provide per-handler, type-safe error recovery.

## Error model

```
Handler runs → fails with error
  ├─ regular error      → retry per RetryPolicy → exhausted → RecoveryHandler (if registered)
  └─ TerminalError  → skip retries          →           → RecoveryHandler (if registered)

RecoveryHandler runs
  ├─ returns (cont, nil)            → routine continues (Then chain continues if cont is Done)
  ├─ returns (nil, regular error)   → retry TE per its RetryPolicy → exhausted → routine fails
  └─ returns (nil, TerminalError) → skip TE retries → routine fails
```

### Interaction with Then

Given `Then(a, b)` where a handler in `a`'s chain fails:

1. RecoveryHandler fires (if registered). It has the failed step's exact input type.
2. If RecoveryHandler returns `(cont, nil)` → chain continues. If `cont` reaches Done → **b runs with the Done result**.
3. If RecoveryHandler returns `(nil, err)` → **routine fails. b never runs.** thenStack is discarded.
4. If no RecoveryHandler is registered → **routine fails. b never runs.**

The RecoveryHandler is the decision point: recover (return continuation) or abort (return error). Then respects that decision. No ambiguity — the developer explicitly chooses whether the pipeline continues or stops.

## TerminalError

Maps directly to Temporal's `temporal.NewNonRetryableApplicationError`. The library surfaces it as a first-class concept.

```go
// durable package
type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

func TerminalError(err error) error  { return &terminalError{err: err} }
func IsTerminal(err error) bool      { var nre *terminalError; return errors.As(err, &nre) }
```

Usage:

```go
func (s *TripService) BookHotel(ctx *durable.Context, input FlightBookedInput) (*durable.Continuation[TripResult], error) {
    conf, err := s.bookHotel(ctx, input.HotelID)
    if isPaymentDeclined(err) {
        // Payment declined — retrying won't help. Go straight to RecoveryHandler.
        return nil, durable.TerminalError(err)
    }
    if err != nil {
        return nil, err  // transient error — will be retried per RetryPolicy
    }
    return durable.Done(TripResult{...}), nil
}
```

Backend mapping:
- **Temporal**: Handler returns TerminalError → activity wraps as `temporal.NewNonRetryableApplicationError` → Temporal skips retries → workflow sees error → checks for RecoveryHandler.
- **In-memory**: No retry logic currently. TerminalError behaves the same as a regular error (RecoveryHandler fires on any error). If retry logic is added later, TerminalError will skip it.

## Design: per-case thenStack

Each `Case` carries a `thenStack []string` — handler keys to invoke sequentially when the case's handler chain reaches Done.

```go
type Case struct {
    timerDuration *time.Duration
    sendName      string
    callName      string
    immediate     bool
    input         any
    handlerKey    string
    thenStack     []string  // NEW: handler keys for Then chain
}
```

### How Then builds the thenStack

Then copies the inner continuation's cases and appends the new handler's key to each case's thenStack:

```go
func Then[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U] {
    if inner.IsDone() {
        return Continue(handler, inner.Result())  // short-circuit
    }
    var zeroT T
    var zeroU U
    thenKey := durablecore.HandlerKey(zeroT.DurableKind(), zeroU.DurableKind())
    newCases := make([]Case, len(inner.cases))
    for i, c := range inner.cases {
        newCases[i] = c
        newCases[i].thenStack = append(append([]string{}, c.thenStack...), thenKey)
    }
    return &Continuation[U]{cases: newCases}
}
```

### Trampoline behavior

The trampoline maintains a `thenStack []string` variable:

```
case fires → set thenStack = firedCase.thenStack

loop:
    output = runHandler(handlerKey, input, msg)

    if handler error (after retries + RecoveryHandler):
        // routine fails — thenStack discarded
        return error

    if output.Done:
        if len(thenStack) > 0:
            handlerKey = thenStack[0]
            thenStack = thenStack[1:]
            input = output.Result    // Done result becomes input to next handler
            msg = nil
            continue
        return output.Result

    // Non-Done: wait for next case
    wait for case → extract handlerKey, input, msg from fired case
    // Combine fired case's thenStack with current outer thenStack:
    thenStack = firedCase.thenStack + existingThenStack
```

**thenStack propagation:** When a handler inside a Then chain returns non-Done cases, those cases may carry their own thenStacks (from nested Then). When a new case fires, the trampoline combines the new case's thenStack with the existing outer thenStack by prepending: `newCaseThenStack + outerThenStack`.

## Composition with Select

Per-case thenStacks compose naturally with Select. Select merges cases, each preserving its own thenStack. No restrictions needed.

```go
// a's cases get thenStack [h1], b's cases get thenStack []
Select(Then(a, h1), b)

// a's cases get thenStack [h1], b's cases get thenStack [h2]
Select(Then(a, h1), Then(b, h2))

// All cases get thenStack [h1]
Then(Select(a, b), h1)

// Nested Select flattens as before
Select(Select(a, b), Select(c, d))  ==  Select(a, b, c, d)

// Nested Then accumulates the stack
Then(Then(a, h1), h2)  →  a's cases get thenStack [h1, h2]
```

## No concurrency introduced

Then does not introduce concurrency. At every point, exactly one handler runs. The thenStack is a return address — when Done is reached, the next handler runs sequentially. Select picks one winner; other branches are discarded.

The goroutine analogy holds: a goroutine's sequential code after a function call maps to Then's "after Done, run the next handler."

## Example: multi-Then happy path

```go
func (s *TripService) StartTrip(ctx *durable.Context, input TripBooking) (*durable.Continuation[TripResult], error) {
    step0 := durable.Continue(s.BookFlight, FlightBooking{
        TripID: input.TripID, FlightID: input.FlightID,
    })                                                        // *Continuation[FlightResult]
    step1 := durable.Then(step0, s.BookHotel)                  // *Continuation[HotelResult]
    step2 := durable.Then(step1, s.BookCar)                    // *Continuation[TripResult]
    return step2, nil
}
```

Each handler has its own result type. BookFlight returns `*Continuation[FlightResult]`, BookHotel returns `*Continuation[HotelResult]`, BookCar returns `*Continuation[TripResult]`. Then bridges between them.

## Example: Then with RecoveryHandler (saga)

```go
func (s *TripService) StartTrip(ctx *durable.Context, input TripBooking) (*durable.Continuation[TripResult], error) {
    step0 := durable.Continue(s.BookFlight, FlightBooking{
        TripID: input.TripID, FlightID: input.FlightID,
    })
    step1 := durable.Then(step0, s.BookHotel)
    step2 := durable.Then(step1, s.BookCar)
    return step2, nil
}

// BookHotel's RecoveryHandler — has FlightBookedInput with flight confirmation
func (s *TripService) CompensateHotel(ctx *durable.Context, input FlightBookedInput, err error) (*durable.Continuation[HotelResult], error) {
    s.cancelFlight(ctx, input.FlightConfirmation)
    // Return error to abort the Then pipeline:
    return nil, fmt.Errorf("hotel booking failed, flight cancelled: %w", err)
    // OR return Done to continue the pipeline with a compensated result:
    // return durable.Done(HotelResult{Status: "compensated", ...}), nil
}

// Registration:
durable.RegisterHandler(w, svc.BookHotel, opts).
    WithRecoveryHandler(svc.CompensateHotel, opts)
```

The RecoveryHandler decides: return `(cont, nil)` to continue the pipeline, or `(nil, err)` to abort it. It has full access to the failed step's typed input for compensation.

## Explored alternatives

### ThenCatch / Catch combinators

We explored adding `ThenCatch(a, onSuccess, onError, errorInput)` and `Catch(a, onError, errorInput)` with per-level error handlers at the composition level.

**Rejected because:**
- The error handler's `errorInput` is captured at construction time (before the pipeline runs), so it cannot include intermediate results from prior pipeline steps. For saga compensation, this means the error handler lacks the context needed to compensate (e.g., flight confirmation numbers).
- RecoveryHandler already solves per-handler error recovery with the right input type. Adding composition-level error handlers creates two overlapping mechanisms.
- The simpler model (Then + RecoveryHandler + TerminalError) covers the same use cases without new handler types or thenStack entry complexity.

### Removing RecoveryHandler entirely

We explored replacing RecoveryHandler with composition-level error handling only.

**Rejected because:**
- The composition-level error handler cannot receive the failed step's typed input (the "input gap" — see above). RecoveryHandler's key advantage is type-safe access to the step's accumulated state.
- RecoveryHandler and Then are complementary: RecoveryHandler handles per-handler recovery, Then handles pipeline composition. They don't conflict.

### Pipeline / Promise (separate routines)

We explored making Then compose separate durable routines rather than handlers within a single routine.

**Deferred because:**
- Requires a new `AwaitChild` continuation primitive (wait for a child routine's result).
- Each pipeline step becomes a separate Temporal workflow — heavier execution model.
- Can be built later on top of Then if the use case arises.

## Decision

**Then + TerminalError.** RecoveryHandler stays. The total API addition is:

```go
func Then[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U]
func TerminalError(err error) error
func IsTerminal(err error) bool
```
