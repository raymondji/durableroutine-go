# Chain and TerminalError — Sequential Continuation Composition

## Problem

Every suspension point in a durable routine requires a separately named handler function and input type, all returning `*Continuation[T]` with the same final result type `T`. This couples intermediate steps to the routine's result type and prevents reusing sub-chains across routines with different result types.

## Proposal

Two additions:

```go
// Chain: sequential composition. When inner's chain reaches Done(result T),
// pass result as input to handler, which returns Continuation[U].
// If inner's chain fails (error), the routine fails — handler never runs.
func Chain[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U]

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
  ├─ returns (cont, nil)            → routine continues (Chain chain continues if cont is Done)
  ├─ returns (nil, regular error)   → retry TE per its RetryPolicy → exhausted → routine fails
  └─ returns (nil, TerminalError) → skip TE retries → routine fails
```

### Interaction with Chain

Given `Chain(a, b)` where a handler in `a`'s chain fails:

1. RecoveryHandler fires (if registered). It has the failed step's exact input type.
2. If RecoveryHandler returns `(cont, nil)` → chain continues. If `cont` reaches Done → **b runs with the Done result**.
3. If RecoveryHandler returns `(nil, err)` → **routine fails. b never runs.** chainStack is discarded.
4. If no RecoveryHandler is registered → **routine fails. b never runs.**

The RecoveryHandler is the decision point: recover (return continuation) or abort (return error). Chain respects that decision. No ambiguity — the developer explicitly chooses whether the pipeline continues or stops.

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

## Design: per-case chainStack

Each `Case` carries a `chainStack []string` — handler keys to invoke sequentially when the case's handler chain reaches Done.

```go
type Case struct {
    timerDuration *time.Duration
    sendName      string
    callName      string
    immediate     bool
    input         any
    handlerKey    string
    chainStack     []string  // NEW: handler keys for Chain chain
}
```

### How Chain builds the chainStack

Chain copies the inner continuation's cases and appends the new handler's key to each case's chainStack:

```go
func Chain[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U] {
    if inner.IsDone() {
        return Continue(handler, inner.Result())  // short-circuit
    }
    var zeroT T
    var zeroU U
    chainKey := durablecore.HandlerKey(zeroT.DurableKind(), zeroU.DurableKind())
    newCases := make([]Case, len(inner.cases))
    for i, c := range inner.cases {
        newCases[i] = c
        newCases[i].chainStack = append(append([]string{}, c.chainStack...), chainKey)
    }
    return &Continuation[U]{cases: newCases}
}
```

### Trampoline behavior

The trampoline maintains a `chainStack []string` variable:

```
case fires → set chainStack = firedCase.chainStack

loop:
    output = runHandler(handlerKey, input, msg)

    if handler error (after retries + RecoveryHandler):
        // routine fails — chainStack discarded
        return error

    if output.Done:
        if len(chainStack) > 0:
            handlerKey = chainStack[0]
            chainStack = chainStack[1:]
            input = output.Result    // Done result becomes input to next handler
            msg = nil
            continue
        return output.Result

    // Non-Done: wait for next case
    wait for case → extract handlerKey, input, msg from fired case
    // Combine fired case's chainStack with current outer chainStack:
    chainStack = firedCase.chainStack + existingChainStack
```

**chainStack propagation:** When a handler inside a Chain chain returns non-Done cases, those cases may carry their own chainStacks (from nested Chain). When a new case fires, the trampoline combines the new case's chainStack with the existing outer chainStack by prepending: `newCaseChainStack + outerChainStack`.

## Composition with Select

Per-case chainStacks compose naturally with Select. Select merges cases, each preserving its own chainStack. No restrictions needed.

```go
// a's cases get chainStack [h1], b's cases get chainStack []
Select(Chain(a, h1), b)

// a's cases get chainStack [h1], b's cases get chainStack [h2]
Select(Chain(a, h1), Chain(b, h2))

// All cases get chainStack [h1]
Chain(Select(a, b), h1)

// Nested Select flattens as before
Select(Select(a, b), Select(c, d))  ==  Select(a, b, c, d)

// Nested Chain accumulates the stack
Chain(Chain(a, h1), h2)  →  a's cases get chainStack [h1, h2]
```

## No concurrency introduced

Chain does not introduce concurrency. At every point, exactly one handler runs. The chainStack is a return address — when Done is reached, the next handler runs sequentially. Select picks one winner; other branches are discarded.

The goroutine analogy holds: a goroutine's sequential code after a function call maps to Chain's "after Done, run the next handler."

## Example: multi-Chain happy path

```go
func (s *TripService) StartTrip(ctx *durable.Context, input TripBooking) (*durable.Continuation[TripResult], error) {
    step0 := durable.Continue(s.BookFlight, FlightBooking{
        TripID: input.TripID, FlightID: input.FlightID,
    })                                                        // *Continuation[FlightResult]
    step1 := durable.Chain(step0, s.BookHotel)                  // *Continuation[HotelResult]
    step2 := durable.Chain(step1, s.BookCar)                    // *Continuation[TripResult]
    return step2, nil
}
```

Each handler has its own result type. BookFlight returns `*Continuation[FlightResult]`, BookHotel returns `*Continuation[HotelResult]`, BookCar returns `*Continuation[TripResult]`. Chain bridges between them.

## Example: Chain with RecoveryHandler (saga)

```go
func (s *TripService) StartTrip(ctx *durable.Context, input TripBooking) (*durable.Continuation[TripResult], error) {
    step0 := durable.Continue(s.BookFlight, FlightBooking{
        TripID: input.TripID, FlightID: input.FlightID,
    })
    step1 := durable.Chain(step0, s.BookHotel)
    step2 := durable.Chain(step1, s.BookCar)
    return step2, nil
}

// BookHotel's RecoveryHandler — has FlightBookedInput with flight confirmation
func (s *TripService) CompensateHotel(ctx *durable.Context, input FlightBookedInput, err error) (*durable.Continuation[HotelResult], error) {
    s.cancelFlight(ctx, input.FlightConfirmation)
    // Return error to abort the Chain pipeline:
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

### ChainCatch / Catch combinators

We explored adding `ChainCatch(a, onSuccess, onError, errorInput)` and `Catch(a, onError, errorInput)` with per-level error handlers at the composition level.

**Rejected because:**
- The error handler's `errorInput` is captured at construction time (before the pipeline runs), so it cannot include intermediate results from prior pipeline steps. For saga compensation, this means the error handler lacks the context needed to compensate (e.g., flight confirmation numbers).
- RecoveryHandler already solves per-handler error recovery with the right input type. Adding composition-level error handlers creates two overlapping mechanisms.
- The simpler model (Chain + RecoveryHandler + TerminalError) covers the same use cases without new handler types or chainStack entry complexity.

### Removing RecoveryHandler entirely

We explored replacing RecoveryHandler with composition-level error handling only.

**Rejected because:**
- The composition-level error handler cannot receive the failed step's typed input (the "input gap" — see above). RecoveryHandler's key advantage is type-safe access to the step's accumulated state.
- RecoveryHandler and Chain are complementary: RecoveryHandler handles per-handler recovery, Chain handles pipeline composition. They don't conflict.

### Pipeline / Promise (separate routines)

We explored making Chain compose separate durable routines rather than handlers within a single routine.

**Deferred because:**
- Requires a new `AwaitChild` continuation primitive (wait for a child routine's result).
- Each pipeline step becomes a separate Temporal workflow — heavier execution model.
- Can be built later on top of Chain if the use case arises.

## Concrete comparisons: with Chain vs. without

Chain introduces a second way to chain handlers. The existing `Continue` embeds sequencing inside each handler; `Chain` lifts sequencing into the caller. The question is whether that trade-off pays for itself. Below are several realistic examples compared side-by-side.

### Example A: Trip saga (linear pipeline)

The existing saga example. Three sequential bookings with compensation on failure.

**Without Chain (current):**

```go
// Every handler returns *Continuation[TripResult] and explicitly calls Continue to the next step.

func (s *TripService) BookFlight(ctx *durable.Context, input TripInput) (*durable.Continuation[TripResult], error) {
    flightConf, err := s.bookFlight(ctx, input.FlightID)
    if err != nil { return nil, err }
    return durable.Continue(s.BookHotel, FlightBookedInput{
        TripID: input.TripID, HotelID: input.HotelID, CarRentalID: input.CarRentalID,
        FlightConfirmation: flightConf,
    }), nil
}

func (s *TripService) BookHotel(ctx *durable.Context, input FlightBookedInput) (*durable.Continuation[TripResult], error) {
    hotelConf, err := s.bookHotel(ctx, input.HotelID)
    if err != nil { return nil, err }
    return durable.Continue(s.BookCar, HotelBookedInput{
        TripID: input.TripID, CarRentalID: input.CarRentalID,
        FlightConfirmation: input.FlightConfirmation, HotelConfirmation: hotelConf,
    }), nil
}

func (s *TripService) BookCar(ctx *durable.Context, input HotelBookedInput) (*durable.Continuation[TripResult], error) {
    carConf, err := s.bookCar(ctx, input.CarRentalID)
    if err != nil { return nil, err }
    return durable.Done(TripResult{
        FlightConfirmation: input.FlightConfirmation,
        HotelConfirmation:  input.HotelConfirmation,
        CarConfirmation:    carConf,
    }), nil
}

// Entry point: just starts the first step.
func (s *TripService) StartTrip(ctx *durable.Context, input TripInput) (*durable.Continuation[TripResult], error) {
    return durable.Continue(s.BookFlight, input), nil
}
```

**With Chain:**

```go
// Each handler returns its own result type and calls Done — doesn't know what comes next.
// But the result must carry ALL state the next handler needs.

func (s *TripService) BookFlight(ctx *durable.Context, input TripInput) (*durable.Continuation[FlightBookedInput], error) {
    flightConf, err := s.bookFlight(ctx, input.FlightID)
    if err != nil { return nil, err }
    return durable.Done(FlightBookedInput{
        TripID: input.TripID, HotelID: input.HotelID, CarRentalID: input.CarRentalID,
        FlightConfirmation: flightConf,
    }), nil
}

func (s *TripService) BookHotel(ctx *durable.Context, input FlightBookedInput) (*durable.Continuation[HotelBookedInput], error) {
    hotelConf, err := s.bookHotel(ctx, input.HotelID)
    if err != nil { return nil, err }
    return durable.Done(HotelBookedInput{
        TripID: input.TripID, CarRentalID: input.CarRentalID,
        FlightConfirmation: input.FlightConfirmation, HotelConfirmation: hotelConf,
    }), nil
}

func (s *TripService) BookCar(ctx *durable.Context, input HotelBookedInput) (*durable.Continuation[TripResult], error) {
    carConf, err := s.bookCar(ctx, input.CarRentalID)
    if err != nil { return nil, err }
    return durable.Done(TripResult{
        FlightConfirmation: input.FlightConfirmation,
        HotelConfirmation:  input.HotelConfirmation,
        CarConfirmation:    carConf,
    }), nil
}

// Entry point: declares the full pipeline structure.
func (s *TripService) StartTrip(ctx *durable.Context, input TripInput) (*durable.Continuation[TripResult], error) {
    step0 := durable.Continue(s.BookFlight, input)
    step1 := durable.Chain(step0, s.BookHotel)
    step2 := durable.Chain(step1, s.BookCar)
    return step2, nil
}
```

**Observations:**
- The handler bodies are nearly identical. `Continue(s.BookHotel, ...)` becomes `Done(FlightBookedInput{...})`. Same fields, same struct.
- The key structural difference: each handler's result type changes from `TripResult` to its own output type (`FlightBookedInput`, `HotelBookedInput`). But these "result types" are really just the next handler's input — they carry the same accumulated state.
- The pipeline is visible in one place (`StartTrip`) vs. embedded across handlers. Whether that's clearer is debatable — the handlers are already named `BookFlight → BookHotel → BookCar`, and `Continue(s.BookHotel, ...)` makes the next step obvious.
- Registration is the same either way. RecoveryHandlers work the same.

**Verdict: Marginal.** Chain moves the sequencing into the entry point, but the handlers do essentially the same work. The "result type decoupling" doesn't save anything because the intermediate results must carry forward the same accumulated state.

### Example B: Reusable identity verification sub-chain

An identity verification flow (send code, wait for response, validate) reused across multiple routines with different result types.

**Without Chain:**

```go
// Can't reuse — the handlers are parameterized by the routine's result type.
// You'd need separate handler sets for each routine that uses verification.

// For AccountCreation routine:
func (s *IdentityService) SendCodeForAccount(ctx *durable.Context, input VerifyForAccountInput) (*durable.Continuation[AccountResult], error) {
    s.sendSMS(input.Phone, generateCode())
    return durable.ReceiveSend(s.CheckCodeForAccount, WaitingForAccountInput{Phone: input.Phone}), nil
}
func (s *IdentityService) CheckCodeForAccount(ctx *durable.Context, input WaitingForAccountInput, code VerifyCode) (*durable.Continuation[AccountResult], error) {
    if !s.validCode(input.Phone, code.Code) {
        return durable.ReceiveSend(s.CheckCodeForAccount, input), nil // retry
    }
    return durable.Continue(accountSvc.CreateAccount, CreateAccountInput{Phone: input.Phone, Verified: true}), nil
}

// For PasswordReset routine — same logic, different result type:
func (s *IdentityService) SendCodeForReset(ctx *durable.Context, input VerifyForResetInput) (*durable.Continuation[ResetResult], error) {
    s.sendSMS(input.Phone, generateCode())
    return durable.ReceiveSend(s.CheckCodeForReset, WaitingForResetInput{Phone: input.Phone}), nil
}
func (s *IdentityService) CheckCodeForReset(ctx *durable.Context, input WaitingForResetInput, code VerifyCode) (*durable.Continuation[ResetResult], error) {
    if !s.validCode(input.Phone, code.Code) {
        return durable.ReceiveSend(s.CheckCodeForReset, input), nil // retry
    }
    return durable.Continue(resetSvc.DoReset, DoResetInput{Phone: input.Phone}), nil
}

// Two separate handler sets, two separate registrations, duplicated verification logic.
```

**With Chain:**

```go
// Verification is written once with its own result type.
func (s *IdentityService) SendCode(ctx *durable.Context, input VerifyInput) (*durable.Continuation[VerifiedIdentity], error) {
    s.sendSMS(input.Phone, generateCode())
    return durable.ReceiveSend(s.CheckCode, WaitingInput{Phone: input.Phone}), nil
}
func (s *IdentityService) CheckCode(ctx *durable.Context, input WaitingInput, code VerifyCode) (*durable.Continuation[VerifiedIdentity], error) {
    if !s.validCode(input.Phone, code.Code) {
        return durable.ReceiveSend(s.CheckCode, input), nil // retry
    }
    return durable.Done(VerifiedIdentity{Phone: input.Phone}), nil
}

// Account creation routine — reuses the verification chain.
func (s *AccountService) StartAccountCreation(ctx *durable.Context, input SignupInput) (*durable.Continuation[AccountResult], error) {
    verify := durable.Continue(identitySvc.SendCode, VerifyInput{Phone: input.Phone})
    return durable.Chain(verify, s.CreateAccount), nil
}

// Password reset routine — reuses the same verification chain.
func (s *ResetService) StartPasswordReset(ctx *durable.Context, input ResetInput) (*durable.Continuation[ResetResult], error) {
    verify := durable.Continue(identitySvc.SendCode, VerifyInput{Phone: input.Phone})
    return durable.Chain(verify, s.DoReset), nil
}

// One handler set for verification, registered once. Used by multiple routines.
```

**Observations:**
- Without Chain, the identity verification logic must be duplicated for each routine that uses it, because the `Continuation[T]` result type `T` is fixed. Each copy has different input/state types too (to match the result type), even though the logic is identical.
- With Chain, the verification chain is written once with its own natural result type (`VerifiedIdentity`). Any routine can `Chain` it into their pipeline.
- The verification chain includes a suspension point (`ReceiveSend` — waiting for the user to enter the code). This is the key difference from a simple helper function — it's a durable sub-chain with its own wait/resume cycle.

**Verdict: Clear win for Chain.** Reusable sub-chains with suspension points are the primary use case.

### Example C: Booking flow (event-driven state machine)

The existing booking example. Each step waits for one of several events (payment, cancellation, timeout).

**Without Chain (current):**

```go
func (s *BookingService) ReserveItem(ctx *durable.Context, input BookingInput) (*durable.Continuation[BookingResult], error) {
    reserved := ReservedInput{UserID: input.UserID, ItemID: input.ItemID}
    return durable.Select(
        durable.ReceiveSend(s.ProcessPayment, reserved),
        durable.ReceiveCall(s.CancelBooking, reserved),
        durable.After(15*time.Minute, s.ExpireReservation, reserved),
    ), nil
}
```

**With Chain:** Chain doesn't apply here. Each handler returns a Select with multiple possible next steps. The next handler depends on which event fires — it's not a linear pipeline.

**Verdict: Chain doesn't apply.** Event-driven branching is the domain of Select, not Chain.

### Example D: Onboarding (linear setup, then event-driven)

A user onboarding routine: (1) provision account, (2) set up billing, (3) enter an event loop waiting for the user to activate or the trial to expire.

**Without Chain:**

```go
// All handlers return *Continuation[OnboardingResult].

func (s *OnboardingSvc) ProvisionAccount(ctx *durable.Context, input SignupInput) (*durable.Continuation[OnboardingResult], error) {
    accountID, err := s.createAccount(ctx, input.Email)
    if err != nil { return nil, err }
    return durable.Continue(s.SetupBilling, BillingInput{AccountID: accountID, Plan: input.Plan}), nil
}

func (s *OnboardingSvc) SetupBilling(ctx *durable.Context, input BillingInput) (*durable.Continuation[OnboardingResult], error) {
    err := s.createSubscription(ctx, input.AccountID, input.Plan)
    if err != nil { return nil, err }
    return durable.Select(
        durable.ReceiveSend(s.Activate, WaitingInput{AccountID: input.AccountID}),
        durable.After(14*24*time.Hour, s.ExpireTrial, WaitingInput{AccountID: input.AccountID}),
    ), nil
}

func (s *OnboardingSvc) Activate(ctx *durable.Context, input WaitingInput, _ ActivateMsg) (*durable.Continuation[OnboardingResult], error) {
    return durable.Done(OnboardingResult{AccountID: input.AccountID, Status: "active"}), nil
}

func (s *OnboardingSvc) ExpireTrial(ctx *durable.Context, input WaitingInput) (*durable.Continuation[OnboardingResult], error) {
    return durable.Done(OnboardingResult{AccountID: input.AccountID, Status: "expired"}), nil
}
```

**With Chain:**

```go
// ProvisionAccount returns its own result type. SetupBilling enters the event loop.

func (s *OnboardingSvc) ProvisionAccount(ctx *durable.Context, input SignupInput) (*durable.Continuation[ProvisionedAccount], error) {
    accountID, err := s.createAccount(ctx, input.Email)
    if err != nil { return nil, err }
    return durable.Done(ProvisionedAccount{AccountID: accountID, Plan: input.Plan}), nil
}

func (s *OnboardingSvc) SetupBilling(ctx *durable.Context, input ProvisionedAccount) (*durable.Continuation[OnboardingResult], error) {
    err := s.createSubscription(ctx, input.AccountID, input.Plan)
    if err != nil { return nil, err }
    return durable.Select(
        durable.ReceiveSend(s.Activate, WaitingInput{AccountID: input.AccountID}),
        durable.After(14*24*time.Hour, s.ExpireTrial, WaitingInput{AccountID: input.AccountID}),
    ), nil
}

// Entry point:
func (s *OnboardingSvc) StartOnboarding(ctx *durable.Context, input SignupInput) (*durable.Continuation[OnboardingResult], error) {
    provision := durable.Continue(s.ProvisionAccount, input)
    return durable.Chain(provision, s.SetupBilling), nil
}

// Activate and ExpireTrial are the same as before.
```

**Observations:**
- The Chain version is slightly cleaner: ProvisionAccount doesn't need to know that SetupBilling comes next, and the entry point shows the structure.
- But the benefit is small. ProvisionAccount with Continue is already clear — `Continue(s.SetupBilling, ...)` reads naturally.
- If ProvisionAccount were a reusable sub-chain used by multiple routines, Chain would be a bigger win. But provisioning is typically specific to onboarding.
- SetupBilling and the event-loop handlers are identical in both versions.

**Verdict: Marginal.** Slight readability improvement in the entry point, but not enough to justify a new concept on its own.

### Summary

| Scenario | Chain benefit |
|---|---|
| Linear pipeline (saga) | Marginal — moves sequencing to entry point, but handlers do the same work |
| Reusable sub-chain with suspension points | Clear win — eliminates duplication when the same sub-chain is used across multiple routines |
| Event-driven state machine | Not applicable — Select handles branching |
| Mixed linear + event-driven | Marginal — small readability improvement |

**Minor benefit: readability.** Chain lifts the pipeline structure into the entry point, so you can see the sequence in one place instead of following `Continue` calls across handlers. But `Continue(s.BookHotel, ...)` already reads clearly, so this is incremental.

**Major benefit: composability.** Chain decouples a handler's result type from the routine's final result type. This enables writing a sub-chain once (with its own suspension points, waits, retries) and reusing it across multiple routines that have different result types. Without Chain, you'd duplicate the entire sub-chain for each routine, or resort to a heavier child-routine approach. The identity verification example above demonstrates this clearly.

For simple linear pipelines or event-driven flows, the current `Continue` approach works equally well and is simpler because there's only one way to chain handlers.

### Does Chain reduce per-handler state? (No — for sagas)

Initial intuition: "BookFlight only needs FlightID and returns FlightConfirmation, so it doesn't need to carry HotelID and CarRentalID for downstream handlers." But this breaks down when compensation is involved.

**The problem:** In `Chain(Chain(Continue(BookFlight, ...), BookHotel), BookCar)`, Chain connects output→input: BookHotel's input IS BookFlight's output. If BookHotel fails, its RecoveryHandler needs the flight confirmation to cancel it. So BookFlight's output must carry the flight confirmation. And CompensateCar needs both flight and hotel confirmations — so BookHotel's output must carry both forward. The state accumulation is the same as without Chain.

**Root cause: the serialization boundary.** In regular code, closures implicitly capture prior results (`flightConf` is just a local variable). In a durable system, closures can't cross suspension points. Any data needed for compensation must be explicitly serialized in handler input/output types.

**Explored alternatives:**

| Approach | State reduction | Type safe? | Complexity | Notes |
|---|---|---|---|---|
| Chain as-is | None | Yes | Low | Result types accumulate same as Continue |
| Chain + extra data param | Partial (static config only) | Yes | Medium | New `ChainHandler[T, Extra, U]` type. Entry point provides static config (IDs known upfront) via `extra`. Dynamic results (confirmations from prior steps) still accumulate through the result chain. |
| Runtime chain history | Full | No (runtime lookups) | High | Runtime saves each step's input/output. RecoveryHandlers query history by type. Breaks type safety. |
| Per-handler compensation registration | Full | Yes | High | Each handler calls `RegisterCompensation(ctx, handler, input)`. Runtime runs compensations in reverse on failure. Different saga model — bigger change than Chain. |

**Conclusion:** For sagas with compensation, state accumulation is inherent to the durability constraint. You can shuffle where the data lives, but the total data that must be serialized is the same. The approaches that achieve full separation (runtime history, compensation registration) are substantially more complex and represent different design patterns, not just Chain improvements.

Chain's state-reduction benefit is real only for sub-chains that don't participate in compensation — like the identity verification example, where each handler's output is genuinely minimal.

## Decision

**Chain + TerminalError.** RecoveryHandler stays. The total API addition is:

```go
func Chain[T, U Payload](inner *Continuation[T], handler Handler[T, U]) *Continuation[U]
func TerminalError(err error) error
func IsTerminal(err error) bool
```
