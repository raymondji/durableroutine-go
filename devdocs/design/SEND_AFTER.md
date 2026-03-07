# SendAfter — Delayed Self-Send Timers

## Problem

The current `After` is a case inside `Select` — it races other cases. This makes multiple concurrent timers awkward. Consider a provider routine managing several reservations, each with its own 15-minute expiry:

```go
// Current approach: single nearest-deadline timer, recalculated on every handler invocation
func (s *Svc) WaitForActivity(ctx *durable.Context, state ProviderState) (*durable.Continuation[Unit], error) {
    conts := []*durable.Continuation[Unit]{
        durable.ReceiveSend(s.HandleReserve, state),
        durable.ReceiveSend(s.HandleConfirmAndPay, state),
    }
    if nearest := state.NearestDeadline(); nearest != nil {
        conts = append(conts, durable.After(time.Until(nearest.Deadline), s.HandleExpire, state))
    }
    return durable.Select(conts...), nil
}
```

This works but requires manual deadline tracking, nearest-timer recalculation on every handler return, and a single `HandleExpire` that checks `time.Now()` against all deadlines.

## Go equivalent

In native Go, a single goroutine processing a channel-based event loop uses `time.AfterFunc` to schedule self-messages:

```go
func (s *Server) run() {
    for {
        select {
        case msg := <-s.reserveCh:
            timer := time.AfterFunc(15*time.Minute, func() {
                s.expireCh <- ExpireMsg{SlotID: msg.SlotID}
            })
            s.timers[msg.SlotID] = timer

        case msg := <-s.confirmCh:
            if t, ok := s.timers[msg.SlotID]; ok {
                t.Stop()
                delete(s.timers, msg.SlotID)
            }
            s.bookSlot(msg)

        case msg := <-s.expireCh:
            if _, ok := s.state.Reservations[msg.SlotID]; ok {
                s.releaseSlot(msg.SlotID)
            }
            delete(s.timers, msg.SlotID)
        }
    }
}
```

Key properties:
- `time.AfterFunc` schedules a deferred message to a channel — it doesn't compete in the current `select`.
- `timer.Stop()` cancels the timer. But there's a race: the callback may have already fired and the message may already be in the channel. Handlers must still validate against current state.
- The expire message arrives on `expireCh`, processed by the same `select` loop as external messages. No special handling needed.

Elixir's GenServer has the equivalent `Process.send_after/3` + `Process.cancel_timer/1`.

## Proposal

```go
// SendAfter schedules a BufferSend to the current routine after the given
// duration. This is exactly a normal send, but delayed — the message arrives
// through the signal channel like any other send, processed by a
// ReceiveSend case that matches E's DurableKind.
//
// The handler parameter follows the same convention as BufferSend: it is
// used only for type inference of I, E, and T — it is not called.
//
// Returns a TimerToken that can be passed to CancelTimer to cancel the
// scheduled send. Cancellation is best-effort: if the timer has already
// fired, the message may still arrive. Handlers should validate messages
// against current state.
//
// Maps to time.AfterFunc(d, func() { ch <- msg }) in native Go.
// Maps to Process.send_after/3 in Elixir.
func SendAfter[I Payload, E Payload, T Payload](
    ctx *Context, d time.Duration, handler SendHandler[I, E, T], msg E,
) TimerToken

// CancelTimer cancels a previously scheduled SendAfter timer. If the timer
// has already fired, this is a no-op — the message may still be delivered.
// Maps to (*time.Timer).Stop() in native Go.
// Maps to Process.cancel_timer/1 in Elixir.
func CancelTimer(ctx *Context, token TimerToken)
```

The handler parameter serves the same purpose as in `BufferSend` — it provides
the full `SendHandler[I, E, T]` signature so the library can derive the correct
signal routing key (`send:{I.DurableKind()}:{E.DurableKind()}:{T.DurableKind()}`).
The handler itself is never called by `SendAfter`.

`TimerToken` is an opaque value that can be stored in handler state (it must implement `Payload` so it can be serialized across handler boundaries).

```go
// TimerToken identifies a scheduled SendAfter timer. Store it in your
// state to cancel the timer later.
type TimerToken string

func (TimerToken) DurableKind() string { return "durable.TimerToken" }
```

## Example: provider with concurrent reservation timers

```go
type ProviderState struct {
    ProviderID   string
    Reservations map[string]Reservation
}

type Reservation struct {
    UserID     string
    SlotID     string
    TimerToken durable.TimerToken
}

type ExpireMsg struct{ SlotID string; UserID string }
func (ExpireMsg) DurableKind() string { return "expire-reservation" }

type ConfirmAndPay struct{ SlotID string; UserID string; PaymentID string }
func (ConfirmAndPay) DurableKind() string { return "confirm-and-pay" }

// --- Handlers ---

func (s *Svc) WaitForActivity(ctx *durable.Context, state ProviderState) (*durable.Continuation[Unit], error) {
    return durable.Select(
        durable.ReceiveSend(s.HandleReserve, state),
        durable.ReceiveSend(s.HandleConfirmAndPay, state),
        durable.ReceiveSend(s.HandleExpire, state),
    ), nil
}

func (s *Svc) HandleReserve(ctx *durable.Context, state ProviderState, msg ReserveSlot) (*durable.Continuation[Unit], error) {
    token := durable.SendAfter(ctx, 15*time.Minute, s.HandleExpire, ExpireMsg{
        SlotID: msg.SlotID, UserID: msg.UserID,
    })
    state.Reservations[msg.SlotID] = Reservation{
        UserID:     msg.UserID,
        SlotID:     msg.SlotID,
        TimerToken: token,
    }
    return durable.Continue(s.WaitForActivity, state), nil
}

func (s *Svc) HandleConfirmAndPay(ctx *durable.Context, state ProviderState, msg ConfirmAndPay) (*durable.Continuation[Unit], error) {
    res, ok := state.Reservations[msg.SlotID]
    if !ok || res.UserID != msg.UserID {
        // Stale or invalid — reject
        return durable.Continue(s.WaitForActivity, state), nil
    }
    durable.CancelTimer(ctx, res.TimerToken)
    state.BookSlot(msg.SlotID, msg.UserID, msg.PaymentID)
    delete(state.Reservations, msg.SlotID)
    return durable.Continue(s.WaitForActivity, state), nil
}

func (s *Svc) HandleExpire(ctx *durable.Context, state ProviderState, msg ExpireMsg) (*durable.Continuation[Unit], error) {
    res, ok := state.Reservations[msg.SlotID]
    if !ok || res.UserID != msg.UserID {
        // Already confirmed or already expired — stale timer, ignore
        return durable.Continue(s.WaitForActivity, state), nil
    }
    state.ReleaseSlot(msg.SlotID)
    delete(state.Reservations, msg.SlotID)
    return durable.Continue(s.WaitForActivity, state), nil
}
```

No nearest-deadline tracking. No Select reconstruction. Multiple timers run concurrently, each identified by its token.

## TimerToken design

The token must be serializable (it crosses handler boundaries via state) and usable as a map key.

A simple approach: generate a unique string per `SendAfter` call. The context tracks a counter:

```go
type TimerToken string

func (TimerToken) DurableKind() string { return "durable.TimerToken" }
```

Inside `SendAfter`:
```go
func SendAfter[I Payload, E Payload, T Payload](
    ctx *Context, d time.Duration, handler SendHandler[I, E, T], msg E,
) TimerToken {
    var zeroI I
    var zeroT T
    ctx.timerSeq++
    token := TimerToken(fmt.Sprintf("timer-%d", ctx.timerSeq))
    ctx.sendAfterRequests = append(ctx.sendAfterRequests, sendAfterRequest{
        token:    token,
        duration: d,
        msg:      msg,
        // Same signal key derivation as BufferSend
        inputKind:        zeroI.DurableKind(),
        externalInputKind: msg.DurableKind(),
        resultKind:       zeroT.DurableKind(),
    })
    return token
}
```

`CancelTimer` buffers a cancel request:
```go
func CancelTimer(ctx *Context, token TimerToken) {
    ctx.cancelTimerRequests = append(ctx.cancelTimerRequests, token)
}
```

Both are buffered side effects, flushed after the handler returns — same as `BufferStart` and `BufferSend`.

## Temporal backend implementation

SendAfter is implemented as a detached workflow goroutine that sleeps and then sends a signal to itself.

After processing handler side effects (step 4 in the workflow loop), add a new step for SendAfter requests:

```go
// 4b. Handle SendAfter requests — start a goroutine per timer.
for _, af := range output.SendAfterRequests {
    af := af
    workflow.Go(ctx, func(gCtx workflow.Context) {
        // Sleep for the timer duration. Uses a cancellable context
        // so CancelTimer can stop it.
        timerCtx, cancel := workflow.WithCancel(gCtx)
        // Store cancel func so CancelTimer can reach it.
        pendingTimers[af.Token] = cancel

        err := workflow.Sleep(timerCtx, af.Duration)
        delete(pendingTimers, af.Token)
        if err != nil {
            return // cancelled
        }

        // Deliver as a self-signal.
        signalName := af.SignalName // "send:{inputKind}:{msgKind}:{resultKind}"
        workflow.SignalExternalWorkflow(gCtx, selfWorkflowID, "", signalName, af.Msg)
    })
}
```

For CancelTimer requests:
```go
// 4c. Handle CancelTimer requests.
for _, token := range output.CancelTimerRequests {
    if cancel, ok := pendingTimers[token]; ok {
        cancel()
        delete(pendingTimers, token)
    }
}
```

### Continue-as-new considerations

On continue-as-new, pending timer goroutines are lost. The workflow must persist pending timers and re-create them on restart:

```go
type WorkflowInput struct {
    // ... existing fields ...
    PendingTimers []PendingTimerEntry `json:"pendingTimers,omitempty"`
}

type PendingTimerEntry struct {
    Token      string          `json:"token"`
    Deadline   time.Time       `json:"deadline"` // absolute, not duration
    SignalName string          `json:"signalName"`
    Msg        json.RawMessage `json:"msg"`
}
```

On restart, recompute remaining durations from absolute deadlines and re-create goroutines. Timers with deadlines in the past fire immediately.

### Self-signal vs internal channel

Alternative: instead of `SignalExternalWorkflow` (which goes through Temporal's signal infrastructure), use an internal `workflow.Channel` that the main select loop receives from.

Tradeoffs:
- **Internal channel**: Lower overhead, no extra signal events in history. But requires the main selector to add a receive case for the timer channel, and timer messages bypass the signal buffering mechanism.
- **Self-signal**: Higher overhead (extra signal event in history), but timer messages arrive through the same signal channel as external sends. The main select loop handles them identically — no special code paths. Simpler to implement and reason about.

**Recommendation: self-signal.** The overhead is minimal (one extra event per timer fire), and the simplicity of "timer messages are just signals" is worth it. This directly mirrors the Go pattern where `time.AfterFunc` pushes to a channel that the `select` loop already reads.

## In-memory backend implementation

The runtime spawns a goroutine per SendAfter that sleeps and then pushes to the instance's `sendCh`:

```go
// In applyContextEffects:
for _, af := range sctx.SendAfterRequests() {
    af := af
    timer := time.AfterFunc(af.Duration, func() {
        inst.sendCh <- sendMsg{key: af.SignalName, msg: af.Msg}
    })
    inst.mu.Lock()
    inst.pendingTimers[af.Token] = timer
    inst.mu.Unlock()
}

for _, token := range sctx.CancelTimerRequests() {
    inst.mu.Lock()
    if t, ok := inst.pendingTimers[token]; ok {
        t.Stop()
        delete(inst.pendingTimers, token)
    }
    inst.mu.Unlock()
}
```

No continue-as-new concerns for the in-memory backend.

## Wire format

New fields on `ActivityOutput`:

```go
type ActivityOutput struct {
    // ... existing fields ...
    SendAfterRequests    []SendAfterEntry `json:"sendAfterRequests,omitempty"`
    CancelTimerRequests  []string         `json:"cancelTimerRequests,omitempty"`
}

type SendAfterEntry struct {
    Token      string          `json:"token"`
    Duration   time.Duration   `json:"duration"`
    SignalName string          `json:"signalName"`
    Msg        json.RawMessage `json:"msg"`
}
```

## Relationship to After

`After` and `SendAfter` are complementary:

- **`After(d, handler, input)`** — a case in `Select`. "Wait for this duration OR another event, whichever comes first." Used for simple timeouts in linear workflows. The timer competes with other cases.
- **`SendAfter(ctx, d, handler, msg)`** — a side effect. "Do a normal send to myself, but after a delay." The delayed send arrives through the signal channel and is processed by a future `ReceiveSend` case. Used for independent timers in GenServer-style event loops.

| | `After` | `SendAfter` |
|---|---|---|
| Go equivalent | `case <-time.After(d):` in select | `time.AfterFunc(d, func() { ch <- msg })` |
| Elixir equivalent | `after` clause in `receive` | `Process.send_after/3` |
| Cancelable | No (abandoned when Select picks another case) | Yes (via `CancelTimer`) |
| Multiple concurrent | Awkward (nearest-deadline tracking) | Natural (each is independent) |
| Use case | Simple timeout/sleep | GenServer-style concurrent timers |

## Handler validation requirement

Like Go's `time.AfterFunc`, cancellation is best-effort. Even with `CancelTimer`, the message may already be in the signal queue. **Handlers must validate timer messages against current state.** This is not a limitation — it's the same discipline required in native Go when `timer.Stop()` returns false.

## Registration

The timer message type `E` must have a `RegisterSendHandler` so the signal can be routed:

```go
durable.RegisterSendHandler(w, svc.HandleExpire, durable.HandlerOptions{})
```

This is required because SendAfter delivers messages through the signal channel. The `ReceiveSend(s.HandleExpire, state)` case in the Select routes them to the handler. No new registration mechanism needed.
