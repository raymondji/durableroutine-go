# Durable Routine for Go

A Go library that gives you durable, distributed goroutines and channels — powered by [Temporal](https://temporal.io/) but without the replay-safety constraints.

## Why Durable Routine?

**Bottom-up: Go concurrency, but durable.** If you know goroutines, channels, and `select`, you already know the programming model. `Go` starts a routine, `BufferSend` sends a message, `ReceiveSend` receives one, and `Select` waits for the first of several events. The difference: your state survives process crashes, deploys, and restarts — automatically.

**Top-down: Temporal's power, without the pain.** Temporal gives you durable execution, but requires replay-safe deterministic code (no `time.Now()`, no real goroutines, no direct I/O) and manual `continue-as-new` for long-running workflows. Durable Routine eliminates both constraints. Your handlers are normal Go functions — call databases, use the standard library, do whatever you want. The library handles replay safety and history management for you.

## Quick Example

A multi-step booking flow with timers, messages, synchronous calls, queries, and compensation:

```go
type BookingService struct { /* injected deps */ }

func (s *BookingService) ReserveItem(ctx *durable.Context, state BookingState) (*durable.Continuation[BookingResult], error) {
    fmt.Printf("reserving item %s for user %s\n", state.ItemID, state.UserID)
    reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}

    durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})
    return durable.Select(
        durable.ReceiveSend(s.ProcessPayment, reserved),  // wait for payment message
        durable.ReceiveCall(s.CancelBooking, reserved),    // wait for cancel request
        durable.After(15*time.Minute, s.ExpireReservation, reserved), // timeout
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Continuation[BookingResult], error) {
    fmt.Printf("charging card ending in %s\n", msg.CardNumber[len(msg.CardNumber)-4:])
    paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}

    durable.SetQueryResult(ctx, StatusResp{Status: "paid", PaymentID: paid.PaymentID})
    return durable.Select(
        durable.ReceiveSend(s.ProcessShipping, paid),
        durable.After(24*time.Hour, s.ExpireShipping, paid),
    ), nil
}

// Terminal error handler: if payment fails after all retries, release the reservation.
func (s *BookingService) PaymentFailed(ctx *durable.Context, state ReservedState, msg PaymentInfo, err error) (*durable.Continuation[BookingResult], error) {
    fmt.Printf("payment failed, releasing reservation for item %s\n", state.ItemID)
    return durable.Done(BookingResult{Status: "payment_failed"}), nil
}

func RegisterHandlers(w *durable.Worker, svc *BookingService) {
    durable.RegisterHandler(w, svc.ReserveItem, durable.HandlerOptions{
        RetryPolicy: durable.RetryPolicy{MaxAttempts: 5},
    })
    durable.RegisterSendHandler(w, svc.ProcessPayment, durable.HandlerOptions{
        RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
    }).WithTerminalErrorHandler(svc.PaymentFailed, durable.HandlerOptions{})
    durable.RegisterCallHandler(w, svc.CancelBooking, durable.HandlerOptions{})
    // ... remaining handlers
}
```

Full example: [`docs/howto/booking/`](docs/howto/booking/booking.go)

## How It Works

- **Handlers run as Temporal activities** — your code has zero replay-safety constraints. Use `time.Now()`, call databases, spawn goroutines — whatever you need.
- **Handlers return declarative Continuations** instead of blocking. `Select(ReceiveSend(...), After(...))` tells the runtime *what to wait for*, not *how to wait*.
- **A library-owned workflow loop** interprets Continuations, sets up Temporal primitives (timers, signals, updates), and invokes the next handler when an event fires.
- **Continue-as-new is automatic.** State is explicit and serializable, so the library manages history compaction transparently.

## Examples

| Example | Description |
|---|---|
| [`reminder`](docs/howto/reminder/reminder.go) | Timer chain with per-handler state (`ContinueAfter`) |
| [`order`](docs/howto/order/order.go) | Order lifecycle with Send + timer + Query (`Select`, `ReceiveSend`, `After`, `SetQueryResult`) |
| [`booking`](docs/howto/booking/booking.go) | Multi-step booking with Send + Call + Query + terminal error compensation |
| [`auction`](docs/howto/auction/auction.go) | Synchronous bidding via `ReceiveCall` with live status queries |
| [`fanout`](docs/howto/fanout/fanout.go) | Fan-out/fan-in with child routines and `BufferSend` |
| [`pipeline`](docs/howto/pipeline/pipeline.go) | Producer-consumer pipeline via routine-to-routine messaging |
| [`saga`](docs/howto/saga/saga.go) | SAGA compensation with terminal error handlers |
| [`batch`](docs/howto/batch/batch.go) | Chunked batch processing with cancellation via `Select` + `Default` |

## Documentation

- [`docs/`](docs/) — Full documentation (how-to guides, tutorials, explanations)
- [`devdocs/macrodesign/OVERALL_DESIGN.md`](devdocs/macrodesign/OVERALL_DESIGN.md) — Design doc with detailed API reference and implementation details
