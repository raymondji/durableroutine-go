# Durable Routines for Go

"Durable goroutines" powered by the battle-tested [Temporal](https://temporal.io/).

## What are Durable Routines?

- Bottom-up perspective: Take your native Go concurrency primitives (goroutines and channels), but add durability and distributed computing.

- Top-down perspective: Take most of Temporal's goodness, but remove sharp edges like managing replay safety and continue-as-new.

## Show me some code

A multi-step booking flow with timers, messages, synchronous calls, queries, and compensation:

```go
func (s *BookingService) ReserveItem(ctx *durable.Context, state BookingState) (*durable.Continuation[BookingResult], error) {
    reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}
    durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})
    return durable.Select(
        durable.ReceiveSend(s.ProcessPayment, reserved),               // wait for payment message
        durable.ReceiveCall(s.CancelBooking, reserved),                // wait for cancel request
        durable.After(15*time.Minute, s.ExpireReservation, reserved),  // timeout
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Continuation[BookingResult], error) {
    chargeCard(msg.CardNumber)
    paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
    durable.SetQueryResult(ctx, StatusResp{Status: "paid", PaymentID: paid.PaymentID})
    return durable.Select(
        durable.ReceiveSend(s.ProcessRefund, paid),
        durable.After(24*time.Hour, s.ProcessShipping, paid),
    ), nil
}

// Terminal error handler: if payment fails after all retries, release the reservation.
func (s *BookingService) PaymentFailed(ctx *durable.Context, state ReservedState, msg PaymentInfo, err error) (*durable.Continuation[BookingResult], error) {
    releaseReservation(state.ItemID)
    return durable.Done(BookingResult{Status: "payment_failed"}), nil
}
```

Interact with the routine from a client:

```go
func main() {
    ctx := context.Background()
    svc := &BookingService{}

    // Start worker (handles the booking routine).
    go func() {
        w := durable.NewWorker("booking-queue")
        durable.RegisterHandler(w, svc.ReserveItem, durable.HandlerOptions{
            RetryPolicy: durable.RetryPolicy{MaxAttempts: 5},
        })
        durable.RegisterCallHandler(w, svc.CancelBooking, durable.HandlerOptions{})
        durable.RegisterSendHandler(w, svc.ProcessPayment, durable.HandlerOptions{
            RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
        }).WithTerminalErrorHandler(svc.PaymentFailed, durable.HandlerOptions{})
        w.Start()
    }

    // Start a booking routine.
    client := durable.NewClient(/* ... */)
    h, _ := durable.Go(client, ctx, "booking-123", svc.ReserveItem,
        BookingState{UserID: "user-42", ItemID: "SKU-900"})

    // Query the current status (read-only).
    status, _ := durable.Query(client, ctx, "booking-123", StatusResp{})
    fmt.Println(status.Status) // "reserved"

    // Send payment info (fire-and-forget message).
    durable.Send(client, ctx, "booking-123", svc.ProcessPayment,
        PaymentInfo{CardNumber: "4111111111111234", Expiry: "12/27"})

    // Wait for the routine to complete and get the typed result.
    result, _ := h.Get(ctx)
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
| [`reminder`](docs/howto/reminder/reminder.go) | Timer chain with per-handler state (`After`) |
| [`order`](docs/howto/order/order.go) | Order lifecycle with Send + timer + Query (`Select`, `ReceiveSend`, `After`, `SetQueryResult`) |
| [`booking`](docs/howto/booking/booking.go) | Multi-step booking with Send + Call + Query + terminal error compensation |
| [`auction`](docs/howto/auction/auction.go) | Synchronous bidding via `ReceiveCall` with live status queries |
| [`fanout`](docs/howto/fanout/fanout.go) | Fan-out/fan-in with child routines and `BufferSend` |
| [`pipeline`](docs/howto/pipeline/pipeline.go) | Producer-consumer pipeline via routine-to-routine messaging |
| [`saga`](docs/howto/saga/saga.go) | SAGA compensation with terminal error handlers |
| [`batch`](docs/howto/batch/batch.go) | Chunked batch processing with cancellation via `Select` + `Default` |

## Comparison

| Concept | Durable Routine | Temporal | Elixir GenServer | Goroutines |
|---|---|---|---|---|
| **Unit of execution** | Durable Routine | Workflow | GenServer process | Goroutine |
| **Start** | `durable.Go` | `client.ExecuteWorkflow` | `GenServer.start_link` | `go func()` |
| **Fire-and-forget msg** | `Send` | Signal | `GenServer.cast` | `ch <- msg` |
| **Request-response** | `Call` | Update | `GenServer.call` | `reqCh <- req; resp := <-respCh` |
| **Read-only query** | `Query` | Query | `:sys.get_state` | `mu.RLock()` + shared state |
| **Timer** | `After` | `workflow.Sleep` | `Process.send_after` | `time.After` |
| **Replay-safety required** | No | Yes | N/A | N/A |
| **Continue-as-new** | Automatic | Manual | N/A | N/A |
| **Durable** | Yes (using Temporal) | Yes | No | No |

## Inspirations

- **[Temporal](https://temporal.io/)** — The durability engine underneath. Durable Routine builds on Temporal's workflow/activity model, signals, updates, queries, and timers.
- **[Elixir GenServer](https://hexdocs.pm/elixir/GenServer.html)** — The actor model semantics. Each routine is an actor with a mailbox; the handler-returns-continuation loop mirrors GenServer's callback model.
- **Go goroutines & channels** — The mental model for concurrency. `Go` starts a routine, `BufferSend` sends a message, `ReceiveSend` receives one.
- **[River](https://riverqueue.com/)** — Type safety ergonomics. River's pattern of job types that self-identify via `Kind()` inspired the handler registration model.
- **[iWF](https://github.com/indeedeng/iwf)** — Similar goal of simplifying Temporal's programming model by moving user code out of the replay-safe workflow function.
