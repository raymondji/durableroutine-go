# Durable Routines for Go

"Durable goroutines" library powered by the battle-tested [Temporal.io](https://temporal.io/).

## What are Durable Routines?

- **Bottom-up view:** Like native goroutines and channels, but durable and distributed.

- **Top-down view:** Most of Temporal's power, minus the pain of replay-safety and continue-as-new 

## Show me some code

A booking flow: reserve an item, wait for payment or cancellation, then ship.

```go
func (s *BookingService) ReserveItem(ctx *durable.Context, input BookingInput) (*durable.Continuation[BookingResult], error) {
    reserved := ReservedInput{UserID: input.UserID, ItemID: input.ItemID}
    durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})

    return durable.Select(
        durable.ReceiveSend(s.ProcessPayment, reserved),               // wait for payment message
        durable.ReceiveCall(s.CancelBooking, reserved),                // wait for cancel request
        durable.After(15*time.Minute, s.ExpireReservation, reserved),  // timeout
    ), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, input ReservedInput, externalInput PaymentInfo) (*durable.Continuation[BookingResult], error) {
    chargeCard(externalInput.CardNumber)
    paid := PaidInput{UserID: input.UserID, ItemID: input.ItemID, PaymentID: "PAY-123"}
    durable.SetQueryResult(ctx, StatusResp{Status: "paid", PaymentID: paid.PaymentID})

    return durable.Select(
        durable.ReceiveSend(s.RefundPayment, paid),
        durable.After(24*time.Hour, s.ProcessShipping, paid),
    ), nil
}

// If payment fails after all retries, run compensations.
func (s *BookingService) PaymentFailed(ctx *durable.Context, input ReservedInput, externalInput PaymentInfo, err error) (*durable.Continuation[BookingResult], error) {
    refundPaymentIfPaid(input.ItemID)
    releaseReservation(input.ItemID)

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
        BookingInput{UserID: "user-42", ItemID: "SKU-900"})

    // Query current status (read-only, instant).
    status, _ := durable.Query(client, ctx, "booking-123", StatusResp{})
    fmt.Println(status.Status) // "reserved"

    // Send payment info (fire-and-forget).
    durable.Send(client, ctx, "booking-123", svc.ProcessPayment,
        PaymentInfo{CardNumber: "4111111111111234", Expiry: "12/27"})

    // Wait for completion and get the typed result.
    result, _ := h.Get(ctx)
}
```

Full example: [`docs/howto/booking/`](docs/howto/booking/booking.go)

## Get Started

Follow the [Quick Start](docs/tutorials/QUICK_START.md) to build your first durable routine in 5 minutes — no Temporal installation required.

## How It Works

- **Handlers are normal Go functions** — no replay-safety or determinism constraints. Call databases, use `time.Now()`, do whatever you need.
- **Handlers return Continuations** describing when to resume and what to do next.
- **Declarative retry policies** just like Temporal (b/c it's just Temporal under the hood)
- **Managed workflow progression**: the library is solely responsible for workflow-level control flow, retries, timers, message passing, and continue-as-new

## Examples

| Example | Description |
|---|---|
| [`reminder`](docs/howto/reminder/reminder.go) | Timer chain with per-handler state |
| [`order`](docs/howto/order/order.go) | Order lifecycle with messages, timers, and queries |
| [`booking`](docs/howto/booking/booking.go) | Multi-step booking with messages, calls, queries, and compensation |
| [`auction`](docs/howto/auction/auction.go) | Synchronous bidding with live status queries |
| [`fanout`](docs/howto/fanout/fanout.go) | Fan-out/fan-in with child routines |
| [`pipeline`](docs/howto/pipeline/pipeline.go) | Producer-consumer pipeline |
| [`saga`](docs/howto/saga/saga.go) | SAGA compensation with terminal error handlers |
| [`batch`](docs/howto/batch/batch.go) | Chunked batch processing with cancellation |

## Comparison

| Concept | Durable Routines | Temporal | Elixir GenServer | Goroutines |
|---|---|---|---|---|
| **Unit of execution** | Durable Routine | Workflow | GenServer process | Goroutine |
| **Start** | `durable.Go` | `client.ExecuteWorkflow` | `GenServer.start_link` | `go func()` |
| **Fire-and-forget msg** | `Send` | Signal | `GenServer.cast` | `ch <- msg` |
| **Request-response** | `Call` | Update | `GenServer.call` | `reqCh <- req; resp := <-respCh` |
| **Read-only query** | `Query` | Query | `:sys.get_state` | `mu.RLock()` + shared state |
| **Timer** | `After` | `workflow.Sleep` | `Process.send_after` | `time.After` |
| **Replay-safety required** | No | Yes | N/A | N/A |
| **Continue-as-new** | Transparent | User managed | N/A | N/A |
| **Durable** | Yes (using Temporal) | Yes | No | No |

## Design Principles

- **Return errors as early as possible.** Prefer compile-time errors (via generics), then startup-time validation,
  then runtime errors.

## Inspirations

- **[Temporal](https://temporal.io/)** — the durability engine underneath
- **[Elixir GenServer](https://hexdocs.pm/elixir/GenServer.html)** — actor model semantics; the handler-returns-continuation loop mirrors GenServer callbacks
- **Go goroutines & channels** — the native primitives this library tries to stay close to in spirit
- **[Continuation-passing style](https://en.wikipedia.org/wiki/Continuation-passing_style)** — functions that return an explicit Continuation (the remaining work), making control flow explicit
- **[River](https://riverqueue.com/)** — type-safe registration via self-identifying `DurableKind()` types
- **[iWF](https://github.com/indeedeng/iwf)** — similar goal of moving user code out of the replay-safe workflow function
