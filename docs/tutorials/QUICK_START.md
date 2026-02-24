# Quick Start

Build a durable reminder email chain — 3 emails sent over a series of delays, surviving process restarts. No Temporal installation required.

## Prerequisites

- Go 1.23+

## Step 1: Create your project

```bash
mkdir myreminder && cd myreminder
go mod init myreminder
go get github.com/raymondji/durableroutine-go
```

## Step 2: Define input types

Each handler step declares its own input type. Input flows forward through the chain via continuations.

Create `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine-go/backend/inmemory"
	"github.com/raymondji/durableroutine-go/durable"
)

// Each input type implements durable.Payload via DurableKind().

type InitialInput struct{ Email string }
func (InitialInput) DurableKind() string { return "reminder.initial" }

type FollowUpInput struct{ Email string }
func (FollowUpInput) DurableKind() string { return "reminder.follow-up" }

type FinalInput struct{ Email string }
func (FinalInput) DurableKind() string { return "reminder.final" }
```

## Step 3: Write handlers

Handlers are normal Go functions — no replay-safety constraints. Each returns a `Continuation` describing what to do next.

```go
type ReminderService struct{}

func (s *ReminderService) SendInitial(ctx *durable.Context, input InitialInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending initial email to %s\n", input.Email)
	return durable.After(1*time.Millisecond, s.SendFollowUp, FollowUpInput{Email: input.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *durable.Context, input FollowUpInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", input.Email)
	return durable.After(1*time.Millisecond, s.SendFinal, FinalInput{Email: input.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *durable.Context, input FinalInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending final email to %s\n", input.Email)
	return durable.Done(durable.Unit{}), nil
}
```

`After` durably sleeps, then calls the next handler. In production you'd use real durations like `24 * time.Hour`.

## Step 4: Register handlers and run

```go
func main() {
	svc := &ReminderService{}

	// 1. Create a worker and register all handlers.
	w := durable.NewWorker("reminder-queue")
	durable.RegisterHandler(w, svc.SendInitial, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFollowUp, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFinal, durable.HandlerOptions{})

	// 2. Start the in-memory runtime (no Temporal needed).
	rt := inmemory.NewRuntime(w)
	client := rt.Client()

	// 3. Launch a durable routine.
	ctx := context.Background()
	h, err := durable.Go(client, ctx, "reminder-user-42",
		svc.SendInitial, InitialInput{Email: "user@example.com"})
	if err != nil {
		log.Fatal(err)
	}

	// 4. Wait for completion.
	if _, err := h.Get(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("all reminders sent")
}
```

Run it:

```bash
go run main.go
```

Output:

```
sending initial email to user@example.com
sending follow-up email to user@example.com
sending final email to user@example.com
all reminders sent
```

## Step 5 (optional): Switch to Temporal

To get real durability (survives process restarts), swap the backend to Temporal. Install and start Temporal:

```bash
brew install temporal
temporal server start-dev
```

Then replace the runtime setup:

```go
import (
	temporalclient "go.temporal.io/sdk/client"
	"github.com/raymondji/durableroutine-go/backend/temporal"
)

// Replace the inmemory runtime with Temporal:
tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
if err != nil {
    log.Fatal(err)
}
defer tc.Close()

tw := temporal.NewWorker(tc, w)
go func() {
    if err := tw.Start(); err != nil {
        log.Fatal(err)
    }
}()
defer tw.Stop()

client := durable.NewClientFrom(temporal.NewClient(tc, "reminder-queue"))
```

Everything else — input types, handlers, client calls — stays exactly the same.

## Next steps

- Browse the [how-to examples](../howto/) for patterns like messages, queries, fan-out, sagas, and batch processing
- Read the full [README](../../README.md) for the API overview and comparison table
