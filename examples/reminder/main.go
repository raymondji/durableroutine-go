// Command reminder demonstrates a simple durable routine that sends a
// sequence of emails with durable sleeps between them.
// Each handler declares its own state type — state flows forward via After().
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args ---

type ReminderArgs struct {
	Email string
}

func (ReminderArgs) Kind() string { return "reminder" }

// --- Handlers ---

func sendInitial(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending initial email to %s\n", args.Email)
	return durable.After(24*time.Hour, sendFollowUp, args), nil
}

func sendFollowUp(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending follow-up email to %s\n", args.Email)
	return durable.After(7*24*time.Hour, sendFinal, args), nil
}

func sendFinal(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending final email to %s\n", args.Email)
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	workers := durable.NewWorkers()
	durable.AddRoutine(workers, sendInitial)

	w := durable.NewWorker("reminder-queue", workers)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()
	if err := durable.Start(client, ctx, "reminder-user-42",
		ReminderArgs{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
