// Command reminder demonstrates a simple durable routine that sends a
// sequence of emails with durable sleeps between them.
// Uses struct-based handlers for dependency injection.
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

// --- Service struct ---

type ReminderService struct {
	// Injected dependencies would go here (e.g., email client).
}

func (s *ReminderService) Handle(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending initial email to %s\n", args.Email)
	return durable.After(24*time.Hour, s.SendFollowUp, args), nil
}

func (s *ReminderService) SendFollowUp(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending follow-up email to %s\n", args.Email)
	return durable.After(7*24*time.Hour, s.SendFinal, args), nil
}

func (s *ReminderService) SendFinal(ctx *durable.Context, args ReminderArgs) (*durable.Suspend, error) {
	fmt.Printf("sending final email to %s\n", args.Email)
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &ReminderService{}

	w := durable.NewWorker("reminder-queue")
	durable.AddRoutineHandler(w, svc.Handle)
	durable.AddHandler(w, svc.SendFollowUp)
	durable.AddHandler(w, svc.SendFinal)

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
