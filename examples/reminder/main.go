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

// --- Per-step state types ---
// Each step has its own state type with a unique Kind, even though they
// carry the same data. This is how the runtime distinguishes handlers.

type InitialState struct {
	Email string
}

func (InitialState) Kind() string { return "reminder.initial" }

type FollowUpState struct {
	Email string
}

func (FollowUpState) Kind() string { return "reminder.follow-up" }

type FinalState struct {
	Email string
}

func (FinalState) Kind() string { return "reminder.final" }

// --- Service struct ---

type ReminderService struct {
	// Injected dependencies would go here (e.g., email client).
}

func (s *ReminderService) SendInitial(ctx *durable.Context, state InitialState) (*durable.Suspend[durable.Unit], error) {
	fmt.Printf("sending initial email to %s\n", state.Email)
	return durable.After(24*time.Hour, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *durable.Context, state FollowUpState) (*durable.Suspend[durable.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", state.Email)
	return durable.After(7*24*time.Hour, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *durable.Context, state FinalState) (*durable.Suspend[durable.Unit], error) {
	fmt.Printf("sending final email to %s\n", state.Email)
	return durable.Done(durable.Unit{}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &ReminderService{}

	w := durable.NewWorker("reminder-queue")
	durable.AddHandler(w, svc.SendInitial)
	durable.AddHandler(w, svc.SendFollowUp)
	durable.AddHandler(w, svc.SendFinal)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()
	if _, err := durable.Start(client, ctx, "reminder-user-42",
		svc.SendInitial, InitialState{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
