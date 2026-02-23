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

	"github.com/raymondji/stateroutine/stateroutine"
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

func (s *ReminderService) SendInitial(ctx *stateroutine.Context, state InitialState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending initial email to %s\n", state.Email)
	return stateroutine.After(24*time.Hour, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *stateroutine.Context, state FollowUpState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", state.Email)
	return stateroutine.After(7*24*time.Hour, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *stateroutine.Context, state FinalState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending final email to %s\n", state.Email)
	return stateroutine.Done(stateroutine.Unit{}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &ReminderService{}

	w := stateroutine.NewWorker("reminder-queue")
	stateroutine.AddHandler(w, svc.SendInitial, stateroutine.ErrorPolicy{})
	stateroutine.AddHandler(w, svc.SendFollowUp, stateroutine.ErrorPolicy{})
	stateroutine.AddHandler(w, svc.SendFinal, stateroutine.ErrorPolicy{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()
	if _, err := stateroutine.Start(client, ctx, "reminder-user-42",
		svc.SendInitial, InitialState{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
