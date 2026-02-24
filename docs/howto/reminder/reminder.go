// Package reminder demonstrates a simple durable routine that sends a
// sequence of emails with durable sleeps between them.
// Uses struct-based handlers for dependency injection.
// Each handler declares its own state type — state flows forward via After().
package reminder

import (
	"fmt"
	"time"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- Per-step state types ---

type InitialInput struct {
	Email string
}

func (InitialInput) DurableKind() string { return "reminder.initial" }

type FollowUpInput struct {
	Email string
}

func (FollowUpInput) DurableKind() string { return "reminder.follow-up" }

type FinalInput struct {
	Email string
}

func (FinalInput) DurableKind() string { return "reminder.final" }

// --- Service struct ---

type ReminderService struct {
	// Injected dependencies would go here (e.g., email client).
	InitialDelay  time.Duration // if zero, defaults to 24h
	FollowUpDelay time.Duration // if zero, defaults to 7 days
}

func (s *ReminderService) SendInitial(ctx *durable.Context, input InitialInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending initial email to %s\n", input.Email)
	delay := 24 * time.Hour
	if s.InitialDelay > 0 {
		delay = s.InitialDelay
	}
	return durable.After(delay, s.SendFollowUp, FollowUpInput{Email: input.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *durable.Context, input FollowUpInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", input.Email)
	delay := 7 * 24 * time.Hour
	if s.FollowUpDelay > 0 {
		delay = s.FollowUpDelay
	}
	return durable.After(delay, s.SendFinal, FinalInput{Email: input.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *durable.Context, input FinalInput) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending final email to %s\n", input.Email)
	return durable.Done(durable.Unit{}), nil
}

// RegisterHandlers registers all reminder handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *ReminderService) {
	durable.RegisterHandler(w, svc.SendInitial, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFollowUp, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFinal, durable.HandlerOptions{})
}
