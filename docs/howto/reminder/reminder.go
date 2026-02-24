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

type InitialState struct {
	Email string
}

func (InitialState) DurableKind() string { return "reminder.initial" }

type FollowUpState struct {
	Email string
}

func (FollowUpState) DurableKind() string { return "reminder.follow-up" }

type FinalState struct {
	Email string
}

func (FinalState) DurableKind() string { return "reminder.final" }

// --- Service struct ---

type ReminderService struct {
	// Injected dependencies would go here (e.g., email client).
	InitialDelay  time.Duration // if zero, defaults to 24h
	FollowUpDelay time.Duration // if zero, defaults to 7 days
}

func (s *ReminderService) SendInitial(ctx *durable.Context, state InitialState) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending initial email to %s\n", state.Email)
	delay := 24 * time.Hour
	if s.InitialDelay > 0 {
		delay = s.InitialDelay
	}
	return durable.After(delay, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *durable.Context, state FollowUpState) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", state.Email)
	delay := 7 * 24 * time.Hour
	if s.FollowUpDelay > 0 {
		delay = s.FollowUpDelay
	}
	return durable.After(delay, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *durable.Context, state FinalState) (*durable.Continuation[durable.Unit], error) {
	fmt.Printf("sending final email to %s\n", state.Email)
	return durable.Done(durable.Unit{}), nil
}

// RegisterHandlers registers all reminder handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *ReminderService) {
	durable.RegisterHandler(w, svc.SendInitial, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFollowUp, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFinal, durable.HandlerOptions{})
}
