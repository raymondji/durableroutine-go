// Package reminder demonstrates a simple durable stateroutine that sends a
// sequence of emails with durable sleeps between them.
// Uses struct-based handlers for dependency injection.
// Each handler declares its own state type — state flows forward via After().
package reminder

import (
	"fmt"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- Per-step state types ---

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
	InitialDelay  time.Duration // if zero, defaults to 24h
	FollowUpDelay time.Duration // if zero, defaults to 7 days
}

func (s *ReminderService) SendInitial(ctx *stateroutine.Context, state InitialState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending initial email to %s\n", state.Email)
	delay := 24 * time.Hour
	if s.InitialDelay > 0 {
		delay = s.InitialDelay
	}
	return stateroutine.After(delay, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *ReminderService) SendFollowUp(ctx *stateroutine.Context, state FollowUpState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending follow-up email to %s\n", state.Email)
	delay := 7 * 24 * time.Hour
	if s.FollowUpDelay > 0 {
		delay = s.FollowUpDelay
	}
	return stateroutine.After(delay, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *ReminderService) SendFinal(ctx *stateroutine.Context, state FinalState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	fmt.Printf("sending final email to %s\n", state.Email)
	return stateroutine.Done(stateroutine.Unit{}), nil
}

// RegisterHandlers registers all reminder handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *ReminderService) {
	stateroutine.AddHandler(w, svc.SendInitial, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.SendFollowUp, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.SendFinal, stateroutine.HandlerOptions{})
}
