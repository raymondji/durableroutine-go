// Package provider demonstrates the event loop / GenServer pattern: a long-lived
// routine that manages concurrent state and processes messages one at a time.
//
// A provider (e.g., a therapist, tutor, or consultant) has appointment slots.
// Users can reserve a slot and have 15 minutes to confirm and pay before the
// reservation expires. Multiple reservations can be pending concurrently.
//
// Key pattern: define a steadyStateSelect helper that returns the Select once,
// and have every handler return it. This keeps the event loop definition in one
// place and avoids an extra Temporal activity per message.
package provider

import (
	"fmt"
	"time"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- State ---

type ProviderInput struct {
	ProviderID string
	Slots      []string // available slot IDs
}

func (ProviderInput) DurableKind() string { return "provider" }

type ProviderState struct {
	ProviderID   string
	Slots        []string
	Reservations map[string]Reservation
	Booked       map[string]Booking
}

func (ProviderState) DurableKind() string { return "provider.state" }

type Reservation struct {
	UserID string
	SlotID string
}

type Booking struct {
	UserID    string
	SlotID    string
	PaymentID string
}

// --- Messages ---

type ReserveSlot struct {
	UserID string
	SlotID string
}

func (ReserveSlot) DurableKind() string { return "reserve-slot" }

type ReserveResp struct {
	Status string
}

func (ReserveResp) DurableKind() string { return "reserve-slot-resp" }

type ConfirmAndPay struct {
	UserID    string
	SlotID    string
	PaymentID string
}

func (ConfirmAndPay) DurableKind() string { return "confirm-and-pay" }

type ConfirmResp struct {
	Status string
}

func (ConfirmResp) DurableKind() string { return "confirm-and-pay-resp" }

type GoOffline struct{}

func (GoOffline) DurableKind() string { return "go-offline" }

type GoOfflineResp struct {
	Booked int
}

func (GoOfflineResp) DurableKind() string { return "go-offline-resp" }

type ExpireReservation struct {
	UserID string
	SlotID string
}

func (ExpireReservation) DurableKind() string { return "expire-reservation" }

// --- Result ---

// ProviderResult is the routine's result type. The routine completes when
// the provider goes offline.
type ProviderResult struct {
	Status string
}

func (ProviderResult) DurableKind() string { return "provider-result" }

// --- Service ---

type ProviderService struct {
	ReservationTimeout time.Duration // if zero, defaults to 15min
}

// steadyStateSelect defines the event loop — the set of messages this routine
// handles. Defined once, returned by every handler.
func (s *ProviderService) steadyStateSelect(state ProviderState) *durable.Continuation[ProviderResult] {
	return durable.Select(
		durable.ReceiveCall(s.HandleReserve, state),
		durable.ReceiveCall(s.HandleConfirmAndPay, state),
		durable.ReceiveCall(s.HandleGoOffline, state),
		durable.ReceiveSend(s.HandleExpire, state),
	)
}

func (s *ProviderService) reservationTimeout() time.Duration {
	if s.ReservationTimeout > 0 {
		return s.ReservationTimeout
	}
	return 15 * time.Minute
}

// Init runs once to set up initial state, then enters the event loop.
func (s *ProviderService) Init(ctx *durable.Context, input ProviderInput) (*durable.Continuation[ProviderResult], error) {
	fmt.Printf("provider %s online with %d slots\n", input.ProviderID, len(input.Slots))
	state := ProviderState{
		ProviderID:   input.ProviderID,
		Slots:        input.Slots,
		Reservations: make(map[string]Reservation),
		Booked:       make(map[string]Booking),
	}
	return s.steadyStateSelect(state), nil
}

// HandleReserve holds a slot for a user. A separate child routine manages the
// reservation timeout and signals back when it expires.
func (s *ProviderService) HandleReserve(ctx *durable.Context, state ProviderState, msg ReserveSlot) (ReserveResp, *durable.Continuation[ProviderResult], error) {
	// Check slot is available.
	if _, reserved := state.Reservations[msg.SlotID]; reserved {
		fmt.Printf("slot %s already reserved, rejecting\n", msg.SlotID)
		return ReserveResp{Status: "rejected: already reserved"}, s.steadyStateSelect(state), nil
	}
	if _, booked := state.Booked[msg.SlotID]; booked {
		fmt.Printf("slot %s already booked, rejecting\n", msg.SlotID)
		return ReserveResp{Status: "rejected: already booked"}, s.steadyStateSelect(state), nil
	}

	fmt.Printf("slot %s reserved for user %s\n", msg.SlotID, msg.UserID)
	state.Reservations[msg.SlotID] = Reservation{UserID: msg.UserID, SlotID: msg.SlotID}

	// Spawn a child routine that will signal us when the reservation expires.
	durable.BufferStart(ctx,
		fmt.Sprintf("reservation-%s-%s", state.ProviderID, msg.SlotID),
		s.ReservationTimer,
		ReservationTimerInput{
			ProviderRoutineID: ctx.RoutineID(),
			UserID:            msg.UserID,
			SlotID:            msg.SlotID,
			Timeout:           s.reservationTimeout(),
		},
	)

	return ReserveResp{Status: "reserved"}, s.steadyStateSelect(state), nil
}

// HandleConfirmAndPay finalizes a reservation into a booking.
func (s *ProviderService) HandleConfirmAndPay(ctx *durable.Context, state ProviderState, msg ConfirmAndPay) (ConfirmResp, *durable.Continuation[ProviderResult], error) {
	res, ok := state.Reservations[msg.SlotID]
	if !ok || res.UserID != msg.UserID {
		fmt.Printf("confirm rejected: no matching reservation for user %s slot %s\n", msg.UserID, msg.SlotID)
		return ConfirmResp{Status: "rejected: no matching reservation"}, s.steadyStateSelect(state), nil
	}

	fmt.Printf("slot %s confirmed and paid by user %s (payment %s)\n", msg.SlotID, msg.UserID, msg.PaymentID)
	delete(state.Reservations, msg.SlotID)
	state.Booked[msg.SlotID] = Booking{
		UserID:    msg.UserID,
		SlotID:    msg.SlotID,
		PaymentID: msg.PaymentID,
	}

	return ConfirmResp{Status: "confirmed"}, s.steadyStateSelect(state), nil
}

// HandleExpire releases a reservation that timed out.
func (s *ProviderService) HandleExpire(ctx *durable.Context, state ProviderState, msg ExpireReservation) (*durable.Continuation[ProviderResult], error) {
	res, ok := state.Reservations[msg.SlotID]
	if !ok || res.UserID != msg.UserID {
		// Already confirmed or already expired — stale timer message, ignore.
		fmt.Printf("expire ignored: no matching reservation for user %s slot %s\n", msg.UserID, msg.SlotID)
		return s.steadyStateSelect(state), nil
	}

	fmt.Printf("reservation expired for user %s slot %s\n", msg.UserID, msg.SlotID)
	delete(state.Reservations, msg.SlotID)

	return s.steadyStateSelect(state), nil
}

// HandleGoOffline takes the provider offline, completing the routine.
func (s *ProviderService) HandleGoOffline(ctx *durable.Context, state ProviderState, _ GoOffline) (GoOfflineResp, *durable.Continuation[ProviderResult], error) {
	fmt.Printf("provider %s going offline with %d bookings\n", state.ProviderID, len(state.Booked))
	return GoOfflineResp{Booked: len(state.Booked)}, durable.Done(ProviderResult{Status: "offline"}), nil
}

// --- Reservation timer (child routine) ---
//
// The child routine is purely a timer. It sleeps for the timeout duration,
// then signals the parent provider that the reservation expired. The client
// sends ConfirmAndPay directly to the provider — not to the child.
//
// If the reservation is confirmed before the timer fires, the expire signal
// arrives at a provider that has already deleted the reservation. The
// HandleExpire handler validates against current state and ignores it.

// ReservationTimerInput is the input for the short-lived timer child routine.
type ReservationTimerInput struct {
	ProviderRoutineID string
	UserID            string
	SlotID            string
	Timeout           time.Duration
}

func (ReservationTimerInput) DurableKind() string { return "reservation-timer" }

// ReservationTimerResult is the result of the timer child routine.
type ReservationTimerResult struct{}

func (ReservationTimerResult) DurableKind() string { return "reservation-timer-result" }

// SendExpireInput is passed from ReservationTimer to SendExpire after the
// timeout elapses. It carries the same fields but has a distinct DurableKind
// so the two handlers produce unique registration keys.
type SendExpireInput struct {
	ProviderRoutineID string
	UserID            string
	SlotID            string
}

func (SendExpireInput) DurableKind() string { return "send-expire" }

// ReservationTimer sleeps for the timeout, then signals the parent that the
// reservation expired. If the reservation was confirmed in the meantime,
// the parent's HandleExpire will see no matching reservation and ignore it.
func (s *ProviderService) ReservationTimer(ctx *durable.Context, input ReservationTimerInput) (*durable.Continuation[ReservationTimerResult], error) {
	return durable.After(input.Timeout, s.SendExpire, SendExpireInput{
		ProviderRoutineID: input.ProviderRoutineID,
		UserID:            input.UserID,
		SlotID:            input.SlotID,
	}), nil
}

// SendExpire fires after the timeout and signals the parent provider.
func (s *ProviderService) SendExpire(ctx *durable.Context, input SendExpireInput) (*durable.Continuation[ReservationTimerResult], error) {
	var stub *ProviderService
	durable.BufferSend(ctx, input.ProviderRoutineID, stub.HandleExpire, ExpireReservation{
		UserID: input.UserID,
		SlotID: input.SlotID,
	})
	return durable.Done(ReservationTimerResult{}), nil
}

// RegisterHandlers registers all provider handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *ProviderService) {
	// Provider event loop handlers.
	durable.RegisterHandler(w, svc.Init, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, svc.HandleReserve, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, svc.HandleConfirmAndPay, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, svc.HandleGoOffline, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.HandleExpire, durable.HandlerOptions{})

	// Reservation timer child routine handlers.
	durable.RegisterHandler(w, svc.ReservationTimer, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendExpire, durable.HandlerOptions{})
}
