// Package booking demonstrates a multi-step durable routine where the client drives
// each step by sending typed messages. Shows Send + Call + Query together
// with struct-based dependency injection.
// Per-step state: BookingState -> ReservedState -> PaidState.
//
// Also demonstrates ReceiveSendTerminalError: if payment processing fails after
// all retries, the terminal error handler releases the reservation instead
// of failing the entire routine.
package booking

import (
	"fmt"
	"time"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- State ---

type BookingState struct {
	UserID string
	ItemID string
}

func (BookingState) DurableKind() string { return "booking" }

// --- Messages ---

type PaymentInfo struct {
	CardNumber string
	Expiry     string
}

func (PaymentInfo) DurableKind() string { return "payment" }

type ShippingInfo struct {
	Address string
	City    string
	Zip     string
}

func (ShippingInfo) DurableKind() string { return "shipping" }

type CancelReq struct{ Reason string }

func (CancelReq) DurableKind() string { return "cancel" }

type CancelResp struct{ Confirmed bool }

func (CancelResp) DurableKind() string { return "cancel-resp" }

type StatusResp struct {
	Status    string
	PaymentID string
}

func (StatusResp) DurableKind() string { return "get-status" }

// --- Result ---

type BookingResult struct {
	Status string
}

func (BookingResult) DurableKind() string { return "booking-result" }

// --- Per-step state types ---

type ReservedState struct {
	UserID string
	ItemID string
}

func (ReservedState) DurableKind() string { return "booking.reserved" }

type PaidState struct {
	UserID    string
	ItemID    string
	PaymentID string
}

func (PaidState) DurableKind() string { return "booking.paid" }

// --- Service struct ---

type BookingService struct {
	// Injected dependencies would go here (e.g., DB, payment gateway).
	ReservationTimeout time.Duration      // if zero, defaults to 15min
	ShippingTimeout    time.Duration      // if zero, defaults to 24h
	ChargeCardFn       func(string) error // if non-nil, called during payment processing
}

func (s *BookingService) ReserveItem(ctx *durable.Context, state BookingState) (*durable.Continuation[BookingResult], error) {
	fmt.Printf("reserving item %s for user %s\n", state.ItemID, state.UserID)
	reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}

	reservationTimeout := 15 * time.Minute
	if s.ReservationTimeout > 0 {
		reservationTimeout = s.ReservationTimeout
	}
	durable.SetQueryResult(ctx, StatusResp{Status: "reserved"})
	return durable.Select(
		durable.ReceiveSend(s.ProcessPayment, reserved),
		durable.ReceiveCall(s.CancelBooking, reserved),
		durable.After(reservationTimeout, s.ExpireReservation, reserved),
	), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Continuation[BookingResult], error) {
	if s.ChargeCardFn != nil {
		if err := s.ChargeCardFn(msg.CardNumber); err != nil {
			return nil, fmt.Errorf("charge card: %w", err)
		}
	}
	fmt.Printf("charging card ending in %s\n", msg.CardNumber[len(msg.CardNumber)-4:])
	paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}

	shippingTimeout := 24 * time.Hour
	if s.ShippingTimeout > 0 {
		shippingTimeout = s.ShippingTimeout
	}
	durable.SetQueryResult(ctx, StatusResp{Status: "paid", PaymentID: paid.PaymentID})
	return durable.Select(
		durable.ReceiveSend(s.ProcessShipping, paid),
		durable.After(shippingTimeout, s.ExpireShipping, paid),
	), nil
}

// PaymentFailed is the terminal error handler for ProcessPayment. If the
// payment gateway is unreachable after all retries, release the reservation
// so the item goes back into inventory.
func (s *BookingService) PaymentFailed(ctx *durable.Context, state ReservedState, msg PaymentInfo, err error) (*durable.Continuation[BookingResult], error) {
	fmt.Printf("payment failed for item %s after all retries: %v\n", state.ItemID, err)
	fmt.Printf("releasing reservation for item %s\n", state.ItemID)
	durable.SetQueryResult(ctx, StatusResp{Status: "payment_failed"})
	return durable.Done(BookingResult{Status: "payment_failed"}), nil
}

func (s *BookingService) ProcessShipping(ctx *durable.Context, state PaidState, msg ShippingInfo) (*durable.Continuation[BookingResult], error) {
	fmt.Printf("shipping to %s, %s %s\n", msg.Address, msg.City, msg.Zip)
	return durable.Done(BookingResult{Status: "shipped"}), nil
}

// Call handler: advances state machine, returns response to caller.
func (s *BookingService) CancelBooking(ctx *durable.Context, _ ReservedState, _ CancelReq) (CancelResp, *durable.Continuation[BookingResult], error) {
	return CancelResp{Confirmed: true}, durable.Done(BookingResult{Status: "cancelled"}), nil
}

func (s *BookingService) ExpireReservation(ctx *durable.Context, _ ReservedState) (*durable.Continuation[BookingResult], error) {
	fmt.Println("reservation expired, no payment received")
	return durable.Done(BookingResult{Status: "expired"}), nil
}

func (s *BookingService) ExpireShipping(ctx *durable.Context, _ PaidState) (*durable.Continuation[BookingResult], error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	return durable.Done(BookingResult{Status: "refunded"}), nil
}

// RegisterHandlers registers all booking handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *BookingService) {
	durable.RegisterHandler(w, svc.ReserveItem, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 5},
	})
	durable.RegisterSendHandler(w, svc.ProcessPayment, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	}).WithTerminalErrorHandler(svc.PaymentFailed, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.ProcessShipping, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	})
	durable.RegisterCallHandler(w, svc.CancelBooking, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.ExpireReservation, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.ExpireShipping, durable.HandlerOptions{})
}
