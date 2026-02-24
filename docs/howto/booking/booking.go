// Package booking demonstrates a multi-step stateroutine where the client drives
// each step by sending typed messages. Shows Send + Call + Query together
// with struct-based dependency injection.
// Per-step state: BookingState → ReservedState → PaidState.
//
// Also demonstrates OnSendTerminalError: if payment processing fails after
// all retries, the terminal error handler releases the reservation instead
// of failing the entire stateroutine.
package booking

import (
	"fmt"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- State ---

type BookingState struct {
	UserID string
	ItemID string
}

func (BookingState) Kind() string { return "booking" }

// --- Messages ---

type PaymentInfo struct {
	CardNumber string
	Expiry     string
}

func (PaymentInfo) Kind() string { return "payment" }

type ShippingInfo struct {
	Address string
	City    string
	Zip     string
}

func (ShippingInfo) Kind() string { return "shipping" }

type CancelReq struct{ Reason string }

func (CancelReq) Kind() string { return "cancel" }

type CancelResp struct{ Confirmed bool }

type StatusResp struct {
	Status    string
	PaymentID string
}

func (StatusResp) Kind() string { return "get-status" }

// --- Result ---

type BookingResult struct {
	Status string
}

// --- Per-step state types ---

type ReservedState struct {
	UserID string
	ItemID string
}

func (ReservedState) Kind() string { return "booking.reserved" }

type PaidState struct {
	UserID    string
	ItemID    string
	PaymentID string
}

func (PaidState) Kind() string { return "booking.paid" }

// --- Service struct ---

type BookingService struct {
	// Injected dependencies would go here (e.g., DB, payment gateway).
	ReservationTimeout time.Duration      // if zero, defaults to 15min
	ShippingTimeout    time.Duration      // if zero, defaults to 24h
	ChargeCardFn       func(string) error // if non-nil, called during payment processing
}

func (s *BookingService) ReserveItem(ctx *stateroutine.Context, state BookingState) (*stateroutine.Suspend[BookingResult], error) {
	fmt.Printf("reserving item %s for user %s\n", state.ItemID, state.UserID)
	reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}

	reservationTimeout := 15 * time.Minute
	if s.ReservationTimeout > 0 {
		reservationTimeout = s.ReservationTimeout
	}
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "reserved"})
	return stateroutine.Select[BookingResult](
		stateroutine.OnSend(s.ProcessPayment, reserved),
		stateroutine.OnCall(s.CancelBooking, reserved),
		stateroutine.OnTimer(reservationTimeout, s.ExpireReservation, reserved),
	), nil
}

func (s *BookingService) ProcessPayment(ctx *stateroutine.Context, state ReservedState, msg PaymentInfo) (*stateroutine.Suspend[BookingResult], error) {
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
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "paid", PaymentID: paid.PaymentID})
	return stateroutine.Select[BookingResult](
		stateroutine.OnSend(s.ProcessShipping, paid),
		stateroutine.OnTimer(shippingTimeout, s.ExpireShipping, paid),
	), nil
}

// PaymentFailed is the terminal error handler for ProcessPayment. If the
// payment gateway is unreachable after all retries, release the reservation
// so the item goes back into inventory.
func (s *BookingService) PaymentFailed(ctx *stateroutine.Context, state ReservedState, msg PaymentInfo, err error) (*stateroutine.Suspend[BookingResult], error) {
	fmt.Printf("payment failed for item %s after all retries: %v\n", state.ItemID, err)
	fmt.Printf("releasing reservation for item %s\n", state.ItemID)
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "payment_failed"})
	return stateroutine.Done(BookingResult{Status: "payment_failed"}), nil
}

func (s *BookingService) ProcessShipping(ctx *stateroutine.Context, state PaidState, msg ShippingInfo) (*stateroutine.Suspend[BookingResult], error) {
	fmt.Printf("shipping to %s, %s %s\n", msg.Address, msg.City, msg.Zip)
	return stateroutine.Done(BookingResult{Status: "shipped"}), nil
}

// Call handler: advances state machine, returns response to caller.
func (s *BookingService) CancelBooking(ctx *stateroutine.Context, _ ReservedState, _ CancelReq) (CancelResp, *stateroutine.Suspend[BookingResult], error) {
	return CancelResp{Confirmed: true}, stateroutine.Done(BookingResult{Status: "cancelled"}), nil
}

func (s *BookingService) ExpireReservation(ctx *stateroutine.Context, _ ReservedState) (*stateroutine.Suspend[BookingResult], error) {
	fmt.Println("reservation expired, no payment received")
	return stateroutine.Done(BookingResult{Status: "expired"}), nil
}

func (s *BookingService) ExpireShipping(ctx *stateroutine.Context, _ PaidState) (*stateroutine.Suspend[BookingResult], error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	return stateroutine.Done(BookingResult{Status: "refunded"}), nil
}

// RegisterHandlers registers all booking handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *BookingService) {
	stateroutine.RegisterHandler(w, svc.ReserveItem, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 5},
	})
	stateroutine.RegisterSendHandler(w, svc.ProcessPayment, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	}).WithTerminalErrorHandler(svc.PaymentFailed, stateroutine.HandlerOptions{})
	stateroutine.RegisterSendHandler(w, svc.ProcessShipping, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	})
	stateroutine.RegisterCallHandler(w, svc.CancelBooking, stateroutine.HandlerOptions{})
	stateroutine.RegisterHandler(w, svc.ExpireReservation, stateroutine.HandlerOptions{})
	stateroutine.RegisterHandler(w, svc.ExpireShipping, stateroutine.HandlerOptions{})
}
