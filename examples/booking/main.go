// Command booking demonstrates a multi-step routine where the client drives
// each step by sending typed messages. Shows Cast + Call + Query together
// with struct-based dependency injection.
// Per-step state: BookingState → ReservedState → PaidState.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
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

type StatusReq struct{}

func (StatusReq) Kind() string { return "get-status" }

type StatusResp struct {
	Status    string
	PaymentID string
}

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
}

func (s *BookingService) ReserveItem(ctx *durable.Context, state BookingState) (*durable.Suspend[BookingResult], error) {
	fmt.Printf("reserving item %s for user %s\n", state.ItemID, state.UserID)
	reserved := ReservedState{UserID: state.UserID, ItemID: state.ItemID}

	durable.SetQueryHandler(ctx, s.GetReservedStatus, reserved)
	return durable.Select[BookingResult](
		durable.OnCast(s.ProcessPayment, reserved),
		durable.OnCall(s.CancelBooking, reserved),
		durable.OnTimer(15*time.Minute, s.ExpireReservation, reserved),
	), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Suspend[BookingResult], error) {
	fmt.Printf("charging card ending in %s\n", msg.CardNumber[len(msg.CardNumber)-4:])
	paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}

	durable.SetQueryHandler(ctx, s.GetPaidStatus, paid)
	return durable.Select[BookingResult](
		durable.OnCast(s.ProcessShipping, paid),
		durable.OnTimer(24*time.Hour, s.ExpireShipping, paid),
	), nil
}

func (s *BookingService) ProcessShipping(ctx *durable.Context, state PaidState, msg ShippingInfo) (*durable.Suspend[BookingResult], error) {
	fmt.Printf("shipping to %s, %s %s\n", msg.Address, msg.City, msg.Zip)
	return durable.Done(BookingResult{Status: "shipped"}), nil
}

// Query handlers: read-only, state by value, don't advance state machine.
func (s *BookingService) GetReservedStatus(ctx *durable.Context, _ ReservedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "reserved"}, nil
}

func (s *BookingService) GetPaidStatus(ctx *durable.Context, state PaidState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "paid", PaymentID: state.PaymentID}, nil
}

// Call handler: advances state machine, returns response to caller.
func (s *BookingService) CancelBooking(ctx *durable.Context, _ ReservedState, _ CancelReq) (CancelResp, *durable.Suspend[BookingResult], error) {
	return CancelResp{Confirmed: true}, durable.Done(BookingResult{Status: "cancelled"}), nil
}

func (s *BookingService) ExpireReservation(ctx *durable.Context, _ ReservedState) (*durable.Suspend[BookingResult], error) {
	fmt.Println("reservation expired, no payment received")
	return durable.Done(BookingResult{Status: "expired"}), nil
}

func (s *BookingService) ExpireShipping(ctx *durable.Context, _ PaidState) (*durable.Suspend[BookingResult], error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	return durable.Done(BookingResult{Status: "refunded"}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &BookingService{}

	w := durable.NewWorker("booking-queue")
	durable.AddHandler(w, svc.ReserveItem, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 5}))
	durable.AddCastHandler(w, svc.ProcessPayment, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))
	durable.AddCastHandler(w, svc.ProcessShipping, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))
	durable.AddCallHandler(w, svc.CancelBooking)
	durable.AddQueryHandler(w, svc.GetReservedStatus)
	durable.AddQueryHandler(w, svc.GetPaidStatus)
	durable.AddHandler(w, svc.ExpireReservation)
	durable.AddHandler(w, svc.ExpireShipping)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	// Start the booking routine.
	h, err := durable.Start(client, ctx, "booking-123", svc.ReserveItem,
		BookingState{UserID: "user-42", ItemID: "SKU-900"})
	if err != nil {
		log.Fatal(err)
	}

	// Query the current status.
	status, err := durable.ClientQuery(client, ctx, "booking-123", svc.GetReservedStatus, StatusReq{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	// Send payment info.
	if err := durable.ClientCast(client, ctx, "booking-123", PaymentInfo{
		CardNumber: "4111111111111234",
		Expiry:     "12/27",
	}); err != nil {
		log.Fatal(err)
	}

	// Send shipping info.
	if err := durable.ClientCast(client, ctx, "booking-123", ShippingInfo{
		Address: "123 Main St",
		City:    "Springfield",
		Zip:     "62704",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the routine to complete and get the result.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("booking result: %s\n", result.Status)
}
