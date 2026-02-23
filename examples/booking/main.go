// Command booking demonstrates a multi-step routine where the client drives
// each step by sending typed messages. Shows Cast + Call + Query together
// with struct-based dependency injection.
// Per-step state: BookingArgs → ReservedState → PaidState.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args & descriptors ---

type BookingArgs struct {
	UserID string
	ItemID string
}

func (BookingArgs) Kind() string { return "booking" }

var PaymentInbox = durable.Inbox[PaymentInfo]{Name: "payment"}
var ShippingInbox = durable.Inbox[ShippingInfo]{Name: "shipping"}
var CancelMethod = durable.Method[CancelReq, CancelResp]{Name: "cancel"}
var GetStatus = durable.Query[StatusReq, StatusResp]{Name: "get-status"}

// --- Messages ---

type PaymentInfo struct {
	CardNumber string
	Expiry     string
}

type ShippingInfo struct {
	Address string
	City    string
	Zip     string
}

type CancelReq struct{ Reason string }
type CancelResp struct{ Confirmed bool }

type StatusReq struct{}
type StatusResp struct {
	Status    string
	PaymentID string
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

func (s *BookingService) Handle(ctx *durable.Context, args BookingArgs) (*durable.Suspend, error) {
	fmt.Printf("reserving item %s for user %s\n", args.ItemID, args.UserID)
	reserved := ReservedState{UserID: args.UserID, ItemID: args.ItemID}
	return durable.Select(
		durable.OnCast(PaymentInbox, s.ProcessPayment, reserved),
		durable.OnCall(CancelMethod, s.HandleCancel, reserved),
		durable.OnQuery(GetStatus, s.GetReservedStatus, reserved),
		durable.AfterFunc(15*time.Minute, s.HandleReservationTimeout, reserved),
	), nil
}

func (s *BookingService) ProcessPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Suspend, error) {
	fmt.Printf("charging card ending in %s\n", msg.CardNumber[len(msg.CardNumber)-4:])
	paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
	return durable.Select(
		durable.OnCast(ShippingInbox, s.ProcessShipping, paid),
		durable.OnQuery(GetStatus, s.GetPaidStatus, paid),
		durable.AfterFunc(24*time.Hour, s.HandleShippingTimeout, paid),
	), nil
}

func (s *BookingService) ProcessShipping(ctx *durable.Context, state PaidState, msg ShippingInfo) (*durable.Suspend, error) {
	fmt.Printf("shipping to %s, %s %s\n", msg.Address, msg.City, msg.Zip)
	return nil, nil // routine complete
}

// Query handlers: read-only, state by value, don't advance state machine.
func (s *BookingService) GetReservedStatus(ctx *durable.Context, _ ReservedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "reserved"}, nil
}

func (s *BookingService) GetPaidStatus(ctx *durable.Context, state PaidState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "paid", PaymentID: state.PaymentID}, nil
}

// Call handler: advances state machine, returns response to caller.
func (s *BookingService) HandleCancel(ctx *durable.Context, _ ReservedState, _ CancelReq) (CancelResp, *durable.Suspend, error) {
	return CancelResp{Confirmed: true}, nil, nil // routine complete
}

func (s *BookingService) HandleReservationTimeout(ctx *durable.Context, _ ReservedState) (*durable.Suspend, error) {
	fmt.Println("reservation expired, no payment received")
	return nil, nil
}

func (s *BookingService) HandleShippingTimeout(ctx *durable.Context, _ PaidState) (*durable.Suspend, error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &BookingService{}

	w := durable.NewWorker("booking-queue")
	durable.AddRoutineHandler(w, svc.Handle, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 5}))
	durable.AddCastHandler(w, svc.ProcessPayment)
	durable.AddCastHandler(w, svc.ProcessShipping)
	durable.AddCallHandler(w, svc.HandleCancel)
	durable.AddQueryHandler(w, svc.GetReservedStatus)
	durable.AddQueryHandler(w, svc.GetPaidStatus)
	durable.AddHandler(w, svc.HandleReservationTimeout)
	durable.AddHandler(w, svc.HandleShippingTimeout)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	// Start the booking routine.
	if err := durable.Start(client, ctx, "booking-123",
		BookingArgs{UserID: "user-42", ItemID: "SKU-900"}); err != nil {
		log.Fatal(err)
	}

	// Query the current status.
	status, err := durable.ClientQuery(client, ctx, "booking-123", GetStatus, StatusReq{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	// Send payment info.
	if err := durable.ClientCast(client, ctx, "booking-123", PaymentInbox, PaymentInfo{
		CardNumber: "4111111111111234",
		Expiry:     "12/27",
	}); err != nil {
		log.Fatal(err)
	}

	// Send shipping info.
	if err := durable.ClientCast(client, ctx, "booking-123", ShippingInbox, ShippingInfo{
		Address: "123 Main St",
		City:    "Springfield",
		Zip:     "62704",
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("booking steps submitted")
}
