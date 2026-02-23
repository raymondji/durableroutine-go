// Command booking demonstrates a multi-step routine where the client drives
// each step by sending typed messages. Shows Cast + Call + Query together.
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

type PaidState struct {
	UserID    string
	ItemID    string
	PaymentID string
}

// --- Handlers ---

func reserveItem(ctx *durable.Context, args BookingArgs) (*durable.Suspend, error) {
	fmt.Printf("reserving item %s for user %s\n", args.ItemID, args.UserID)
	reserved := ReservedState{UserID: args.UserID, ItemID: args.ItemID}
	return durable.Select(
		durable.OnCast(PaymentInbox, processPayment, reserved),
		durable.OnCall(CancelMethod, handleCancel, reserved),
		durable.OnQuery(GetStatus, getReservedStatus, reserved),
		durable.AfterFunc(15*time.Minute, handleReservationTimeout, reserved),
	), nil
}

func processPayment(ctx *durable.Context, state ReservedState, msg PaymentInfo) (*durable.Suspend, error) {
	fmt.Printf("charging card ending in %s\n", msg.CardNumber[len(msg.CardNumber)-4:])
	paid := PaidState{UserID: state.UserID, ItemID: state.ItemID, PaymentID: "PAY-123"}
	return durable.Select(
		durable.OnCast(ShippingInbox, processShipping, paid),
		durable.OnQuery(GetStatus, getPaidStatus, paid),
		durable.AfterFunc(24*time.Hour, handleShippingTimeout, paid),
	), nil
}

func processShipping(ctx *durable.Context, state PaidState, msg ShippingInfo) (*durable.Suspend, error) {
	fmt.Printf("shipping to %s, %s %s\n", msg.Address, msg.City, msg.Zip)
	return nil, nil // routine complete
}

// Query handlers: read-only, state by value, don't advance state machine.
func getReservedStatus(ctx *durable.Context, _ ReservedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "reserved"}, nil
}

func getPaidStatus(ctx *durable.Context, state PaidState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "paid", PaymentID: state.PaymentID}, nil
}

// Call handler: advances state machine, returns response to caller.
func handleCancel(ctx *durable.Context, _ ReservedState, _ CancelReq) (CancelResp, *durable.Suspend, error) {
	return CancelResp{Confirmed: true}, nil, nil // routine complete
}

func handleReservationTimeout(ctx *durable.Context, _ ReservedState) (*durable.Suspend, error) {
	fmt.Println("reservation expired, no payment received")
	return nil, nil
}

func handleShippingTimeout(ctx *durable.Context, _ PaidState) (*durable.Suspend, error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	workers := durable.NewWorkers()
	durable.AddRoutine(workers, reserveItem)

	w := durable.NewWorker("booking-queue", workers)
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
