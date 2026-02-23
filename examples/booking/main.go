// Command booking demonstrates a multi-step process where the client drives
// each step by sending typed messages. Each step suspends with Receive,
// waiting for its own message type — so the State struct only holds what
// persists across steps, not every step's inputs.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Start args ---

type StartArgs struct {
	UserID string
	ItemID string
}

// --- Per-step messages (each step gets its own typed input) ---

type PaymentInfo struct {
	CardNumber string
	Expiry     string
}

type ShippingInfo struct {
	Address string
	City    string
	Zip     string
}

// --- State holds only what persists across steps ---

type State struct {
	UserID         string
	ItemID         string
	Status         string
	PaymentID      string
	ConfirmationID string
}

// --- Process definition ---

var process = durable.Process[State]{
	Name: "booking",
	InitState: func(args any) State {
		a := args.(StartArgs)
		return State{
			UserID: a.UserID,
			ItemID: a.ItemID,
			Status: "pending",
		}
	},
	Initial: reserveItem,
}

// Step 1: Reserve the item, then suspend waiting for payment.
func reserveItem(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Printf("reserving item %s for user %s\n", state.ItemID, state.UserID)
	state.Status = "reserved"

	// Suspend: wait for the client to send payment info.
	return durable.Select(
		durable.Receive[State, PaymentInfo]("payment", processPayment),
		durable.AfterFunc(15*time.Minute, handleReservationTimeout),
	), nil
}

// Step 2: Process payment (receives PaymentInfo), then suspend waiting for shipping.
func processPayment(ctx context.Context, state *State, payment PaymentInfo) (*durable.Suspend[State], error) {
	fmt.Printf("charging card ending in %s\n", payment.CardNumber[len(payment.CardNumber)-4:])

	// Simulate payment processing.
	state.PaymentID = fmt.Sprintf("PAY-%d", time.Now().UnixMilli())
	state.Status = "paid"

	// Suspend: wait for the client to send shipping info.
	return durable.Select(
		durable.Receive[State, ShippingInfo]("shipping", processShipping),
		durable.AfterFunc(24*time.Hour, handleShippingTimeout),
	), nil
}

// Step 3: Process shipping (receives ShippingInfo), complete the process.
func processShipping(ctx context.Context, state *State, shipping ShippingInfo) (*durable.Suspend[State], error) {
	fmt.Printf("shipping to %s, %s %s\n", shipping.Address, shipping.City, shipping.Zip)

	state.ConfirmationID = fmt.Sprintf("CONF-%s-%d", state.ItemID, time.Now().UnixMilli())
	state.Status = "confirmed"

	return nil, nil // process complete
}

func handleReservationTimeout(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Println("reservation expired, no payment received")
	state.Status = "expired"
	return nil, nil
}

func handleShippingTimeout(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Println("shipping info not provided in time, refunding payment")
	state.Status = "refunded"
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("booking-queue")
	w.Register(process)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	// Step 1: Start the process with initial args.
	if err := client.Start(ctx, "booking-123", process, StartArgs{
		UserID: "user-42",
		ItemID: "SKU-900",
	}); err != nil {
		log.Fatal(err)
	}

	// Step 2: Send payment info (the process is suspended waiting for this).
	if err := client.SendMessage(ctx, "booking-123", "payment", PaymentInfo{
		CardNumber: "4111111111111234",
		Expiry:     "12/27",
	}); err != nil {
		log.Fatal(err)
	}

	// Step 3: Send shipping info (the process is again suspended waiting for this).
	if err := client.SendMessage(ctx, "booking-123", "shipping", ShippingInfo{
		Address: "123 Main St",
		City:    "Springfield",
		Zip:     "62704",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the process to complete and get the final state.
	var result State
	if err := client.GetResult(ctx, "booking-123", &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("booking complete: status=%s confirmation=%s\n", result.Status, result.ConfirmationID)
}
