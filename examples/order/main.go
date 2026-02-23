// Command order demonstrates a durable process that waits for messages
// using Select/Receive, modelling an order lifecycle.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Messages ---

type PlaceOrderReq struct {
	OrderID       string
	Items         []string
	PaymentMethod string
	Total         float64
}

type CancelOrderReq struct {
	Reason string
}

// --- State ---

type State struct {
	Status    string
	OrderID   string
	Items     []string
	PaymentID string
}

// --- Process definition ---

var process = durable.Process[State]{
	Name:      "order",
	InitState: func(_ any) State { return State{Status: "pending"} },
	Initial:   waitForOrder,
}

func waitForOrder(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	return durable.Select(
		durable.Receive[State, PlaceOrderReq]("place", handlePlace),
		durable.AfterFunc(30*time.Minute, handleTimeout),
	), nil
}

func handlePlace(ctx context.Context, state *State, req PlaceOrderReq) (*durable.Suspend[State], error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	state.OrderID = req.OrderID
	state.Items = req.Items
	state.Status = "placed"

	return durable.Select(
		durable.Receive[State, CancelOrderReq]("cancel", handleCancel),
		durable.AfterFunc(24*time.Hour, handleShip),
	), nil
}

func handleCancel(ctx context.Context, state *State, _ CancelOrderReq) (*durable.Suspend[State], error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	state.Status = "cancelled"
	return nil, nil
}

func handleShip(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	state.Status = "shipped"
	return nil, nil
}

func handleTimeout(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Println("order timed out, no placement received")
	state.Status = "timed_out"
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("order-queue")
	w.Register(process)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	if err := client.Start(ctx, "order-123", process, nil); err != nil {
		log.Fatal(err)
	}

	err := client.SendMessage(ctx, "order-123", "place", PlaceOrderReq{
		OrderID:       "ORD-456",
		Items:         []string{"widget-a", "widget-b"},
		PaymentMethod: "card",
		Total:         99.99,
	})
	if err != nil {
		log.Fatal(err)
	}

	var result State
	if err := client.GetResult(ctx, "order-123", &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("order complete: status=%s\n", result.Status)
}
