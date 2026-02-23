// Command order demonstrates a durable routine that waits for messages
// using Select/OnCast, modelling an order lifecycle with Cast + timer + Query.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args & descriptors ---

type OrderArgs struct{}

func (OrderArgs) Kind() string { return "order" }

var PlaceOrderInbox = durable.Inbox[PlaceOrderReq]{Name: "place"}
var CancelOrderInbox = durable.Inbox[CancelOrderReq]{Name: "cancel"}
var GetOrderStatus = durable.Query[StatusReq, StatusResp]{Name: "get-order-status"}

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

type StatusReq struct{}
type StatusResp struct {
	Status  string
	OrderID string
}

// --- Per-step state types ---

type PendingState struct{}

type PlacedState struct {
	OrderID string
	Items   []string
}

// --- Handlers ---

func waitForOrder(ctx *durable.Context, _ OrderArgs) (*durable.Suspend, error) {
	return durable.Select(
		durable.OnCast(PlaceOrderInbox, handlePlace, PendingState{}),
		durable.OnQuery(GetOrderStatus, queryPendingStatus, PendingState{}),
		durable.AfterFunc(30*time.Minute, handleTimeout, PendingState{}),
	), nil
}

func handlePlace(ctx *durable.Context, _ PendingState, req PlaceOrderReq) (*durable.Suspend, error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	return durable.Select(
		durable.OnCast(CancelOrderInbox, handleCancel, placed),
		durable.OnQuery(GetOrderStatus, queryPlacedStatus, placed),
		durable.AfterFunc(24*time.Hour, handleShip, placed),
	), nil
}

func handleCancel(ctx *durable.Context, state PlacedState, _ CancelOrderReq) (*durable.Suspend, error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	return nil, nil
}

func handleShip(ctx *durable.Context, state PlacedState) (*durable.Suspend, error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	return nil, nil
}

func handleTimeout(ctx *durable.Context, _ PendingState) (*durable.Suspend, error) {
	fmt.Println("order timed out, no placement received")
	return nil, nil
}

func queryPendingStatus(ctx *durable.Context, _ PendingState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "pending"}, nil
}

func queryPlacedStatus(ctx *durable.Context, state PlacedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "placed", OrderID: state.OrderID}, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	workers := durable.NewWorkers()
	durable.AddRoutine(workers, waitForOrder)

	w := durable.NewWorker("order-queue", workers)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	if err := durable.Start(client, ctx, "order-123", OrderArgs{}); err != nil {
		log.Fatal(err)
	}

	status, err := durable.ClientQuery(client, ctx, "order-123", GetOrderStatus, StatusReq{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	if err := durable.ClientCast(client, ctx, "order-123", PlaceOrderInbox, PlaceOrderReq{
		OrderID:       "ORD-456",
		Items:         []string{"widget-a", "widget-b"},
		PaymentMethod: "card",
		Total:         99.99,
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("order placed, will ship in 24h or be cancelled")
}
