// Command order demonstrates a durable routine that waits for messages
// using Select/OnCast, modelling an order lifecycle with Cast + timer + Query.
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args ---

type OrderArgs struct{}

func (OrderArgs) Kind() string { return "order" }

// --- Messages ---

type PlaceOrderReq struct {
	OrderID       string
	Items         []string
	PaymentMethod string
	Total         float64
}

func (PlaceOrderReq) Kind() string { return "place" }

type CancelOrderReq struct {
	Reason string
}

func (CancelOrderReq) Kind() string { return "cancel" }

type StatusReq struct{}

func (StatusReq) Kind() string { return "get-order-status" }

type StatusResp struct {
	Status  string
	OrderID string
}

// --- Per-step state types ---

type PendingState struct{}

func (PendingState) Kind() string { return "order.pending" }

type PlacedState struct {
	OrderID string
	Items   []string
}

func (PlacedState) Kind() string { return "order.placed" }

// --- Service struct ---

type OrderService struct {
	// Injected dependencies would go here.
}

func (s *OrderService) Handle(ctx *durable.Context, _ OrderArgs) (*durable.Suspend, error) {
	return durable.Select(
		durable.OnCast(s.HandlePlace, PendingState{}),
		durable.OnQuery(s.QueryPendingStatus, PendingState{}),
		durable.AfterFunc(30*time.Minute, s.HandleTimeout, PendingState{}),
	), nil
}

func (s *OrderService) HandlePlace(ctx *durable.Context, _ PendingState, req PlaceOrderReq) (*durable.Suspend, error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	return durable.Select(
		durable.OnCast(s.HandleCancel, placed),
		durable.OnQuery(s.QueryPlacedStatus, placed),
		durable.AfterFunc(24*time.Hour, s.HandleShip, placed),
	), nil
}

func (s *OrderService) HandleCancel(ctx *durable.Context, state PlacedState, _ CancelOrderReq) (*durable.Suspend, error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	return durable.Done(), nil
}

func (s *OrderService) HandleShip(ctx *durable.Context, state PlacedState) (*durable.Suspend, error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	return durable.Done(), nil
}

func (s *OrderService) HandleTimeout(ctx *durable.Context, _ PendingState) (*durable.Suspend, error) {
	fmt.Println("order timed out, no placement received")
	return durable.Done(), nil
}

func (s *OrderService) QueryPendingStatus(ctx *durable.Context, _ PendingState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "pending"}, nil
}

func (s *OrderService) QueryPlacedStatus(ctx *durable.Context, state PlacedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "placed", OrderID: state.OrderID}, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &OrderService{}

	w := durable.NewWorker("order-queue")
	durable.AddRoutineHandler(w, svc.Handle)
	durable.AddCastHandler(w, svc.HandlePlace)
	durable.AddCastHandler(w, svc.HandleCancel)
	durable.AddHandler(w, svc.HandleShip)
	durable.AddHandler(w, svc.HandleTimeout)
	durable.AddQueryHandler(w, svc.QueryPendingStatus)
	durable.AddQueryHandler(w, svc.QueryPlacedStatus)

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

	status, err := durable.ClientQuery(client, ctx, "order-123", svc.QueryPendingStatus, StatusReq{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	if err := durable.ClientCast(client, ctx, "order-123", PlaceOrderReq{
		OrderID:       "ORD-456",
		Items:         []string{"widget-a", "widget-b"},
		PaymentMethod: "card",
		Total:         99.99,
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("order placed, will ship in 24h or be cancelled")
}
