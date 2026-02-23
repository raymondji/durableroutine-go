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

// --- State ---

type OrderState struct{}

func (OrderState) Kind() string { return "order" }

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

// --- Result ---

type OrderResult struct {
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

func (s *OrderService) CreateOrder(ctx *durable.Context, _ OrderState) (*durable.Suspend[OrderResult], error) {
	durable.SetQueryHandler(ctx, s.GetPendingStatus, PendingState{})
	return durable.Select[OrderResult](
		durable.OnCast(s.PlaceOrder, PendingState{}),
		durable.OnTimer(30*time.Minute, s.ExpireOrder, PendingState{}),
	), nil
}

func (s *OrderService) PlaceOrder(ctx *durable.Context, _ PendingState, req PlaceOrderReq) (*durable.Suspend[OrderResult], error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	durable.SetQueryHandler(ctx, s.GetPlacedStatus, placed)
	return durable.Select[OrderResult](
		durable.OnCast(s.CancelOrder, placed),
		durable.OnTimer(24*time.Hour, s.ShipOrder, placed),
	), nil
}

func (s *OrderService) CancelOrder(ctx *durable.Context, state PlacedState, _ CancelOrderReq) (*durable.Suspend[OrderResult], error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	return durable.Done(OrderResult{Status: "cancelled", OrderID: state.OrderID}), nil
}

func (s *OrderService) ShipOrder(ctx *durable.Context, state PlacedState) (*durable.Suspend[OrderResult], error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	return durable.Done(OrderResult{Status: "shipped", OrderID: state.OrderID}), nil
}

func (s *OrderService) ExpireOrder(ctx *durable.Context, _ PendingState) (*durable.Suspend[OrderResult], error) {
	fmt.Println("order timed out, no placement received")
	return durable.Done(OrderResult{Status: "timed_out"}), nil
}

func (s *OrderService) GetPendingStatus(ctx *durable.Context, _ PendingState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "pending"}, nil
}

func (s *OrderService) GetPlacedStatus(ctx *durable.Context, state PlacedState, _ StatusReq) (StatusResp, error) {
	return StatusResp{Status: "placed", OrderID: state.OrderID}, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &OrderService{}

	w := durable.NewWorker("order-queue")
	durable.AddHandler(w, svc.CreateOrder)
	durable.AddCastHandler(w, svc.PlaceOrder)
	durable.AddCastHandler(w, svc.CancelOrder)
	durable.AddHandler(w, svc.ShipOrder)
	durable.AddHandler(w, svc.ExpireOrder)
	durable.AddQueryHandler(w, svc.GetPendingStatus)
	durable.AddQueryHandler(w, svc.GetPlacedStatus)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	h, err := durable.Start(client, ctx, "order-123", svc.CreateOrder, OrderState{})
	if err != nil {
		log.Fatal(err)
	}

	status, err := durable.ClientQuery(client, ctx, "order-123", svc.GetPendingStatus, StatusReq{})
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

	// Wait for the routine to complete and get the result.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("order result: %s\n", result.Status)
}
