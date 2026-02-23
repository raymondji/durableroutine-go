// Command order demonstrates a durable routine that waits for messages
// using Select/OnSend, modelling an order lifecycle with Send + timer + Query.
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
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

type StatusResp struct {
	Status  string
	OrderID string
}

func (StatusResp) Kind() string { return "get-order-status" }

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

func (s *OrderService) CreateOrder(ctx *stateroutine.Context, _ OrderState) (*stateroutine.Suspend[OrderResult], error) {
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "pending"})
	return stateroutine.Select[OrderResult](
		stateroutine.OnSend(s.PlaceOrder, PendingState{}),
		stateroutine.OnTimer(30*time.Minute, s.ExpireOrder, PendingState{}),
	), nil
}

func (s *OrderService) PlaceOrder(ctx *stateroutine.Context, _ PendingState, req PlaceOrderReq) (*stateroutine.Suspend[OrderResult], error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	stateroutine.SetQueryResult(ctx, StatusResp{Status: "placed", OrderID: req.OrderID})
	return stateroutine.Select[OrderResult](
		stateroutine.OnSend(s.CancelOrder, placed),
		stateroutine.OnTimer(24*time.Hour, s.ShipOrder, placed),
	), nil
}

func (s *OrderService) CancelOrder(ctx *stateroutine.Context, state PlacedState, _ CancelOrderReq) (*stateroutine.Suspend[OrderResult], error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	return stateroutine.Done(OrderResult{Status: "cancelled", OrderID: state.OrderID}), nil
}

func (s *OrderService) ShipOrder(ctx *stateroutine.Context, state PlacedState) (*stateroutine.Suspend[OrderResult], error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	return stateroutine.Done(OrderResult{Status: "shipped", OrderID: state.OrderID}), nil
}

func (s *OrderService) ExpireOrder(ctx *stateroutine.Context, _ PendingState) (*stateroutine.Suspend[OrderResult], error) {
	fmt.Println("order timed out, no placement received")
	return stateroutine.Done(OrderResult{Status: "timed_out"}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &OrderService{}

	w := stateroutine.NewWorker("order-queue")
	stateroutine.AddHandler(w, svc.CreateOrder, stateroutine.ErrorPolicy{})
	stateroutine.AddSendHandler(w, svc.PlaceOrder, stateroutine.ErrorPolicy{})
	stateroutine.AddSendHandler(w, svc.CancelOrder, stateroutine.ErrorPolicy{})
	stateroutine.AddHandler(w, svc.ShipOrder, stateroutine.ErrorPolicy{})
	stateroutine.AddHandler(w, svc.ExpireOrder, stateroutine.ErrorPolicy{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	h, err := stateroutine.Start(client, ctx, "order-123", svc.CreateOrder, OrderState{})
	if err != nil {
		log.Fatal(err)
	}

	status, err := stateroutine.ClientQuery(client, ctx, "order-123", StatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	if err := stateroutine.ClientSend(client, ctx, "order-123", PlaceOrderReq{
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
