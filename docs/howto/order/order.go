// Package order demonstrates a durable routine that waits for messages
// using Select/ReceiveSend, modelling an order lifecycle with Send + timer + Query.
// Uses struct-based handlers for dependency injection.
package order

import (
	"fmt"
	"time"

	"github.com/raymondji/durableroutine-go/durable"
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
	ExpireTimeout time.Duration // if zero, defaults to 30min
	ShipTimeout   time.Duration // if zero, defaults to 24h
}

func (s *OrderService) CreateOrder(ctx *durable.Context, _ OrderState) (*durable.Continuation[OrderResult], error) {
	durable.SetQueryResult(ctx, StatusResp{Status: "pending"})
	expireTimeout := 30 * time.Minute
	if s.ExpireTimeout > 0 {
		expireTimeout = s.ExpireTimeout
	}
	return durable.Select(
		durable.ReceiveSend(s.PlaceOrder, PendingState{}),
		durable.After(expireTimeout, s.ExpireOrder, PendingState{}),
	), nil
}

func (s *OrderService) PlaceOrder(ctx *durable.Context, _ PendingState, req PlaceOrderReq) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	shipTimeout := 24 * time.Hour
	if s.ShipTimeout > 0 {
		shipTimeout = s.ShipTimeout
	}
	durable.SetQueryResult(ctx, StatusResp{Status: "placed", OrderID: req.OrderID})
	return durable.Select(
		durable.ReceiveSend(s.CancelOrder, placed),
		durable.After(shipTimeout, s.ShipOrder, placed),
	), nil
}

func (s *OrderService) CancelOrder(ctx *durable.Context, state PlacedState, _ CancelOrderReq) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("cancelling order %s\n", state.OrderID)
	return durable.Done(OrderResult{Status: "cancelled", OrderID: state.OrderID}), nil
}

func (s *OrderService) ShipOrder(ctx *durable.Context, state PlacedState) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("shipping order %s\n", state.OrderID)
	return durable.Done(OrderResult{Status: "shipped", OrderID: state.OrderID}), nil
}

func (s *OrderService) ExpireOrder(ctx *durable.Context, _ PendingState) (*durable.Continuation[OrderResult], error) {
	fmt.Println("order timed out, no placement received")
	return durable.Done(OrderResult{Status: "timed_out"}), nil
}

// RegisterHandlers registers all order handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *OrderService) {
	durable.RegisterHandler(w, svc.CreateOrder, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.PlaceOrder, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.CancelOrder, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.ShipOrder, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.ExpireOrder, durable.HandlerOptions{})
}
