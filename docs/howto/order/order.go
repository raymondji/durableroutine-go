// Package order demonstrates a durable stateroutine that waits for messages
// using Select/OnSend, modelling an order lifecycle with Send + timer + Query.
// Uses struct-based handlers for dependency injection.
package order

import (
	"fmt"
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
	ExpireTimeout time.Duration // if zero, defaults to 30min
	ShipTimeout   time.Duration // if zero, defaults to 24h
}

func (s *OrderService) CreateOrder(ctx *stateroutine.Context, _ OrderState) (*stateroutine.Suspend[OrderResult], error) {
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "pending"})
	expireTimeout := 30 * time.Minute
	if s.ExpireTimeout > 0 {
		expireTimeout = s.ExpireTimeout
	}
	return stateroutine.Select[OrderResult](
		stateroutine.OnSend(s.PlaceOrder, PendingState{}),
		stateroutine.OnTimer(expireTimeout, s.ExpireOrder, PendingState{}),
	), nil
}

func (s *OrderService) PlaceOrder(ctx *stateroutine.Context, _ PendingState, req PlaceOrderReq) (*stateroutine.Suspend[OrderResult], error) {
	fmt.Printf("placing order %s\n", req.OrderID)
	placed := PlacedState{OrderID: req.OrderID, Items: req.Items}

	shipTimeout := 24 * time.Hour
	if s.ShipTimeout > 0 {
		shipTimeout = s.ShipTimeout
	}
	stateroutine.SetQueryResult(ctx, StatusResp{Status: "placed", OrderID: req.OrderID})
	return stateroutine.Select[OrderResult](
		stateroutine.OnSend(s.CancelOrder, placed),
		stateroutine.OnTimer(shipTimeout, s.ShipOrder, placed),
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

// RegisterHandlers registers all order handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *OrderService) {
	stateroutine.RegisterHandler(w, svc.CreateOrder, stateroutine.HandlerOptions{})
	stateroutine.RegisterSendHandler(w, svc.PlaceOrder, stateroutine.HandlerOptions{})
	stateroutine.RegisterSendHandler(w, svc.CancelOrder, stateroutine.HandlerOptions{})
	stateroutine.RegisterHandler(w, svc.ShipOrder, stateroutine.HandlerOptions{})
	stateroutine.RegisterHandler(w, svc.ExpireOrder, stateroutine.HandlerOptions{})
}
