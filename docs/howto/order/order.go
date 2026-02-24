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

type OrderInput struct{}

func (OrderInput) DurableKind() string { return "order" }

// --- Messages ---

type PlaceOrderReq struct {
	OrderID       string
	Items         []string
	PaymentMethod string
	Total         float64
}

func (PlaceOrderReq) DurableKind() string { return "place" }

type CancelOrderReq struct {
	Reason string
}

func (CancelOrderReq) DurableKind() string { return "cancel" }

type StatusResp struct {
	Status  string
	OrderID string
}

func (StatusResp) DurableKind() string { return "get-order-status" }

// --- Result ---

type OrderResult struct {
	Status  string
	OrderID string
}

func (OrderResult) DurableKind() string { return "order-result" }

// --- Per-step state types ---

type PendingInput struct{}

func (PendingInput) DurableKind() string { return "order.pending" }

type PlacedInput struct {
	OrderID string
	Items   []string
}

func (PlacedInput) DurableKind() string { return "order.placed" }

// --- Service struct ---

type OrderService struct {
	// Injected dependencies would go here.
	ExpireTimeout time.Duration // if zero, defaults to 30min
	ShipTimeout   time.Duration // if zero, defaults to 24h
}

func (s *OrderService) CreateOrder(ctx *durable.Context, _ OrderInput) (*durable.Continuation[OrderResult], error) {
	durable.SetQueryResult(ctx, StatusResp{Status: "pending"})
	expireTimeout := 30 * time.Minute
	if s.ExpireTimeout > 0 {
		expireTimeout = s.ExpireTimeout
	}
	return durable.Select(
		durable.ReceiveSend(s.PlaceOrder, PendingInput{}),
		durable.After(expireTimeout, s.ExpireOrder, PendingInput{}),
	), nil
}

func (s *OrderService) PlaceOrder(ctx *durable.Context, _ PendingInput, externalInput PlaceOrderReq) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("placing order %s\n", externalInput.OrderID)
	placed := PlacedInput{OrderID: externalInput.OrderID, Items: externalInput.Items}

	shipTimeout := 24 * time.Hour
	if s.ShipTimeout > 0 {
		shipTimeout = s.ShipTimeout
	}
	durable.SetQueryResult(ctx, StatusResp{Status: "placed", OrderID: externalInput.OrderID})
	return durable.Select(
		durable.ReceiveSend(s.CancelOrder, placed),
		durable.After(shipTimeout, s.ShipOrder, placed),
	), nil
}

func (s *OrderService) CancelOrder(ctx *durable.Context, input PlacedInput, _ CancelOrderReq) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("cancelling order %s\n", input.OrderID)
	return durable.Done(OrderResult{Status: "cancelled", OrderID: input.OrderID}), nil
}

func (s *OrderService) ShipOrder(ctx *durable.Context, input PlacedInput) (*durable.Continuation[OrderResult], error) {
	fmt.Printf("shipping order %s\n", input.OrderID)
	return durable.Done(OrderResult{Status: "shipped", OrderID: input.OrderID}), nil
}

func (s *OrderService) ExpireOrder(ctx *durable.Context, _ PendingInput) (*durable.Continuation[OrderResult], error) {
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
