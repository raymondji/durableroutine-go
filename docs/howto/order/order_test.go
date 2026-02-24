package order_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/order"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
)

func TestOrderHappyPath(t *testing.T) {
	svc := &order.OrderService{
		ShipTimeout: 1 * time.Millisecond,
	}
	env := testenv.Setup(t, func(w *stateroutine.Worker) {
		order.RegisterHandlers(w, svc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := env.UniqueID("order-happy")
	h, err := stateroutine.Start(env.Client, ctx, id, svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for workflow to reach Select
	time.Sleep(2 * time.Second)

	err = stateroutine.ClientSend(env.Client, ctx, id, svc.PlaceOrder, order.PlaceOrderReq{
		OrderID: "ORD-1", Items: []string{"widget"}, PaymentMethod: "card", Total: 99.99,
	})
	if err != nil {
		t.Fatalf("ClientSend PlaceOrder failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Status != "shipped" {
		t.Fatalf("expected status 'shipped', got %q", result.Status)
	}
	t.Logf("Order happy path: %+v", result)
}

func TestOrderCancel(t *testing.T) {
	svc := &order.OrderService{}
	env := testenv.Setup(t, func(w *stateroutine.Worker) {
		order.RegisterHandlers(w, svc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := env.UniqueID("order-cancel")
	h, err := stateroutine.Start(env.Client, ctx, id, svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	err = stateroutine.ClientSend(env.Client, ctx, id, svc.PlaceOrder, order.PlaceOrderReq{
		OrderID: "ORD-2", Items: []string{"gadget"}, PaymentMethod: "card", Total: 49.99,
	})
	if err != nil {
		t.Fatalf("ClientSend PlaceOrder failed: %v", err)
	}

	// Wait for PlaceOrder to process and reach the next Select
	time.Sleep(2 * time.Second)

	err = stateroutine.ClientSend(env.Client, ctx, id, svc.CancelOrder, order.CancelOrderReq{Reason: "changed mind"})
	if err != nil {
		t.Fatalf("ClientSend CancelOrder failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Status != "cancelled" {
		t.Fatalf("expected status 'cancelled', got %q", result.Status)
	}
	t.Logf("Order cancel: %+v", result)
}

func TestOrderExpire(t *testing.T) {
	svc := &order.OrderService{
		ExpireTimeout: 1 * time.Millisecond,
	}
	env := testenv.Setup(t, func(w *stateroutine.Worker) {
		order.RegisterHandlers(w, svc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("order-expire"), svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Status != "timed_out" {
		t.Fatalf("expected status 'timed_out', got %q", result.Status)
	}
	t.Logf("Order expire: %+v", result)
}
