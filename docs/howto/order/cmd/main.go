package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/backend/temporal"
	"github.com/raymondji/durableroutine-go/docs/howto/order"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &order.OrderService{}

	w := durable.NewWorker("order-queue")
	order.RegisterHandlers(w, svc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "order-queue"))

	h, err := durable.Go(client, ctx, "order-123", svc.CreateOrder, order.OrderState{})
	if err != nil {
		log.Fatal(err)
	}

	status, err := durable.Query(client, ctx, "order-123", order.StatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	if err := durable.Send(client, ctx, "order-123", svc.PlaceOrder, order.PlaceOrderReq{
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
