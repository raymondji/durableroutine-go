package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/docs/howto/order"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	svc := &order.OrderService{}

	w := stateroutine.NewWorker("order-queue")
	order.RegisterHandlers(w, svc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	h, err := stateroutine.Start(client, ctx, "order-123", svc.CreateOrder, order.OrderState{})
	if err != nil {
		log.Fatal(err)
	}

	status, err := stateroutine.ClientQuery(client, ctx, "order-123", order.StatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	if err := stateroutine.ClientSend(client, ctx, "order-123", svc.PlaceOrder, order.PlaceOrderReq{
		OrderID:       "ORD-456",
		Items:         []string{"widget-a", "widget-b"},
		PaymentMethod: "card",
		Total:         99.99,
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the stateroutine to complete and get the result.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("order result: %s\n", result.Status)
}
