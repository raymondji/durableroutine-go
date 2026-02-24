package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/order"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &order.OrderService{
		ExpireTimeout: 5 * time.Second,
		ShipTimeout:   1 * time.Millisecond,
	}

	w := durable.NewWorker("order-queue")
	order.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		h, err := durable.Go(client, ctx, "order-123", svc.CreateOrder, order.OrderState{})
		if err != nil {
			log.Fatal(err)
		}

		// Let the handler goroutine run before querying.
		time.Sleep(10 * time.Millisecond)

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

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("order result: %s\n", result.Status)
	})
}
