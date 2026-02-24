package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/booking"
	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &booking.BookingService{
		ReservationTimeout: 5 * time.Second,
		ShippingTimeout:    5 * time.Second,
	}

	w := durable.NewWorker("booking-queue")
	booking.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		h, err := durable.Go(client, ctx, "booking-123", svc.ReserveItem,
			booking.BookingState{UserID: "user-42", ItemID: "SKU-900"})
		if err != nil {
			log.Fatal(err)
		}

		// Let the handler goroutine run before querying.
		time.Sleep(10 * time.Millisecond)

		status, err := durable.Query(client, ctx, "booking-123", booking.StatusResp{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("status: %s\n", status.Status)

		if err := durable.Send(client, ctx, "booking-123", svc.ProcessPayment, booking.PaymentInfo{
			CardNumber: "4111111111111234",
			Expiry:     "12/27",
		}); err != nil {
			log.Fatal(err)
		}

		if err := durable.Send(client, ctx, "booking-123", svc.ProcessShipping, booking.ShippingInfo{
			Address: "123 Main St",
			City:    "Springfield",
			Zip:     "62704",
		}); err != nil {
			log.Fatal(err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("booking result: %s\n", result.Status)
	})
}
