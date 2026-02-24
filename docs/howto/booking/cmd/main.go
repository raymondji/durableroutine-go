package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/backend/temporal"
	"github.com/raymondji/durableroutine-go/docs/howto/booking"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &booking.BookingService{}

	w := durable.NewWorker("booking-queue")
	booking.RegisterHandlers(w, svc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "booking-queue"))

	// Start the booking routine.
	h, err := durable.Go(client, ctx, "booking-123", svc.ReserveItem,
		booking.BookingState{UserID: "user-42", ItemID: "SKU-900"})
	if err != nil {
		log.Fatal(err)
	}

	// Query the current status.
	status, err := durable.Query(client, ctx, "booking-123", booking.StatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	// Send payment info.
	if err := durable.Send(client, ctx, "booking-123", svc.ProcessPayment, booking.PaymentInfo{
		CardNumber: "4111111111111234",
		Expiry:     "12/27",
	}); err != nil {
		log.Fatal(err)
	}

	// Send shipping info.
	if err := durable.Send(client, ctx, "booking-123", svc.ProcessShipping, booking.ShippingInfo{
		Address: "123 Main St",
		City:    "Springfield",
		Zip:     "62704",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the routine to complete and get the result.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("booking result: %s\n", result.Status)
}
