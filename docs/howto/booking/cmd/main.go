package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/docs/howto/booking"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	svc := &booking.BookingService{}

	w := stateroutine.NewWorker("booking-queue")
	booking.RegisterHandlers(w, svc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	// Start the booking stateroutine.
	h, err := stateroutine.Start(client, ctx, "booking-123", svc.ReserveItem,
		booking.BookingState{UserID: "user-42", ItemID: "SKU-900"})
	if err != nil {
		log.Fatal(err)
	}

	// Query the current status.
	status, err := stateroutine.ClientQuery(client, ctx, "booking-123", booking.StatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", status.Status)

	// Send payment info.
	if err := stateroutine.ClientSend(client, ctx, "booking-123", svc.ProcessPayment, booking.PaymentInfo{
		CardNumber: "4111111111111234",
		Expiry:     "12/27",
	}); err != nil {
		log.Fatal(err)
	}

	// Send shipping info.
	if err := stateroutine.ClientSend(client, ctx, "booking-123", svc.ProcessShipping, booking.ShippingInfo{
		Address: "123 Main St",
		City:    "Springfield",
		Zip:     "62704",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the stateroutine to complete and get the result.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("booking result: %s\n", result.Status)
}
