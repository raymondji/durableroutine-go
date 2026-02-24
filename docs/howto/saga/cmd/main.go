package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/docs/howto/saga"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/backend/temporal"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &saga.TripService{}

	w := durable.NewWorker("saga-queue")
	saga.RegisterHandlers(w, svc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "saga-queue"))

	h, err := durable.Go(client, ctx, "trip-789", svc.BookFlight, saga.TripState{
		TripID:      "TRIP-789",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the trip booking to complete.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("trip booked: flight=%s hotel=%s car=%s\n",
		result.FlightConfirmation, result.HotelConfirmation, result.CarConfirmation)
}
