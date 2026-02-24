package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/examples/saga"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	svc := &saga.TripService{}

	w := stateroutine.NewWorker("saga-queue")
	saga.RegisterHandlers(w, svc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	h, err := stateroutine.Start(client, ctx, "trip-789", svc.BookFlight, saga.TripState{
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
