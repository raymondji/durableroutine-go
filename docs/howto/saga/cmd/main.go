package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/saga"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &saga.TripService{}

	w := durable.NewWorker("saga-queue")
	saga.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		h, err := durable.Go(client, ctx, "trip-789", svc.BookFlight, saga.TripInput{
			TripID:      "TRIP-789",
			FlightID:    "FL-100",
			HotelID:     "HT-200",
			CarRentalID: "CR-300",
		})
		if err != nil {
			log.Fatal(err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("trip booked: flight=%s hotel=%s car=%s\n",
			result.FlightConfirmation, result.HotelConfirmation, result.CarConfirmation)
	})
}
