// Command saga demonstrates the SAGA compensation pattern. Because handlers
// run as activities, you write sequential calls with normal Go error handling
// and invoke compensations on failure — no special SAGA framework needed.
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args ---

type TripArgs struct {
	TripID      string
	FlightID    string
	HotelID     string
	CarRentalID string
}

func (TripArgs) Kind() string { return "trip-booking" }

// --- Service struct ---

type TripService struct {
	// Injected dependencies would go here (e.g., flight/hotel/car API clients).
}

func (s *TripService) Handle(ctx *durable.Context, args TripArgs) (*durable.Suspend, error) {
	// Step 1: Book flight
	flightConf, err := s.bookFlight(ctx, args.FlightID)
	if err != nil {
		return nil, fmt.Errorf("book flight: %w", err)
	}

	// Step 2: Book hotel — compensate flight on failure
	hotelConf, err := s.bookHotel(ctx, args.HotelID)
	if err != nil {
		s.cancelFlight(ctx, flightConf)
		return nil, fmt.Errorf("book hotel: %w", err)
	}

	// Step 3: Book car — compensate hotel and flight on failure
	_, err = s.bookCar(ctx, args.CarRentalID)
	if err != nil {
		s.cancelHotel(ctx, hotelConf)
		s.cancelFlight(ctx, flightConf)
		return nil, fmt.Errorf("book car: %w", err)
	}

	fmt.Printf("trip %s fully booked\n", args.TripID)
	return nil, nil
}

// --- Service calls (replace with real API clients) ---

func (s *TripService) bookFlight(_ context.Context, flightID string) (string, error) {
	if flightID == "" {
		return "", errors.New("flight ID required")
	}
	return "FLIGHT-CONF-" + flightID, nil
}

func (s *TripService) bookHotel(_ context.Context, hotelID string) (string, error) {
	if hotelID == "" {
		return "", errors.New("hotel ID required")
	}
	return "HOTEL-CONF-" + hotelID, nil
}

func (s *TripService) bookCar(_ context.Context, carID string) (string, error) {
	if carID == "" {
		return "", errors.New("car rental ID required")
	}
	return "CAR-CONF-" + carID, nil
}

func (s *TripService) cancelFlight(_ context.Context, confirmation string) {
	fmt.Printf("compensating: cancelling flight %s\n", confirmation)
}

func (s *TripService) cancelHotel(_ context.Context, confirmation string) {
	fmt.Printf("compensating: cancelling hotel %s\n", confirmation)
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &TripService{}

	w := durable.NewWorker("saga-queue")
	durable.AddRoutineHandler(w, svc.Handle, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	if err := durable.Start(client, ctx, "trip-789", TripArgs{
		TripID:      "TRIP-789",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("trip booking started")
}
