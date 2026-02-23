// Command saga demonstrates the SAGA compensation pattern with checkpointing
// between each step. Each booking runs as its own activity with independent
// retries. Continue checkpoints state between steps so that if the worker
// crashes after booking a flight but before booking a hotel, the flight
// confirmation is preserved and the saga resumes from the hotel step.
//
// Compare with Temporal's saga sample:
// https://github.com/temporalio/samples-go/blob/main/saga/workflow.go
//
// In Temporal's model, each step is an activity and the workflow orchestrates
// them. Here, each step is a handler connected via Continue. The effect is the
// same — each step is individually retried and state is checkpointed — but
// the user writes plain Go code without replay-safety constraints.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Per-step state types ---
// Each step carries the original trip details plus accumulated confirmations.
// Continue checkpoints these between steps.

type TripState struct {
	TripID      string
	FlightID    string
	HotelID     string
	CarRentalID string
}

func (TripState) Kind() string { return "trip-booking" }

type FlightBookedState struct {
	TripID             string
	HotelID            string
	CarRentalID        string
	FlightConfirmation string
}

func (FlightBookedState) Kind() string { return "trip.flight-booked" }

type HotelBookedState struct {
	TripID             string
	CarRentalID        string
	FlightConfirmation string
	HotelConfirmation  string
}

func (HotelBookedState) Kind() string { return "trip.hotel-booked" }

// --- Result ---

type TripResult struct {
	FlightConfirmation string
	HotelConfirmation  string
	CarConfirmation    string
}

// --- Service struct ---

type TripService struct {
	// Injected dependencies would go here (e.g., flight/hotel/car API clients).
}

// BookFlight is the entry point. Books a flight and checkpoints the
// confirmation before proceeding to the hotel.
func (s *TripService) BookFlight(ctx *durable.Context, state TripState) (*durable.Suspend[TripResult], error) {
	flightConf, err := s.bookFlight(ctx, state.FlightID)
	if err != nil {
		return nil, fmt.Errorf("book flight: %w", err)
	}

	fmt.Printf("trip %s: flight booked (%s)\n", state.TripID, flightConf)
	return durable.Continue(s.BookHotel, FlightBookedState{
		TripID:             state.TripID,
		HotelID:            state.HotelID,
		CarRentalID:        state.CarRentalID,
		FlightConfirmation: flightConf,
	}), nil
}

// BookHotel runs after the flight confirmation is checkpointed. On failure,
// compensates the flight booking.
func (s *TripService) BookHotel(ctx *durable.Context, state FlightBookedState) (*durable.Suspend[TripResult], error) {
	hotelConf, err := s.bookHotel(ctx, state.HotelID)
	if err != nil {
		s.cancelFlight(ctx, state.FlightConfirmation)
		return nil, fmt.Errorf("book hotel: %w", err)
	}

	fmt.Printf("trip %s: hotel booked (%s)\n", state.TripID, hotelConf)
	return durable.Continue(s.BookCar, HotelBookedState{
		TripID:             state.TripID,
		CarRentalID:        state.CarRentalID,
		FlightConfirmation: state.FlightConfirmation,
		HotelConfirmation:  hotelConf,
	}), nil
}

// BookCar runs after the hotel confirmation is checkpointed. On failure,
// compensates both hotel and flight bookings.
func (s *TripService) BookCar(ctx *durable.Context, state HotelBookedState) (*durable.Suspend[TripResult], error) {
	carConf, err := s.bookCar(ctx, state.CarRentalID)
	if err != nil {
		s.cancelHotel(ctx, state.HotelConfirmation)
		s.cancelFlight(ctx, state.FlightConfirmation)
		return nil, fmt.Errorf("book car: %w", err)
	}

	fmt.Printf("trip %s: car booked (%s), trip fully booked\n", state.TripID, carConf)
	return durable.Done(TripResult{
		FlightConfirmation: state.FlightConfirmation,
		HotelConfirmation:  state.HotelConfirmation,
		CarConfirmation:    carConf,
	}), nil
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
	durable.AddHandler(w, svc.BookFlight, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))
	durable.AddHandler(w, svc.BookHotel, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))
	durable.AddHandler(w, svc.BookCar, durable.WithRetryPolicy(durable.RetryPolicy{MaxAttempts: 3}))

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	h, err := durable.Start(client, ctx, "trip-789", svc.BookFlight, TripState{
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
