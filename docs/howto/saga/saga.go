// Package saga demonstrates the SAGA compensation pattern using terminal error
// handlers. Each booking step runs as its own activity with independent retries.
// When all retries are exhausted, the terminal error handler runs compensation
// logic (cancelling previously booked services) instead of failing the stateroutine.
//
// Continue checkpoints state between steps so that if the worker crashes after
// booking a flight but before booking a hotel, the flight confirmation is
// preserved and the saga resumes from the hotel step.
package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- Per-step state types ---

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

func (s *TripService) BookFlight(ctx *stateroutine.Context, state TripState) (*stateroutine.Suspend[TripResult], error) {
	flightConf, err := s.bookFlight(ctx, state.FlightID)
	if err != nil {
		return nil, fmt.Errorf("book flight: %w", err)
	}

	fmt.Printf("trip %s: flight booked (%s)\n", state.TripID, flightConf)
	return stateroutine.Continue(s.BookHotel, FlightBookedState{
		TripID:             state.TripID,
		HotelID:            state.HotelID,
		CarRentalID:        state.CarRentalID,
		FlightConfirmation: flightConf,
	}), nil
}

func (s *TripService) BookHotel(ctx *stateroutine.Context, state FlightBookedState) (*stateroutine.Suspend[TripResult], error) {
	hotelConf, err := s.bookHotel(ctx, state.HotelID)
	if err != nil {
		return nil, fmt.Errorf("book hotel: %w", err)
	}

	fmt.Printf("trip %s: hotel booked (%s)\n", state.TripID, hotelConf)
	return stateroutine.Continue(s.BookCar, HotelBookedState{
		TripID:             state.TripID,
		CarRentalID:        state.CarRentalID,
		FlightConfirmation: state.FlightConfirmation,
		HotelConfirmation:  hotelConf,
	}), nil
}

func (s *TripService) CompensateHotel(ctx *stateroutine.Context, state FlightBookedState, err error) (*stateroutine.Suspend[TripResult], error) {
	s.cancelFlight(ctx, state.FlightConfirmation)
	return nil, fmt.Errorf("book hotel failed, compensated flight: %w", err)
}

func (s *TripService) BookCar(ctx *stateroutine.Context, state HotelBookedState) (*stateroutine.Suspend[TripResult], error) {
	carConf, err := s.bookCar(ctx, state.CarRentalID)
	if err != nil {
		return nil, fmt.Errorf("book car: %w", err)
	}

	fmt.Printf("trip %s: car booked (%s), trip fully booked\n", state.TripID, carConf)
	return stateroutine.Done(TripResult{
		FlightConfirmation: state.FlightConfirmation,
		HotelConfirmation:  state.HotelConfirmation,
		CarConfirmation:    carConf,
	}), nil
}

func (s *TripService) CompensateCar(ctx *stateroutine.Context, state HotelBookedState, err error) (*stateroutine.Suspend[TripResult], error) {
	s.cancelHotel(ctx, state.HotelConfirmation)
	s.cancelFlight(ctx, state.FlightConfirmation)
	return nil, fmt.Errorf("book car failed, compensated hotel and flight: %w", err)
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

// RegisterHandlers registers all saga handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *TripService) {
	stateroutine.AddHandler(w, svc.BookFlight, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	})
	stateroutine.AddHandler(w, svc.BookHotel, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	}).OnTerminalError(svc.CompensateHotel)
	stateroutine.AddHandler(w, svc.BookCar, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	}).OnTerminalError(svc.CompensateCar)
}
