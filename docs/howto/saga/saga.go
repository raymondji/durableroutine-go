// Package saga demonstrates the SAGA compensation pattern using terminal error
// handlers. Each booking step runs as its own activity with independent retries.
// When all retries are exhausted, the terminal error handler runs compensation
// logic (cancelling previously booked services) instead of failing the routine.
//
// Continue checkpoints state between steps so that if the worker crashes after
// booking a flight but before booking a hotel, the flight confirmation is
// preserved and the saga resumes from the hotel step.
package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- Per-step state types ---

type TripInput struct {
	TripID      string
	FlightID    string
	HotelID     string
	CarRentalID string
}

func (TripInput) DurableKind() string { return "trip-booking" }

type FlightBookedInput struct {
	TripID             string
	HotelID            string
	CarRentalID        string
	FlightConfirmation string
}

func (FlightBookedInput) DurableKind() string { return "trip.flight-booked" }

type HotelBookedInput struct {
	TripID             string
	CarRentalID        string
	FlightConfirmation string
	HotelConfirmation  string
}

func (HotelBookedInput) DurableKind() string { return "trip.hotel-booked" }

// --- Result ---

type TripResult struct {
	FlightConfirmation string
	HotelConfirmation  string
	CarConfirmation    string
}

func (TripResult) DurableKind() string { return "trip-result" }

// --- Service struct ---

type TripService struct {
	// Injected dependencies would go here (e.g., flight/hotel/car API clients).
	BookFlightFn func(ctx context.Context, flightID string) (string, error) // if non-nil, replaces default
	BookHotelFn  func(ctx context.Context, hotelID string) (string, error)  // if non-nil, replaces default
	BookCarFn    func(ctx context.Context, carID string) (string, error)    // if non-nil, replaces default
}

func (s *TripService) BookFlight(ctx *durable.Context, input TripInput) (*durable.Continuation[TripResult], error) {
	flightConf, err := s.bookFlight(ctx, input.FlightID)
	if err != nil {
		return nil, fmt.Errorf("book flight: %w", err)
	}

	fmt.Printf("trip %s: flight booked (%s)\n", input.TripID, flightConf)
	return durable.Continue(s.BookHotel, FlightBookedInput{
		TripID:             input.TripID,
		HotelID:            input.HotelID,
		CarRentalID:        input.CarRentalID,
		FlightConfirmation: flightConf,
	}), nil
}

func (s *TripService) BookHotel(ctx *durable.Context, input FlightBookedInput) (*durable.Continuation[TripResult], error) {
	hotelConf, err := s.bookHotel(ctx, input.HotelID)
	if err != nil {
		return nil, fmt.Errorf("book hotel: %w", err)
	}

	fmt.Printf("trip %s: hotel booked (%s)\n", input.TripID, hotelConf)
	return durable.Continue(s.BookCar, HotelBookedInput{
		TripID:             input.TripID,
		CarRentalID:        input.CarRentalID,
		FlightConfirmation: input.FlightConfirmation,
		HotelConfirmation:  hotelConf,
	}), nil
}

func (s *TripService) CompensateHotel(ctx *durable.Context, input FlightBookedInput, err error) (*durable.Continuation[TripResult], error) {
	s.cancelFlight(ctx, input.FlightConfirmation)
	return nil, fmt.Errorf("book hotel failed, compensated flight: %w", err)
}

func (s *TripService) BookCar(ctx *durable.Context, input HotelBookedInput) (*durable.Continuation[TripResult], error) {
	carConf, err := s.bookCar(ctx, input.CarRentalID)
	if err != nil {
		return nil, fmt.Errorf("book car: %w", err)
	}

	fmt.Printf("trip %s: car booked (%s), trip fully booked\n", input.TripID, carConf)
	return durable.Done(TripResult{
		FlightConfirmation: input.FlightConfirmation,
		HotelConfirmation:  input.HotelConfirmation,
		CarConfirmation:    carConf,
	}), nil
}

func (s *TripService) CompensateCar(ctx *durable.Context, input HotelBookedInput, err error) (*durable.Continuation[TripResult], error) {
	s.cancelHotel(ctx, input.HotelConfirmation)
	s.cancelFlight(ctx, input.FlightConfirmation)
	return nil, fmt.Errorf("book car failed, compensated hotel and flight: %w", err)
}

// --- Service calls (replace with real API clients) ---

func (s *TripService) bookFlight(ctx context.Context, flightID string) (string, error) {
	if s.BookFlightFn != nil {
		return s.BookFlightFn(ctx, flightID)
	}
	if flightID == "" {
		return "", errors.New("flight ID required")
	}
	return "FLIGHT-CONF-" + flightID, nil
}

func (s *TripService) bookHotel(ctx context.Context, hotelID string) (string, error) {
	if s.BookHotelFn != nil {
		return s.BookHotelFn(ctx, hotelID)
	}
	if hotelID == "" {
		return "", errors.New("hotel ID required")
	}
	return "HOTEL-CONF-" + hotelID, nil
}

func (s *TripService) bookCar(ctx context.Context, carID string) (string, error) {
	if s.BookCarFn != nil {
		return s.BookCarFn(ctx, carID)
	}
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
func RegisterHandlers(w *durable.Worker, svc *TripService) {
	durable.RegisterHandler(w, svc.BookFlight, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	})
	durable.RegisterHandler(w, svc.BookHotel, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	}).WithTerminalErrorHandler(svc.CompensateHotel, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 1},
	})
	durable.RegisterHandler(w, svc.BookCar, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	}).WithTerminalErrorHandler(svc.CompensateCar, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 1},
	})
}
