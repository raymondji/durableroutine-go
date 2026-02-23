// Command saga demonstrates the SAGA compensation pattern. Because handlers
// run as activities, you write sequential calls with normal Go error handling
// and invoke compensations on failure — no special SAGA framework needed.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args & state ---

type StartArgs struct {
	TripID      string
	FlightID    string
	HotelID     string
	CarRentalID string
}

type State struct {
	TripID      string
	FlightID    string
	HotelID     string
	CarRentalID string
	Status      string

	// Track what has been booked so we know what to compensate.
	FlightConfirmation string
	HotelConfirmation  string
	CarConfirmation    string
}

// --- Process definition ---

var process = durable.Process[State]{
	Name: "trip-booking",
	InitState: func(args any) State {
		a := args.(StartArgs)
		return State{
			TripID:      a.TripID,
			FlightID:    a.FlightID,
			HotelID:     a.HotelID,
			CarRentalID: a.CarRentalID,
			Status:      "pending",
		}
	},
	Initial: bookTrip,
}

// bookTrip attempts to book flight, hotel, and car in sequence.
// If any step fails, it compensates the previously completed steps.
func bookTrip(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	// Step 1: Book flight
	flightConf, err := bookFlight(ctx, state.FlightID)
	if err != nil {
		state.Status = "failed"
		return nil, fmt.Errorf("book flight: %w", err)
	}
	state.FlightConfirmation = flightConf

	// Step 2: Book hotel — compensate flight on failure
	hotelConf, err := bookHotel(ctx, state.HotelID)
	if err != nil {
		cancelFlight(ctx, flightConf)
		state.Status = "failed"
		return nil, fmt.Errorf("book hotel: %w", err)
	}
	state.HotelConfirmation = hotelConf

	// Step 3: Book car — compensate hotel and flight on failure
	carConf, err := bookCar(ctx, state.CarRentalID)
	if err != nil {
		cancelHotel(ctx, hotelConf)
		cancelFlight(ctx, flightConf)
		state.Status = "failed"
		return nil, fmt.Errorf("book car: %w", err)
	}
	state.CarConfirmation = carConf

	state.Status = "confirmed"
	fmt.Printf("trip %s fully booked\n", state.TripID)
	return nil, nil
}

// --- Stub service calls (replace with real API clients) ---

func bookFlight(_ context.Context, flightID string) (string, error) {
	if flightID == "" {
		return "", errors.New("flight ID required")
	}
	return "FLIGHT-CONF-" + flightID, nil
}

func bookHotel(_ context.Context, hotelID string) (string, error) {
	if hotelID == "" {
		return "", errors.New("hotel ID required")
	}
	return "HOTEL-CONF-" + hotelID, nil
}

func bookCar(_ context.Context, carID string) (string, error) {
	if carID == "" {
		return "", errors.New("car rental ID required")
	}
	return "CAR-CONF-" + carID, nil
}

func cancelFlight(_ context.Context, confirmation string) {
	fmt.Printf("compensating: cancelling flight %s\n", confirmation)
}

func cancelHotel(_ context.Context, confirmation string) {
	fmt.Printf("compensating: cancelling hotel %s\n", confirmation)
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("saga-queue")
	w.Register(process)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	if err := client.Start(ctx, "trip-789", process, StartArgs{
		TripID:      "TRIP-789",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	}); err != nil {
		log.Fatal(err)
	}

	var result State
	if err := client.GetResult(ctx, "trip-789", &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("trip booking complete: status=%s flight=%s hotel=%s car=%s\n",
		result.Status, result.FlightConfirmation, result.HotelConfirmation, result.CarConfirmation)
}
