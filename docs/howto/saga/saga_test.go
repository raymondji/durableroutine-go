package saga_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/saga"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
)

func TestSagaHappyPath(t *testing.T) {
	svc := &saga.TripService{}
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		saga.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("saga-happy"), svc.BookFlight, saga.TripState{
			TripID:      "TRIP-1",
			FlightID:    "FL-100",
			HotelID:     "HT-200",
			CarRentalID: "CR-300",
		})
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.FlightConfirmation == "" {
			t.Fatal("expected flight confirmation")
		}
		if result.HotelConfirmation == "" {
			t.Fatal("expected hotel confirmation")
		}
		if result.CarConfirmation == "" {
			t.Fatal("expected car confirmation")
		}
		t.Logf("Saga happy path: %+v", result)
	})
}

func TestSagaHotelFailure(t *testing.T) {
	svc := &saga.TripService{
		BookHotelFn: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("hotel fully booked")
		},
	}
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		saga.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("saga-hotel-fail"), svc.BookFlight, saga.TripState{
			TripID:      "TRIP-2",
			FlightID:    "FL-100",
			HotelID:     "HT-200",
			CarRentalID: "CR-300",
		})
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		_, err = h.Get(ctx)
		if err == nil {
			t.Fatal("expected error from saga compensation")
		}

		if !strings.Contains(err.Error(), "compensated flight") {
			t.Fatalf("expected compensation error, got: %v", err)
		}
		t.Logf("Saga hotel failure with compensation: %v", err)
	})
}

func TestSagaCarFailure(t *testing.T) {
	svc := &saga.TripService{
		BookCarFn: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("no cars available")
		},
	}
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		saga.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("saga-car-fail"), svc.BookFlight, saga.TripState{
			TripID:      "TRIP-3",
			FlightID:    "FL-100",
			HotelID:     "HT-200",
			CarRentalID: "CR-300",
		})
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		_, err = h.Get(ctx)
		if err == nil {
			t.Fatal("expected error from saga compensation")
		}

		if !strings.Contains(err.Error(), "compensated hotel and flight") {
			t.Fatalf("expected compensation error, got: %v", err)
		}
		t.Logf("Saga car failure with compensation: %v", err)
	})
}
