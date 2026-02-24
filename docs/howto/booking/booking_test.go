package booking_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/booking"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestBookingHappyPath(t *testing.T) {
	svc := &booking.BookingService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		booking.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		id := env.UniqueID("booking-happy")
		h, err := durable.Go(env.Client, ctx, id, svc.ReserveItem, booking.BookingState{
			UserID: "user-1", ItemID: "item-1",
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		err = durable.Send(env.Client, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
			CardNumber: "4111111111111111", Expiry: "12/26",
		})
		if err != nil {
			t.Fatalf("ClientSend PaymentInfo failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		err = durable.Send(env.Client, ctx, id, svc.ProcessShipping, booking.ShippingInfo{
			Address: "123 Main St", City: "Springfield", Zip: "62701",
		})
		if err != nil {
			t.Fatalf("ClientSend ShippingInfo failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Status != "shipped" {
			t.Fatalf("expected status 'shipped', got %q", result.Status)
		}
		t.Logf("Booking happy path: %+v", result)
	})
}

func TestBookingCancel(t *testing.T) {
	svc := &booking.BookingService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		booking.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		id := env.UniqueID("booking-cancel")
		h, err := durable.Go(env.Client, ctx, id, svc.ReserveItem, booking.BookingState{
			UserID: "user-1", ItemID: "item-1",
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		resp, err := durable.Call(env.Client, ctx, id, svc.CancelBooking, booking.CancelReq{Reason: "changed mind"})
		if err != nil {
			t.Fatalf("ClientCall CancelBooking failed: %v", err)
		}

		if !resp.Confirmed {
			t.Fatalf("expected cancel confirmed")
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Status != "cancelled" {
			t.Fatalf("expected status 'cancelled', got %q", result.Status)
		}
		t.Logf("Booking cancel: %+v", result)
	})
}

func TestBookingExpireReservation(t *testing.T) {
	svc := &booking.BookingService{
		ReservationTimeout: 1 * time.Millisecond,
	}
	testenv.RunAll(t, func(w *durable.Worker) {
		booking.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h, err := durable.Go(env.Client, ctx, env.UniqueID("booking-expire"), svc.ReserveItem, booking.BookingState{
			UserID: "user-1", ItemID: "item-1",
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Status != "expired" {
			t.Fatalf("expected status 'expired', got %q", result.Status)
		}
		t.Logf("Booking expire: %+v", result)
	})
}

func TestBookingPaymentFailure(t *testing.T) {
	svc := &booking.BookingService{
		ChargeCardFn: func(cardNumber string) error {
			return errors.New("payment gateway unavailable")
		},
	}
	testenv.RunAll(t, func(w *durable.Worker) {
		booking.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		id := env.UniqueID("booking-pay-fail")
		h, err := durable.Go(env.Client, ctx, id, svc.ReserveItem, booking.BookingState{
			UserID: "user-1", ItemID: "item-1",
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		err = durable.Send(env.Client, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
			CardNumber: "4111111111111111", Expiry: "12/26",
		})
		if err != nil {
			t.Fatalf("ClientSend PaymentInfo failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Status != "payment_failed" {
			t.Fatalf("expected status 'payment_failed', got %q", result.Status)
		}
		t.Logf("Booking payment failure: %+v", result)
	})
}

func TestBookingQueryStatus(t *testing.T) {
	svc := &booking.BookingService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		booking.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		id := env.UniqueID("booking-query")
		_, err := durable.Go(env.Client, ctx, id, svc.ReserveItem, booking.BookingState{
			UserID: "user-1", ItemID: "item-1",
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		// Poll for "reserved" status
		var status booking.StatusResp
		for i := 0; i < 20; i++ {
			time.Sleep(500 * time.Millisecond)
			status, err = durable.Query(env.Client, ctx, id, booking.StatusResp{})
			if err == nil && status.Status == "reserved" {
				break
			}
		}
		if status.Status != "reserved" {
			t.Fatalf("expected query status 'reserved', got %q (err: %v)", status.Status, err)
		}
		t.Logf("Query status after reserve: %+v", status)

		// Send payment
		err = durable.Send(env.Client, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
			CardNumber: "4111111111111111", Expiry: "12/26",
		})
		if err != nil {
			t.Fatalf("ClientSend PaymentInfo failed: %v", err)
		}

		// Poll for "paid" status
		for i := 0; i < 20; i++ {
			time.Sleep(500 * time.Millisecond)
			status, err = durable.Query(env.Client, ctx, id, booking.StatusResp{})
			if err == nil && status.Status == "paid" {
				break
			}
		}
		if status.Status != "paid" {
			t.Fatalf("expected query status 'paid', got %q (err: %v)", status.Status, err)
		}
		t.Logf("Query status after payment: %+v", status)

		// Complete the workflow so it doesn't remain as an orphan.
		err = durable.Send(env.Client, ctx, id, svc.ProcessShipping, booking.ShippingInfo{
			Address: "123 Main St", City: "Springfield", Zip: "62701",
		})
		if err != nil {
			t.Fatalf("ClientSend ShippingInfo failed: %v", err)
		}
		result, err := durable.Get[booking.BookingResult](env.Client, ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Status != "shipped" {
			t.Fatalf("expected status 'shipped', got %q", result.Status)
		}
	})
}
