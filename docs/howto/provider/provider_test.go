package provider_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/provider"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestReserveAndConfirm(t *testing.T) {
	svc := &provider.ProviderService{ReservationTimeout: 5 * time.Second}
	testenv.RunAll(t, func(w *durable.Worker) {
		provider.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		id := env.UniqueID("provider")
		_, err := durable.Go(env.Client, ctx, id, svc.Init, provider.ProviderInput{
			ProviderID: "dr-smith",
			Slots:      []string{"3pm", "4pm", "5pm"},
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		// Give the routine a moment to start and enter the select.
		time.Sleep(500 * time.Millisecond)

		// Reserve a slot.
		reserveResp, err := durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "alice",
			SlotID: "3pm",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot failed: %v", err)
		}
		t.Logf("reserve response: %s", reserveResp.Status)

		// Give the child routine a moment to start.
		time.Sleep(500 * time.Millisecond)

		// Confirm and pay.
		confirmResp, err := durable.Call(env.Client, ctx, id, svc.HandleConfirmAndPay, provider.ConfirmAndPay{
			UserID:    "alice",
			SlotID:    "3pm",
			PaymentID: "PAY-001",
		})
		if err != nil {
			t.Fatalf("Call ConfirmAndPay failed: %v", err)
		}
		t.Logf("confirm response: %s", confirmResp.Status)

		// Provider goes offline.
		offlineResp, err := durable.Call(env.Client, ctx, id, svc.HandleGoOffline, provider.GoOffline{})
		if err != nil {
			t.Fatalf("Call GoOffline failed: %v", err)
		}
		t.Logf("offline response: %d bookings", offlineResp.Booked)

		// Wait for the routine to complete.
		result, err := durable.Get[provider.ProviderResult](env.Client, ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Status != "offline" {
			t.Fatalf("expected offline, got %s", result.Status)
		}
	})
}

func TestReservationExpires(t *testing.T) {
	svc := &provider.ProviderService{ReservationTimeout: 1 * time.Second}
	testenv.RunAll(t, func(w *durable.Worker) {
		provider.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		id := env.UniqueID("provider")
		_, err := durable.Go(env.Client, ctx, id, svc.Init, provider.ProviderInput{
			ProviderID: "dr-jones",
			Slots:      []string{"10am", "11am"},
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Reserve a slot but don't confirm.
		reserveResp, err := durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "bob",
			SlotID: "10am",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot failed: %v", err)
		}
		t.Logf("reserve response: %s", reserveResp.Status)

		// Wait for the reservation to expire (1s timeout + buffer).
		time.Sleep(3 * time.Second)

		// Provider goes offline.
		offlineResp, err := durable.Call(env.Client, ctx, id, svc.HandleGoOffline, provider.GoOffline{})
		if err != nil {
			t.Fatalf("Call GoOffline failed: %v", err)
		}
		t.Logf("offline response: %d bookings", offlineResp.Booked)

		result, err := durable.Get[provider.ProviderResult](env.Client, ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Status != "offline" {
			t.Fatalf("expected offline, got %s", result.Status)
		}
	})
}

func TestConcurrentReservations(t *testing.T) {
	svc := &provider.ProviderService{ReservationTimeout: 5 * time.Second}
	testenv.RunAll(t, func(w *durable.Worker) {
		provider.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		id := env.UniqueID("provider")
		_, err := durable.Go(env.Client, ctx, id, svc.Init, provider.ProviderInput{
			ProviderID: "dr-lee",
			Slots:      []string{"9am", "10am", "11am"},
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Reserve two slots concurrently.
		reserveResp, err := durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "alice", SlotID: "9am",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot 9am failed: %v", err)
		}
		t.Logf("reserve 9am response: %s", reserveResp.Status)

		reserveResp, err = durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "bob", SlotID: "10am",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot 10am failed: %v", err)
		}
		t.Logf("reserve 10am response: %s", reserveResp.Status)

		time.Sleep(500 * time.Millisecond)

		// Alice confirms, Bob doesn't.
		confirmResp, err := durable.Call(env.Client, ctx, id, svc.HandleConfirmAndPay, provider.ConfirmAndPay{
			UserID: "alice", SlotID: "9am", PaymentID: "PAY-A",
		})
		if err != nil {
			t.Fatalf("Call ConfirmAndPay 9am failed: %v", err)
		}
		t.Logf("confirm 9am response: %s", confirmResp.Status)

		// Wait for Bob's reservation to expire.
		time.Sleep(6 * time.Second)

		// Provider goes offline.
		offlineResp, err := durable.Call(env.Client, ctx, id, svc.HandleGoOffline, provider.GoOffline{})
		if err != nil {
			t.Fatalf("Call GoOffline failed: %v", err)
		}
		t.Logf("offline response: %d bookings", offlineResp.Booked)

		result, err := durable.Get[provider.ProviderResult](env.Client, ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Status != "offline" {
			t.Fatalf("expected offline, got %s", result.Status)
		}
	})
}

func TestReserveConflict(t *testing.T) {
	svc := &provider.ProviderService{ReservationTimeout: 5 * time.Second}
	testenv.RunAll(t, func(w *durable.Worker) {
		provider.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		id := env.UniqueID("provider")
		_, err := durable.Go(env.Client, ctx, id, svc.Init, provider.ProviderInput{
			ProviderID: "dr-park",
			Slots:      []string{"2pm"},
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Alice reserves 2pm.
		reserveResp, err := durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "alice", SlotID: "2pm",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot failed: %v", err)
		}
		if reserveResp.Status != "reserved" {
			t.Fatalf("expected reserved, got %s", reserveResp.Status)
		}

		// Bob tries the same slot — should be rejected.
		reserveResp, err = durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "bob", SlotID: "2pm",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot failed: %v", err)
		}
		if reserveResp.Status != "rejected: already reserved" {
			t.Fatalf("expected rejected: already reserved, got %s", reserveResp.Status)
		}
		t.Logf("bob's response: %s", reserveResp.Status)

		// Alice confirms and pays.
		confirmResp, err := durable.Call(env.Client, ctx, id, svc.HandleConfirmAndPay, provider.ConfirmAndPay{
			UserID: "alice", SlotID: "2pm", PaymentID: "PAY-A",
		})
		if err != nil {
			t.Fatalf("Call ConfirmAndPay failed: %v", err)
		}
		if confirmResp.Status != "confirmed" {
			t.Fatalf("expected confirmed, got %s", confirmResp.Status)
		}

		// Charlie tries to reserve the now-booked slot — should be rejected.
		reserveResp, err = durable.Call(env.Client, ctx, id, svc.HandleReserve, provider.ReserveSlot{
			UserID: "charlie", SlotID: "2pm",
		})
		if err != nil {
			t.Fatalf("Call ReserveSlot failed: %v", err)
		}
		if reserveResp.Status != "rejected: already booked" {
			t.Fatalf("expected rejected: already booked, got %s", reserveResp.Status)
		}
		t.Logf("charlie's response: %s", reserveResp.Status)

		// Provider goes offline.
		offlineResp, err := durable.Call(env.Client, ctx, id, svc.HandleGoOffline, provider.GoOffline{})
		if err != nil {
			t.Fatalf("Call GoOffline failed: %v", err)
		}
		if offlineResp.Booked != 1 {
			t.Fatalf("expected 1 booking, got %d", offlineResp.Booked)
		}

		result, err := durable.Get[provider.ProviderResult](env.Client, ctx, id)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Status != "offline" {
			t.Fatalf("expected offline, got %s", result.Status)
		}
	})
}
