package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/provider"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &provider.ProviderService{ReservationTimeout: 3 * time.Second}

	w := durable.NewWorker("provider-queue")
	provider.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		// Start the provider routine.
		_, err := durable.Go(client, ctx, "provider-dr-smith", svc.Init, provider.ProviderInput{
			ProviderID: "dr-smith",
			Slots:      []string{"3pm", "4pm", "5pm"},
		})
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)

		// Alice reserves 3pm.
		fmt.Println("\n--- Alice reserves 3pm ---")
		reserveResp, err := durable.Call(client, ctx, "provider-dr-smith", svc.HandleReserve, provider.ReserveSlot{
			UserID: "alice", SlotID: "3pm",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("response: %s\n", reserveResp.Status)

		// Bob reserves 4pm.
		fmt.Println("\n--- Bob reserves 4pm ---")
		reserveResp, err = durable.Call(client, ctx, "provider-dr-smith", svc.HandleReserve, provider.ReserveSlot{
			UserID: "bob", SlotID: "4pm",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("response: %s\n", reserveResp.Status)
		time.Sleep(300 * time.Millisecond)

		// Alice confirms and pays.
		fmt.Println("\n--- Alice confirms and pays ---")
		confirmResp, err := durable.Call(client, ctx, "provider-dr-smith", svc.HandleConfirmAndPay, provider.ConfirmAndPay{
			UserID: "alice", SlotID: "3pm", PaymentID: "PAY-ALICE",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("response: %s\n", confirmResp.Status)

		// Charlie tries to reserve 3pm, which Alice already has — conflict.
		fmt.Println("\n--- Charlie tries to reserve 3pm (already booked) ---")
		reserveResp, err = durable.Call(client, ctx, "provider-dr-smith", svc.HandleReserve, provider.ReserveSlot{
			UserID: "charlie", SlotID: "3pm",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("response: %s\n", reserveResp.Status)

		// Bob doesn't pay — wait for his reservation to expire.
		fmt.Println("\n--- Waiting for Bob's reservation to expire ---")
		time.Sleep(5 * time.Second)

		// Provider goes offline.
		fmt.Println("\n--- Provider goes offline ---")
		offlineResp, err := durable.Call(client, ctx, "provider-dr-smith", svc.HandleGoOffline, provider.GoOffline{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("response: %d bookings\n", offlineResp.Booked)

		fmt.Println("\n--- Demo complete ---")
	})
}
