package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/docs/howto/reminder"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/backend/temporal"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &reminder.ReminderService{}

	w := durable.NewWorker("reminder-queue")
	reminder.RegisterHandlers(w, svc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "reminder-queue"))
	if _, err := durable.Go(client, ctx, "reminder-user-42",
		svc.SendInitial, reminder.InitialState{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
