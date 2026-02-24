package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/docs/howto/reminder"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/temporalimpl"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &reminder.ReminderService{}

	w := stateroutine.NewWorker("reminder-queue")
	reminder.RegisterHandlers(w, svc)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := stateroutine.NewClientFrom(temporalimpl.NewClient(tc, "reminder-queue"))
	if _, err := stateroutine.Start(client, ctx, "reminder-user-42",
		svc.SendInitial, reminder.InitialState{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
