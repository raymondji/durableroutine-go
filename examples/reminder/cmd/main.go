package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/examples/reminder"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	svc := &reminder.ReminderService{}

	w := stateroutine.NewWorker("reminder-queue")
	reminder.RegisterHandlers(w, svc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()
	if _, err := stateroutine.Start(client, ctx, "reminder-user-42",
		svc.SendInitial, reminder.InitialState{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("reminder started, will send 3 emails over ~8 days")
}
