package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/reminder"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &reminder.ReminderService{
		InitialDelay:  1 * time.Millisecond,
		FollowUpDelay: 1 * time.Millisecond,
	}

	w := durable.NewWorker("reminder-queue")
	reminder.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		h, err := durable.Go(client, ctx, "reminder-user-42",
			svc.SendInitial, reminder.InitialState{Email: "user@example.com"})
		if err != nil {
			log.Fatal(err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}

		_ = result
		fmt.Println("reminder completed")
	})
}
