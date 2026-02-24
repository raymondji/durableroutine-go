package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine-go/docs/howto/batch"
	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	svc := &batch.BatchService{}

	w := durable.NewWorker("batch-queue")
	batch.RegisterHandlers(w, svc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		// Generate a batch of items.
		items := make([]string, 350)
		for i := range items {
			items[i] = fmt.Sprintf("item-%d", i)
		}

		h, err := durable.Go(client, ctx, "batch-001", svc.StartBatch, batch.BatchInput{Items: items})
		if err != nil {
			log.Fatal(err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("batch done: %d processed, %d errors, cancelled=%v\n",
			result.Processed, result.Errors, result.Cancelled)
	})
}
