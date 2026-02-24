package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/docs/howto/batch"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	svc := &batch.BatchService{}

	w := stateroutine.NewWorker("batch-queue")
	batch.RegisterHandlers(w, svc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	// Generate a batch of items.
	items := make([]string, 350)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}

	h, err := stateroutine.Start(client, ctx, "batch-001", svc.StartBatch, batch.BatchState{Items: items})
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the batch to complete (or be cancelled).
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("batch done: %d processed, %d errors, cancelled=%v\n",
		result.Processed, result.Errors, result.Cancelled)
}
