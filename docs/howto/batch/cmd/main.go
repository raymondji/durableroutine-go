package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/docs/howto/batch"
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

	svc := &batch.BatchService{}

	w := stateroutine.NewWorker("batch-queue")
	batch.RegisterHandlers(w, svc)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := stateroutine.NewClientFrom(temporalimpl.NewClient(tc, "batch-queue"))

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
