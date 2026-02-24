package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/docs/howto/batch"
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

	svc := &batch.BatchService{}

	w := durable.NewWorker("batch-queue")
	batch.RegisterHandlers(w, svc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "batch-queue"))

	// Generate a batch of items.
	items := make([]string, 350)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}

	h, err := durable.Go(client, ctx, "batch-001", svc.StartBatch, batch.BatchState{Items: items})
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
