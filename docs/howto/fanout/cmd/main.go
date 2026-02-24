package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/docs/howto/fanout"
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

	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}

	w := durable.NewWorker("fanout-queue")
	fanout.RegisterHandlers(w, fanoutSvc, itemSvc)

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := durable.NewClientFrom(temporal.NewClient(tc, "fanout-queue"))

	h, err := durable.Go(client, ctx, "batch-001", fanoutSvc.StartItems, fanout.FanoutState{
		Items: []struct {
			ID   string
			Data string
		}{
			{ID: "1", Data: "foo"},
			{ID: "2", Data: "bar"},
			{ID: "3", Data: "baz"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Wait for all children to complete and get collected results.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("fanout complete: %d results\n", len(result.Results))
}
