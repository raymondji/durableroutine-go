package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/docs/howto/fanout"
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

	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}

	w := stateroutine.NewWorker("fanout-queue")
	fanout.RegisterHandlers(w, fanoutSvc, itemSvc)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := stateroutine.NewClientFrom(temporalimpl.NewClient(tc, "fanout-queue"))

	h, err := stateroutine.Start(client, ctx, "batch-001", fanoutSvc.SpawnItems, fanout.FanoutState{
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
