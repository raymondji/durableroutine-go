package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/examples/fanout"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}

	w := stateroutine.NewWorker("fanout-queue")
	fanout.RegisterHandlers(w, fanoutSvc, itemSvc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

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
