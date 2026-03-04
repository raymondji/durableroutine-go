package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/fanout"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}

	w := durable.NewWorker("fanout-queue")
	fanout.RegisterHandlers(w, fanoutSvc, itemSvc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		h, err := durable.Go(client, ctx, "batch-001", fanoutSvc.StartItems, fanout.FanoutInput{
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

		result, err := h.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("fanout complete: %d results\n", len(result.Results))
	})
}
