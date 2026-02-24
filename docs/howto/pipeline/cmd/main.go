package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/examples/pipeline"
	"github.com/raymondji/stateroutine/stateroutine"
)

func main() {
	ctx := context.Background()

	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}

	w := stateroutine.NewWorker("pipeline-queue")
	pipeline.RegisterHandlers(w, producerSvc, consumerSvc)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	// Start the consumer first so it's ready to receive.
	consumerH, err := stateroutine.Start(client, ctx, "consumer-1",
		consumerSvc.StartConsumer, pipeline.ConsumerState{Name: "my-consumer"})
	if err != nil {
		log.Fatal(err)
	}

	// Start the producer, pointing it at the consumer.
	if _, err := stateroutine.Start(client, ctx, "producer-1", producerSvc.Produce, pipeline.ProducerState{
		Items:                  []string{"alpha", "bravo", "charlie", "delta"},
		ConsumerStateroutineID: "consumer-1",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the consumer to finish processing all items.
	result, err := consumerH.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("consumer received %d items\n", len(result.Received))
}
