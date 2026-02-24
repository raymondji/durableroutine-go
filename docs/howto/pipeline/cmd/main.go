package main

import (
	"context"
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/docs/howto/pipeline"
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

	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}

	w := stateroutine.NewWorker("pipeline-queue")
	pipeline.RegisterHandlers(w, producerSvc, consumerSvc)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := stateroutine.NewClientFrom(temporalimpl.NewClient(tc, "pipeline-queue"))

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
