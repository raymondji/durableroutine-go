package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine-go/docs/howto/demorunner"
	"github.com/raymondji/durableroutine-go/docs/howto/pipeline"
	"github.com/raymondji/durableroutine-go/durable"
)

func main() {
	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}

	w := durable.NewWorker("pipeline-queue")
	pipeline.RegisterHandlers(w, producerSvc, consumerSvc)

	demorunner.Run(w, func(client durable.Client) {
		ctx := context.Background()

		// Start the consumer first so it's ready to receive.
		consumerH, err := durable.Go(client, ctx, "consumer-1",
			consumerSvc.StartConsumer, pipeline.ConsumerInput{Name: "my-consumer"})
		if err != nil {
			log.Fatal(err)
		}

		// Start the producer, pointing it at the consumer.
		if _, err := durable.Go(client, ctx, "producer-1", producerSvc.Produce, pipeline.ProducerInput{
			Items:             []string{"alpha", "bravo", "charlie", "delta"},
			ConsumerRoutineID: "consumer-1",
		}); err != nil {
			log.Fatal(err)
		}

		// Wait for the consumer to finish processing all items.
		result, err := consumerH.Get(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer received %d items\n", len(result.Received))
	})
}
