package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/pipeline"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestPipelineFullSequence(t *testing.T) {
	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		pipeline.RegisterHandlers(w, producerSvc, consumerSvc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		consumerID := env.UniqueID("consumer")

		// Start the consumer first — it waits for items.
		_, err := durable.Go(env.Client, ctx, consumerID, consumerSvc.StartConsumer, pipeline.ConsumerInput{
			Name: "test-consumer",
		})
		if err != nil {
			t.Fatalf("Go consumer failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Start the producer — it sends items to the consumer.
		producerH, err := durable.Go(env.Client, ctx, env.UniqueID("producer"), producerSvc.Produce, pipeline.ProducerInput{
			Items:             []string{"one", "two", "three", "four"},
			ConsumerRoutineID: consumerID,
		})
		if err != nil {
			t.Fatalf("Go producer failed: %v", err)
		}

		// Wait for producer to finish.
		_, err = producerH.Get(ctx)
		if err != nil {
			t.Fatalf("Producer Get failed: %v", err)
		}

		// Get consumer result.
		consumerResult, err := durable.Get[pipeline.ConsumerResult](env.Client, ctx, consumerID)
		if err != nil {
			t.Fatalf("Consumer Get failed: %v", err)
		}

		if len(consumerResult.Received) != 4 {
			t.Fatalf("expected 4 items received, got %d", len(consumerResult.Received))
		}
		t.Logf("Pipeline result: %+v", consumerResult)
	})
}
