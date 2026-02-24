package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/pipeline"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
)

func TestPipelineFullSequence(t *testing.T) {
	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		pipeline.RegisterHandlers(w, producerSvc, consumerSvc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		consumerID := env.UniqueID("consumer")

		// Start the consumer first — it waits for items.
		_, err := stateroutine.Start(env.Client, ctx, consumerID, consumerSvc.StartConsumer, pipeline.ConsumerState{
			Name: "test-consumer",
		})
		if err != nil {
			t.Fatalf("Start consumer failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Start the producer — it sends items to the consumer.
		producerH, err := stateroutine.Start(env.Client, ctx, env.UniqueID("producer"), producerSvc.Produce, pipeline.ProducerState{
			Items:                  []string{"one", "two", "three", "four"},
			ConsumerStateroutineID: consumerID,
		})
		if err != nil {
			t.Fatalf("Start producer failed: %v", err)
		}

		// Wait for producer to finish.
		_, err = producerH.Get(ctx)
		if err != nil {
			t.Fatalf("Producer Get failed: %v", err)
		}

		// Get consumer result.
		consumerResult, err := stateroutine.ClientGet[pipeline.ConsumerResult](env.Client, ctx, consumerID)
		if err != nil {
			t.Fatalf("Consumer Get failed: %v", err)
		}

		if len(consumerResult.Received) != 4 {
			t.Fatalf("expected 4 items received, got %d", len(consumerResult.Received))
		}
		t.Logf("Pipeline result: %+v", consumerResult)
	})
}
