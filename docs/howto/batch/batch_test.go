package batch_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/batch"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func makeItems(n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	return items
}

func TestBatchCompleteAll(t *testing.T) {
	svc := &batch.BatchService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		batch.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		h, err := durable.Go(env.Client, ctx, env.UniqueID("batch-all"), svc.StartBatch, batch.BatchInput{
			Items: makeItems(350),
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Processed != 350 {
			t.Fatalf("expected 350 processed, got %d", result.Processed)
		}
		if result.Cancelled {
			t.Fatal("expected not cancelled")
		}
		t.Logf("Batch complete: %+v", result)
	})
}

func TestBatchCancelMidBatch(t *testing.T) {
	svc := &batch.BatchService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		batch.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Use enough items so the batch takes a while (especially with
		// Temporal activity overhead per chunk), giving us time to send a
		// cancel signal. Keep it under Temporal's 2 MB payload limit —
		// ProcessingInput carries the full items slice through each
		// continuation. 50k items ≈ 1.3 MB serialized (2 cases).
		const numItems = 50000
		id := env.UniqueID("batch-cancel")
		h, err := durable.Go(env.Client, ctx, id, svc.StartBatch, batch.BatchInput{
			Items: makeItems(numItems),
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		// Send cancel after a short delay. The signal is buffered and
		// picked up when the routine reaches a Select with ReceiveSend.
		// 500 ms is enough for some chunks to process but not all 50k items.
		time.Sleep(500 * time.Millisecond)

		err = durable.Send(env.Client, ctx, id, svc.CancelBatch, batch.CancelMsg{Reason: "test cancel"})
		if err != nil {
			t.Fatalf("ClientSend CancelMsg failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !result.Cancelled {
			t.Fatal("expected cancelled=true")
		}
		// Some chunks were processed before the cancel signal was picked up.
		if result.Processed < 100 {
			t.Fatalf("expected at least 100 items processed, got %d", result.Processed)
		}
		if result.Processed >= numItems {
			t.Fatal("expected cancel to stop processing before completion")
		}
		t.Logf("Batch cancel: processed=%d out of %d", result.Processed, numItems)
	})
}
