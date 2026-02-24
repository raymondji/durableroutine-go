package batch_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/batch"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
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
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		batch.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("batch-all"), svc.StartBatch, batch.BatchState{
			Items: makeItems(350),
		})
		if err != nil {
			t.Fatalf("Start failed: %v", err)
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
	testenv.RunAll(t, func(w *stateroutine.Worker) {
		batch.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Use a large number of items so the batch takes several seconds,
		// giving us time to send a cancel signal.
		id := env.UniqueID("batch-cancel")
		h, err := stateroutine.Start(env.Client, ctx, id, svc.StartBatch, batch.BatchState{
			Items: makeItems(100000),
		})
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		// Send cancel after a short delay. The signal is buffered and
		// picked up when the stateroutine reaches a Select with OnSend.
		time.Sleep(2 * time.Second)

		err = stateroutine.ClientSend(env.Client, ctx, id, svc.CancelBatch, batch.CancelMsg{Reason: "test cancel"})
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
		if result.Processed >= 100000 {
			t.Fatal("expected cancel to stop processing before completion")
		}
		t.Logf("Batch cancel: processed=%d out of 100000", result.Processed)
	})
}
