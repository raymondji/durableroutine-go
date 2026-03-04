package fanout_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/fanout"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestFanoutCollectAllResults(t *testing.T) {
	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}
	testenv.RunAll(t, func(w *durable.Worker) {
		fanout.RegisterHandlers(w, fanoutSvc, itemSvc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		h, err := durable.Go(env.Client, ctx, env.UniqueID("fanout"), fanoutSvc.StartItems, fanout.FanoutInput{
			Items: []struct {
				ID   string
				Data string
			}{
				{ID: "a", Data: "alpha"},
				{ID: "b", Data: "beta"},
				{ID: "c", Data: "gamma"},
			},
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result.Results))
		}
		t.Logf("Fanout results: %+v", result)
	})
}
