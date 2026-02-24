package fanout_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/fanout"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
)

func TestFanoutCollectAllResults(t *testing.T) {
	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}
	env := testenv.Setup(t, func(w *stateroutine.Worker) {
		fanout.RegisterHandlers(w, fanoutSvc, itemSvc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("fanout"), fanoutSvc.SpawnItems, fanout.FanoutState{
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
		t.Fatalf("Start failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(result.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.Results))
	}
	t.Logf("Fanout results: %+v", result)
}
