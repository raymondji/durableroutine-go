// Command fanout demonstrates fan-out/fan-in using child routines and
// routine-to-routine Cast, mirroring goroutines and channels:
//
//	ch := make(chan Result)
//	for _, item := range items {
//	    go func(it Item) { ch <- process(it) }(item)
//	}
//	for range items { results = append(results, <-ch) }
//
// Each child runs as its own durable routine (Temporal child workflow) with
// independent retries, timeouts, and event history. Children send results
// back to the parent via durable.Cast — like writing to a channel.
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Child routine ---

type ItemState struct {
	ID       string
	Data     string
	ParentID string
}

func (ItemState) Kind() string { return "process-item" }

// --- Messages ---

type ItemResult struct {
	ID     string
	Output string
}

func (ItemResult) Kind() string { return "results" }

// --- Parent routine ---

type FanoutState struct {
	Items []struct {
		ID   string
		Data string
	}
}

func (FanoutState) Kind() string { return "fanout" }

// --- Results ---

type FanoutResult struct {
	Results []ItemResult
}

type CollectingState struct {
	Pending int
	Results []ItemResult
}

func (CollectingState) Kind() string { return "fanout.collecting" }

// --- Service struct ---

type FanoutService struct {
	// Injected dependencies would go here.
}

func (s *FanoutService) SpawnItems(ctx *durable.Context, state FanoutState) (*durable.Suspend[FanoutResult], error) {
	parentID := ctx.RoutineID()

	for _, item := range state.Items {
		ctx.Spawn(fmt.Sprintf("item-%s", item.ID),
			ItemState{ID: item.ID, Data: item.Data, ParentID: parentID})
	}

	collecting := CollectingState{Pending: len(state.Items)}
	return durable.Select[FanoutResult](
		durable.OnCast(s.CollectResult, collecting),
	), nil
}

func (s *FanoutService) CollectResult(ctx *durable.Context, state CollectingState, result ItemResult) (*durable.Suspend[FanoutResult], error) {
	state.Results = append(state.Results, result)
	state.Pending--

	if state.Pending > 0 {
		return durable.Select[FanoutResult](
			durable.OnCast(s.CollectResult, state),
		), nil
	}

	// All children done.
	fmt.Printf("collected %d results\n", len(state.Results))
	for _, r := range state.Results {
		fmt.Printf("  %s: %s\n", r.ID, r.Output)
	}
	return durable.Done(FanoutResult{Results: state.Results}), nil
}

// --- Item processor service ---

type ItemService struct {
	// Injected dependencies would go here.
}

func (s *ItemService) ProcessItem(ctx *durable.Context, state ItemState) (*durable.Suspend[durable.Unit], error) {
	result := ItemResult{
		ID:     state.ID,
		Output: fmt.Sprintf("processed: %s", state.Data),
	}

	// Send result back to the parent — like ch <- result.
	if err := durable.Cast(ctx, state.ParentID, result); err != nil {
		return nil, fmt.Errorf("send result: %w", err)
	}
	return durable.Done(durable.Unit{}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	fanoutSvc := &FanoutService{}
	itemSvc := &ItemService{}

	w := durable.NewWorker("fanout-queue")
	durable.AddHandler(w, fanoutSvc.SpawnItems)
	durable.AddHandler(w, itemSvc.ProcessItem)
	durable.AddCastHandler(w, fanoutSvc.CollectResult)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	h, err := durable.Start(client, ctx, "batch-001", fanoutSvc.SpawnItems, FanoutState{
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

	// Wait for all children to complete and get collected results.
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("fanout complete: %d results\n", len(result.Results))
}
