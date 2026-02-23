// Command fanout demonstrates fan-out/fan-in using child stateroutines and
// stateroutine-to-stateroutine Send, mirroring goroutines and channels:
//
//	ch := make(chan Result)
//	for _, item := range items {
//	    go func(it Item) { ch <- process(it) }(item)
//	}
//	for range items { results = append(results, <-ch) }
//
// Each child runs as its own durable stateroutine (Temporal child workflow) with
// independent retries, timeouts, and event history. Children send results
// back to the parent via stateroutine.Send — like writing to a channel.
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- Child stateroutine ---

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

// --- Parent stateroutine ---

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

func (s *FanoutService) SpawnItems(ctx *stateroutine.Context, state FanoutState) (*stateroutine.Suspend[FanoutResult], error) {
	parentID := ctx.StateroutineID()

	for _, item := range state.Items {
		ctx.Spawn(fmt.Sprintf("item-%s", item.ID),
			ItemState{ID: item.ID, Data: item.Data, ParentID: parentID})
	}

	collecting := CollectingState{Pending: len(state.Items)}
	return stateroutine.Select[FanoutResult](
		stateroutine.OnSend(s.CollectResult, collecting),
	), nil
}

func (s *FanoutService) CollectResult(ctx *stateroutine.Context, state CollectingState, result ItemResult) (*stateroutine.Suspend[FanoutResult], error) {
	state.Results = append(state.Results, result)
	state.Pending--

	if state.Pending > 0 {
		return stateroutine.Select[FanoutResult](
			stateroutine.OnSend(s.CollectResult, state),
		), nil
	}

	// All children done.
	fmt.Printf("collected %d results\n", len(state.Results))
	for _, r := range state.Results {
		fmt.Printf("  %s: %s\n", r.ID, r.Output)
	}
	return stateroutine.Done(FanoutResult{Results: state.Results}), nil
}

// --- Item processor service ---

type ItemService struct {
	// Injected dependencies would go here.
}

func (s *ItemService) ProcessItem(ctx *stateroutine.Context, state ItemState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	result := ItemResult{
		ID:     state.ID,
		Output: fmt.Sprintf("processed: %s", state.Data),
	}

	// Send result back to the parent — like ch <- result.
	if err := stateroutine.Send(ctx, state.ParentID, result); err != nil {
		return nil, fmt.Errorf("send result: %w", err)
	}
	return stateroutine.Done(stateroutine.Unit{}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	fanoutSvc := &FanoutService{}
	itemSvc := &ItemService{}

	w := stateroutine.NewWorker("fanout-queue")
	stateroutine.AddHandler(w, fanoutSvc.SpawnItems, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, itemSvc.ProcessItem, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, fanoutSvc.CollectResult, stateroutine.HandlerOptions{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	h, err := stateroutine.Start(client, ctx, "batch-001", fanoutSvc.SpawnItems, FanoutState{
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
