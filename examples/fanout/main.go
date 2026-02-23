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

type ItemArgs struct {
	ID       string
	Data     string
	ParentID string
}

func (ItemArgs) Kind() string { return "process-item" }

// --- Messages ---

type ItemResult struct {
	ID     string
	Output string
}

func (ItemResult) Kind() string { return "results" }

// --- Parent routine ---

type BatchArgs struct {
	Items []struct {
		ID   string
		Data string
	}
}

func (BatchArgs) Kind() string { return "fanout" }

type FanoutState struct {
	Pending int
	Results []ItemResult
}

func (FanoutState) Kind() string { return "fanout.collecting" }

// --- Service struct ---

type FanoutService struct {
	// Injected dependencies would go here.
}

func (s *FanoutService) Handle(ctx *durable.Context, args BatchArgs) (*durable.Suspend, error) {
	parentID := ctx.RoutineID()

	for _, item := range args.Items {
		ctx.Spawn(fmt.Sprintf("item-%s", item.ID),
			ItemArgs{ID: item.ID, Data: item.Data, ParentID: parentID})
	}

	state := FanoutState{Pending: len(args.Items)}
	return durable.Select(
		durable.OnCast(s.CollectResult, state),
	), nil
}

func (s *FanoutService) CollectResult(ctx *durable.Context, state FanoutState, result ItemResult) (*durable.Suspend, error) {
	state.Results = append(state.Results, result)
	state.Pending--

	if state.Pending > 0 {
		return durable.Select(
			durable.OnCast(s.CollectResult, state),
		), nil
	}

	// All children done.
	fmt.Printf("collected %d results\n", len(state.Results))
	for _, r := range state.Results {
		fmt.Printf("  %s: %s\n", r.ID, r.Output)
	}
	return durable.Done(), nil
}

// --- Item processor service ---

type ItemService struct {
	// Injected dependencies would go here.
}

func (s *ItemService) Handle(ctx *durable.Context, args ItemArgs) (*durable.Suspend, error) {
	result := ItemResult{
		ID:     args.ID,
		Output: fmt.Sprintf("processed: %s", args.Data),
	}

	// Send result back to the parent — like ch <- result.
	if err := durable.Cast(ctx, args.ParentID, result); err != nil {
		return nil, fmt.Errorf("send result: %w", err)
	}
	return durable.Done(), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	fanoutSvc := &FanoutService{}
	itemSvc := &ItemService{}

	w := durable.NewWorker("fanout-queue")
	durable.AddRoutineHandler(w, fanoutSvc.Handle)
	durable.AddRoutineHandler(w, itemSvc.Handle)
	durable.AddCastHandler(w, fanoutSvc.CollectResult)

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	if err := durable.Start(client, ctx, "batch-001", BatchArgs{
		Items: []struct {
			ID   string
			Data string
		}{
			{ID: "1", Data: "foo"},
			{ID: "2", Data: "bar"},
			{ID: "3", Data: "baz"},
		},
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("fanout started, children processing items")
}
