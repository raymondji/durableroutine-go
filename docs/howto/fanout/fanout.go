// Package fanout demonstrates fan-out/fan-in using child routines and
// routine-to-routine Send. Each child runs as its own durable
// routine with independent retries, timeouts, and event history. Children
// send results back to the parent via durable.BufferSend.
// Uses struct-based handlers for dependency injection.
package fanout

import (
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- Child routine ---

type ItemState struct {
	ID       string
	Data     string
	ParentID string
}

func (ItemState) DurableKind() string { return "process-item" }

// --- Messages ---

type ItemResult struct {
	ID     string
	Output string
}

func (ItemResult) DurableKind() string { return "results" }

// --- Parent routine ---

type FanoutState struct {
	Items []struct {
		ID   string
		Data string
	}
}

func (FanoutState) DurableKind() string { return "fanout" }

// --- Results ---

type FanoutResult struct {
	Results []ItemResult
}

func (FanoutResult) DurableKind() string { return "fanout-result" }

type CollectingState struct {
	Pending int
	Results []ItemResult
}

func (CollectingState) DurableKind() string { return "fanout.collecting" }

// --- Service struct ---

type FanoutService struct {
	// Injected dependencies would go here.
}

func (s *FanoutService) StartItems(ctx *durable.Context, state FanoutState) (*durable.Continuation[FanoutResult], error) {
	parentID := ctx.RoutineID()

	var itemStub *ItemService
	for _, item := range state.Items {
		durable.BufferStart(ctx, fmt.Sprintf("item-%s", item.ID),
			itemStub.ProcessItem, ItemState{ID: item.ID, Data: item.Data, ParentID: parentID})
	}

	collecting := CollectingState{Pending: len(state.Items)}
	return durable.ReceiveSend(s.CollectResult, collecting), nil
}

func (s *FanoutService) CollectResult(ctx *durable.Context, state CollectingState, result ItemResult) (*durable.Continuation[FanoutResult], error) {
	state.Results = append(state.Results, result)
	state.Pending--

	if state.Pending > 0 {
		return durable.ReceiveSend(s.CollectResult, state), nil
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

func (s *ItemService) ProcessItem(ctx *durable.Context, state ItemState) (*durable.Continuation[durable.Unit], error) {
	result := ItemResult{
		ID:     state.ID,
		Output: fmt.Sprintf("processed: %s", state.Data),
	}

	// Send result back to the parent — like ch <- result.
	var stub *FanoutService
	durable.BufferSend(ctx, state.ParentID, stub.CollectResult, result)
	return durable.Done(durable.Unit{}), nil
}

// RegisterHandlers registers all fanout handlers with the worker.
func RegisterHandlers(w *durable.Worker, fanoutSvc *FanoutService, itemSvc *ItemService) {
	durable.RegisterHandler(w, fanoutSvc.StartItems, durable.HandlerOptions{})
	durable.RegisterHandler(w, itemSvc.ProcessItem, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, fanoutSvc.CollectResult, durable.HandlerOptions{})
}
