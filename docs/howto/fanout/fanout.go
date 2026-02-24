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

type ItemInput struct {
	ID       string
	Data     string
	ParentID string
}

func (ItemInput) DurableKind() string { return "process-item" }

// --- Messages ---

type ItemResult struct {
	ID     string
	Output string
}

func (ItemResult) DurableKind() string { return "results" }

// --- Parent routine ---

type FanoutInput struct {
	Items []struct {
		ID   string
		Data string
	}
}

func (FanoutInput) DurableKind() string { return "fanout" }

// --- Results ---

type FanoutResult struct {
	Results []ItemResult
}

func (FanoutResult) DurableKind() string { return "fanout-result" }

type CollectingInput struct {
	Pending int
	Results []ItemResult
}

func (CollectingInput) DurableKind() string { return "fanout.collecting" }

// --- Service struct ---

type FanoutService struct {
	// Injected dependencies would go here.
}

func (s *FanoutService) StartItems(ctx *durable.Context, input FanoutInput) (*durable.Continuation[FanoutResult], error) {
	parentID := ctx.RoutineID()

	var itemStub *ItemService
	for _, item := range input.Items {
		durable.BufferStart(ctx, fmt.Sprintf("item-%s", item.ID),
			itemStub.ProcessItem, ItemInput{ID: item.ID, Data: item.Data, ParentID: parentID})
	}

	collecting := CollectingInput{Pending: len(input.Items)}
	return durable.ReceiveSend(s.CollectResult, collecting), nil
}

func (s *FanoutService) CollectResult(ctx *durable.Context, input CollectingInput, externalInput ItemResult) (*durable.Continuation[FanoutResult], error) {
	input.Results = append(input.Results, externalInput)
	input.Pending--

	if input.Pending > 0 {
		return durable.ReceiveSend(s.CollectResult, input), nil
	}

	// All children done.
	fmt.Printf("collected %d results\n", len(input.Results))
	for _, r := range input.Results {
		fmt.Printf("  %s: %s\n", r.ID, r.Output)
	}
	return durable.Done(FanoutResult{Results: input.Results}), nil
}

// --- Item processor service ---

type ItemService struct {
	// Injected dependencies would go here.
}

func (s *ItemService) ProcessItem(ctx *durable.Context, input ItemInput) (*durable.Continuation[durable.Unit], error) {
	result := ItemResult{
		ID:     input.ID,
		Output: fmt.Sprintf("processed: %s", input.Data),
	}

	// Send result back to the parent — like ch <- result.
	var stub *FanoutService
	durable.BufferSend(ctx, input.ParentID, stub.CollectResult, result)
	return durable.Done(durable.Unit{}), nil
}

// RegisterHandlers registers all fanout handlers with the worker.
func RegisterHandlers(w *durable.Worker, fanoutSvc *FanoutService, itemSvc *ItemService) {
	durable.RegisterHandler(w, fanoutSvc.StartItems, durable.HandlerOptions{})
	durable.RegisterHandler(w, itemSvc.ProcessItem, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, fanoutSvc.CollectResult, durable.HandlerOptions{})
}
