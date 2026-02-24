// Package batch demonstrates chunked batch processing with cancellation.
// Processes a large dataset in chunks using Select + Default, checking for a
// cancel signal between chunks. Like a GenServer that checks its mailbox
// between batches.
package batch

import (
	"fmt"

	"github.com/raymondji/stateroutine/stateroutine"
)

const ChunkSize = 100

// --- State ---

type BatchState struct {
	Items []string
}

func (BatchState) Kind() string { return "batch" }

// --- Messages ---

type CancelMsg struct {
	Reason string
}

func (CancelMsg) Kind() string { return "cancel" }

// --- Result ---

type BatchResult struct {
	Processed int
	Errors    int
	Cancelled bool
}

// --- Per-step state types ---

type ProcessingState struct {
	Items     []string
	Offset    int
	Processed int
	Errors    int
}

func (ProcessingState) Kind() string { return "batch.processing" }

// --- Service struct ---

type BatchService struct {
	// Injected dependencies would go here (e.g., DB client, API client).
}

func (s *BatchService) StartBatch(ctx *stateroutine.Context, state BatchState) (*stateroutine.Suspend[BatchResult], error) {
	fmt.Printf("starting batch of %d items\n", len(state.Items))
	processing := ProcessingState{Items: state.Items}

	// Process the first chunk immediately, then use Continue for subsequent chunks.
	return s.processChunk(ctx, processing)
}

func (s *BatchService) ProcessChunk(ctx *stateroutine.Context, state ProcessingState) (*stateroutine.Suspend[BatchResult], error) {
	return s.processChunk(ctx, state)
}

func (s *BatchService) processChunk(_ *stateroutine.Context, state ProcessingState) (*stateroutine.Suspend[BatchResult], error) {
	end := state.Offset + ChunkSize
	if end > len(state.Items) {
		end = len(state.Items)
	}

	// Process this chunk — normal Go code, no replay-safety constraints.
	for _, item := range state.Items[state.Offset:end] {
		if err := ProcessItem(item); err != nil {
			state.Errors++
			fmt.Printf("  error processing %s: %v\n", item, err)
		} else {
			state.Processed++
		}
	}
	state.Offset = end

	fmt.Printf("  chunk done: %d/%d processed (%d errors)\n",
		state.Offset, len(state.Items), state.Errors)

	if state.Offset >= len(state.Items) {
		// All items processed.
		fmt.Printf("batch complete: %d processed, %d errors\n",
			state.Processed, state.Errors)
		return stateroutine.Done(BatchResult{Processed: state.Processed, Errors: state.Errors}), nil
	}

	return stateroutine.Select[BatchResult](
		stateroutine.OnSend(s.CancelBatch, state),
		stateroutine.Default(s.ProcessChunk, state),
	), nil
}

func (s *BatchService) CancelBatch(ctx *stateroutine.Context, state ProcessingState, msg CancelMsg) (*stateroutine.Suspend[BatchResult], error) {
	fmt.Printf("batch cancelled (reason: %s) after %d/%d items (%d errors)\n",
		msg.Reason, state.Offset, len(state.Items), state.Errors)
	return stateroutine.Done(BatchResult{Processed: state.Processed, Errors: state.Errors, Cancelled: true}), nil
}

// ProcessItem simulates processing a single item.
func ProcessItem(item string) error {
	fmt.Printf("  processing: %s\n", item)
	return nil
}

// RegisterHandlers registers all batch handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *BatchService) {
	stateroutine.AddHandler(w, svc.StartBatch, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.ProcessChunk, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, svc.CancelBatch, stateroutine.HandlerOptions{})
}
