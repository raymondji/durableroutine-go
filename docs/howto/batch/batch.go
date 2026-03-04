// Package batch demonstrates chunked batch processing with cancellation.
// Processes a large dataset in chunks using Select + Default, checking for a
// cancel signal between chunks. Like a GenServer that checks its mailbox
// between batches.
package batch

import (
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
)

const ChunkSize = 100

// --- State ---

type BatchInput struct {
	Items []string
}

func (BatchInput) DurableKind() string { return "batch" }

// --- Messages ---

type CancelMsg struct {
	Reason string
}

func (CancelMsg) DurableKind() string { return "cancel" }

// --- Result ---

type BatchResult struct {
	Processed int
	Errors    int
	Cancelled bool
}

func (BatchResult) DurableKind() string { return "batch-result" }

// --- Per-step state types ---

type ProcessingInput struct {
	Items     []string
	Offset    int
	Processed int
	Errors    int
}

func (ProcessingInput) DurableKind() string { return "batch.processing" }

// --- Service struct ---

type BatchService struct {
	// Injected dependencies would go here (e.g., DB client, API client).
}

func (s *BatchService) StartBatch(ctx *durable.Context, input BatchInput) (*durable.Continuation[BatchResult], error) {
	fmt.Printf("starting batch of %d items\n", len(input.Items))
	processing := ProcessingInput{Items: input.Items}

	// Process the first chunk immediately, then use Continue for subsequent chunks.
	return s.processChunk(ctx, processing)
}

func (s *BatchService) ProcessChunk(ctx *durable.Context, input ProcessingInput) (*durable.Continuation[BatchResult], error) {
	return s.processChunk(ctx, input)
}

func (s *BatchService) processChunk(_ *durable.Context, input ProcessingInput) (*durable.Continuation[BatchResult], error) {
	end := input.Offset + ChunkSize
	if end > len(input.Items) {
		end = len(input.Items)
	}

	// Process this chunk — normal Go code, no replay-safety constraints.
	for _, item := range input.Items[input.Offset:end] {
		if err := ProcessItem(item); err != nil {
			input.Errors++
			fmt.Printf("  error processing %s: %v\n", item, err)
		} else {
			input.Processed++
		}
	}
	input.Offset = end

	fmt.Printf("  chunk done: %d/%d processed (%d errors)\n",
		input.Offset, len(input.Items), input.Errors)

	if input.Offset >= len(input.Items) {
		// All items processed.
		fmt.Printf("batch complete: %d processed, %d errors\n",
			input.Processed, input.Errors)
		return durable.Done(BatchResult{Processed: input.Processed, Errors: input.Errors}), nil
	}

	return durable.Select(
		durable.ReceiveSend(s.CancelBatch, input),
		durable.Default(s.ProcessChunk, input),
	), nil
}

func (s *BatchService) CancelBatch(ctx *durable.Context, input ProcessingInput, externalInput CancelMsg) (*durable.Continuation[BatchResult], error) {
	fmt.Printf("batch cancelled (reason: %s) after %d/%d items (%d errors)\n",
		externalInput.Reason, input.Offset, len(input.Items), input.Errors)
	return durable.Done(BatchResult{Processed: input.Processed, Errors: input.Errors, Cancelled: true}), nil
}

// ProcessItem simulates processing a single item.
func ProcessItem(item string) error {
	fmt.Printf("  processing: %s\n", item)
	return nil
}

// RegisterHandlers registers all batch handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *BatchService) {
	durable.RegisterHandler(w, svc.StartBatch, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.ProcessChunk, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.CancelBatch, durable.HandlerOptions{})
}
