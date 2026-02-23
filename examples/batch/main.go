// Command batch demonstrates chunked processing of a large dataset using
// Continue for checkpointing. This pattern keeps the stateroutine responsive to
// signals between chunks — like a GenServer that processes a batch and then
// checks its mailbox before continuing:
//
//	def handle_info(:process_batch, state) do
//	  state = process_chunk(state)
//	  if more_work?(state), do: send(self(), :process_batch)
//	  {:noreply, state}
//	end
//
// Each chunk runs as its own activity with independent retries. Between
// chunks, the workflow loop runs — checking for pending signals, handling
// continue-as-new if history is large, and spawning any requested children.
//
// The stateroutine also listens for a cancel signal. If a CancelInbox message
// arrives between chunks, the stateroutine stops early and reports partial
// progress. This is only possible because Continue yields control back
// to the workflow loop between chunks.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/stateroutine"
)

const chunkSize = 100

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
	end := state.Offset + chunkSize
	if end > len(state.Items) {
		end = len(state.Items)
	}

	// Process this chunk — normal Go code, no replay-safety constraints.
	for _, item := range state.Items[state.Offset:end] {
		if err := processItem(item); err != nil {
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

	// More items to process. Use Select with OnSend + Default so the workflow
	// loop checks for a cancel signal before processing the next chunk.
	//
	// - If no cancel signal is pending: Default fires immediately → next chunk.
	// - If a cancel signal arrived:     OnSend fires → CancelBatch runs.
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

// processItem simulates processing a single item.
func processItem(item string) error {
	fmt.Printf("  processing: %s\n", item)
	return nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &BatchService{}

	w := stateroutine.NewWorker("batch-queue")
	stateroutine.AddHandler(w, svc.StartBatch, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.ProcessChunk, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, svc.CancelBatch, stateroutine.HandlerOptions{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	// Generate a batch of items.
	items := make([]string, 350)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}

	h, err := stateroutine.Start(client, ctx, "batch-001", svc.StartBatch, BatchState{Items: items})
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the batch to complete (or be cancelled).
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("batch done: %d processed, %d errors, cancelled=%v\n",
		result.Processed, result.Errors, result.Cancelled)
}
