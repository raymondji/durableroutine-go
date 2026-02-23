// Command fanout demonstrates fan-out/fan-in using child processes and
// explicit message passing, mirroring how you'd use goroutines and channels:
//
//	ch := make(chan Result)
//	for _, item := range items {
//	    go func(it Item) { ch <- process(it) }(item)   // spawn worker, pass channel
//	}
//	for range items { results = append(results, <-ch) } // collect from channel
//
// Each child runs as its own durable process (Temporal child workflow) with
// independent retries, timeouts, and event history. Children send results
// back to the parent via durable.SendMessage — just like writing to a channel.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Child process: processes a single item ---

// ItemArgs includes the parent's process ID and channel name — like passing
// a channel to a goroutine so it can send results back.
type ItemArgs struct {
	ID              string
	Data            string
	ReplyProcessID  string
	ReplyChannel    string
}

type ItemResult struct {
	ID     string
	Output string
}

type ItemState struct {
	ID             string
	Data           string
	ReplyProcessID string
	ReplyChannel   string
}

var itemProcess = durable.Process[ItemState]{
	Name: "process-item",
	InitState: func(args any) ItemState {
		a := args.(ItemArgs)
		return ItemState{
			ID:             a.ID,
			Data:           a.Data,
			ReplyProcessID: a.ReplyProcessID,
			ReplyChannel:   a.ReplyChannel,
		}
	},
	Initial: processItem,
}

func processItem(ctx context.Context, state *ItemState) (*durable.Suspend[ItemState], error) {
	// Normal Go code — call APIs, query databases, etc.
	result := ItemResult{
		ID:     state.ID,
		Output: fmt.Sprintf("processed: %s", state.Data),
	}

	// Send the result back to the parent — like ch <- result.
	if err := durable.SendMessage(ctx, state.ReplyProcessID, state.ReplyChannel, result); err != nil {
		return nil, fmt.Errorf("send result: %w", err)
	}
	return nil, nil
}

// --- Parent process: spawns children, collects results via channel ---

type BatchArgs struct {
	Items []ItemArgs
}

type BatchState struct {
	Items   []ItemArgs
	Pending int
	Results []ItemResult
}

var batchProcess = durable.Process[BatchState]{
	Name: "fanout",
	InitState: func(args any) BatchState {
		a := args.(BatchArgs)
		return BatchState{Items: a.Items}
	},
	Initial: fanOut,
}

// fanOut spawns a child process per item and waits for results.
func fanOut(ctx context.Context, state *BatchState) (*durable.Suspend[BatchState], error) {
	parentID := durable.ProcessID(ctx)

	spawns := make([]durable.Spawn, len(state.Items))
	for i, item := range state.Items {
		spawns[i] = durable.Spawn{
			ProcessID: fmt.Sprintf("item-%s", item.ID),
			Process:   itemProcess,
			Args: ItemArgs{
				ID:             item.ID,
				Data:           item.Data,
				ReplyProcessID: parentID,  // tell the child where to send results
				ReplyChannel:   "results", // tell the child which channel
			},
		}
	}
	state.Pending = len(state.Items)

	// Start all children, then wait for results to arrive on the "results" channel.
	return durable.Select[BatchState](
		durable.Receive[BatchState, ItemResult]("results", collectResult),
	).WithSpawns(spawns...), nil
}

// collectResult is called once per completed child — just like reading from a channel.
func collectResult(ctx context.Context, state *BatchState, result ItemResult) (*durable.Suspend[BatchState], error) {
	state.Results = append(state.Results, result)
	state.Pending--

	if state.Pending > 0 {
		// Still waiting for more children — keep receiving.
		return durable.Select[BatchState](
			durable.Receive[BatchState, ItemResult]("results", collectResult),
		), nil
	}

	// All children done.
	fmt.Printf("collected %d results\n", len(state.Results))
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("fanout-queue")
	w.Register(batchProcess)
	w.Register(itemProcess)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	items := []ItemArgs{
		{ID: "1", Data: "foo"},
		{ID: "2", Data: "bar"},
		{ID: "3", Data: "baz"},
	}
	if err := client.Start(ctx, "batch-001", batchProcess, BatchArgs{Items: items}); err != nil {
		log.Fatal(err)
	}

	var result BatchState
	if err := client.GetResult(ctx, "batch-001", &result); err != nil {
		log.Fatal(err)
	}
	for _, r := range result.Results {
		fmt.Printf("  %s: %s\n", r.ID, r.Output)
	}
	fmt.Printf("fanout complete: %d results\n", len(result.Results))
}
