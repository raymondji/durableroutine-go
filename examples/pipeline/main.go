// Command pipeline demonstrates a producer-consumer pattern between two
// durable processes. The producer generates items one at a time and sends
// each to the consumer via durable.SendMessage — exactly like a goroutine
// writing to a channel:
//
//	ch := make(chan Item)
//	go producer(ch)           // sends many items
//	for item := range ch {    // consumer reads one at a time
//	    process(item)
//	}
//
// The producer and consumer are fully independent durable processes. Either
// can crash and resume without losing messages (Temporal signals are durable).
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Shared message types ---

type Item struct {
	Seq  int
	Data string
}

// DoneMsg is a sentinel sent on the "done" channel to signal no more items.
type DoneMsg struct{}

// --- Producer process ---

type ProducerArgs struct {
	Items            []string
	ConsumerProcessID string
}

type ProducerState struct {
	Items            []string
	ConsumerProcessID string
}

var producerProcess = durable.Process[ProducerState]{
	Name: "producer",
	InitState: func(args any) ProducerState {
		a := args.(ProducerArgs)
		return ProducerState{
			Items:            a.Items,
			ConsumerProcessID: a.ConsumerProcessID,
		}
	},
	Initial: produce,
}

// produce sends each item to the consumer's "items" channel, then sends a
// DoneMsg on the "done" channel. Like a goroutine doing:
//
//	for i, s := range items { ch <- Item{i, s} }
//	close(ch)  // we use a "done" signal instead of close
func produce(ctx context.Context, state *ProducerState) (*durable.Suspend[ProducerState], error) {
	for i, data := range state.Items {
		item := Item{Seq: i, Data: data}
		if err := durable.SendMessage(ctx, state.ConsumerProcessID, "items", item); err != nil {
			return nil, fmt.Errorf("send item %d: %w", i, err)
		}
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	// Signal that production is complete.
	if err := durable.SendMessage(ctx, state.ConsumerProcessID, "done", DoneMsg{}); err != nil {
		return nil, fmt.Errorf("send done: %w", err)
	}
	fmt.Println("producer finished")
	return nil, nil
}

// --- Consumer process ---

type ConsumerArgs struct {
	Name string
}

type ConsumerState struct {
	Name      string
	Received  []Item
	Completed bool
}

var consumerProcess = durable.Process[ConsumerState]{
	Name: "consumer",
	InitState: func(args any) ConsumerState {
		a := args.(ConsumerArgs)
		return ConsumerState{Name: a.Name}
	},
	Initial: waitForItems,
}

// waitForItems suspends until either an item or a done signal arrives.
// This is like `for item := range ch` — we keep receiving until the
// producer signals completion.
func waitForItems(ctx context.Context, state *ConsumerState) (*durable.Suspend[ConsumerState], error) {
	return durable.Select(
		durable.Receive[ConsumerState, Item]("items", handleItem),
		durable.Receive[ConsumerState, DoneMsg]("done", handleDone),
	), nil
}

func handleItem(ctx context.Context, state *ConsumerState, item Item) (*durable.Suspend[ConsumerState], error) {
	fmt.Printf("consumer %s received item %d: %s\n", state.Name, item.Seq, item.Data)
	state.Received = append(state.Received, item)

	// Keep waiting for more items.
	return durable.Select(
		durable.Receive[ConsumerState, Item]("items", handleItem),
		durable.Receive[ConsumerState, DoneMsg]("done", handleDone),
	), nil
}

func handleDone(ctx context.Context, state *ConsumerState, _ DoneMsg) (*durable.Suspend[ConsumerState], error) {
	state.Completed = true
	fmt.Printf("consumer %s done, received %d items\n", state.Name, len(state.Received))
	return nil, nil // process complete
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("pipeline-queue")
	w.Register(producerProcess)
	w.Register(consumerProcess)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	// Start the consumer first so it's ready to receive.
	if err := client.Start(ctx, "consumer-1", consumerProcess, ConsumerArgs{
		Name: "my-consumer",
	}); err != nil {
		log.Fatal(err)
	}

	// Start the producer, pointing it at the consumer.
	if err := client.Start(ctx, "producer-1", producerProcess, ProducerArgs{
		Items:            []string{"alpha", "bravo", "charlie", "delta"},
		ConsumerProcessID: "consumer-1",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the consumer to finish processing all items.
	var result ConsumerState
	if err := client.GetResult(ctx, "consumer-1", &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("pipeline complete: consumer received %d items\n", len(result.Received))
}
