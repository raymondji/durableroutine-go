// Command pipeline demonstrates a producer-consumer pattern between two
// durable routines. The producer generates items one at a time and sends
// each to the consumer via durable.Cast — exactly like a goroutine
// writing to a channel:
//
//	ch := make(chan Item)
//	go producer(ch)           // sends many items
//	for item := range ch {    // consumer reads one at a time
//	    process(item)
//	}
//
// The producer and consumer are fully independent durable routines. Either
// can crash and resume without losing messages (Temporal signals are durable).
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Descriptors ---

var ItemsInbox = durable.Inbox[Item]{Name: "items"}
var DoneInbox = durable.Inbox[DoneMsg]{Name: "done"}

// --- Types ---

type Item struct {
	Seq  int
	Data string
}

type DoneMsg struct{}

type ProducerArgs struct {
	Items             []string
	ConsumerRoutineID string
}

func (ProducerArgs) Kind() string { return "producer" }

type ConsumerArgs struct {
	Name string
}

func (ConsumerArgs) Kind() string { return "consumer" }

type ConsumerState struct {
	Name     string
	Received []Item
}

// --- Producer handler ---

func produce(ctx *durable.Context, args ProducerArgs) (*durable.Suspend, error) {
	for i, data := range args.Items {
		item := Item{Seq: i, Data: data}
		if err := durable.Cast(ctx, args.ConsumerRoutineID, ItemsInbox, item); err != nil {
			return nil, fmt.Errorf("send item %d: %w", i, err)
		}
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	if err := durable.Cast(ctx, args.ConsumerRoutineID, DoneInbox, DoneMsg{}); err != nil {
		return nil, fmt.Errorf("send done: %w", err)
	}
	fmt.Println("producer finished")
	return nil, nil
}

// --- Consumer handlers ---

func waitForItems(ctx *durable.Context, args ConsumerArgs) (*durable.Suspend, error) {
	state := ConsumerState{Name: args.Name}
	return durable.Select(
		durable.OnCast(ItemsInbox, handleItem, state),
		durable.OnCast(DoneInbox, handleDone, state),
	), nil
}

func handleItem(ctx *durable.Context, state ConsumerState, item Item) (*durable.Suspend, error) {
	fmt.Printf("consumer %s received item %d: %s\n", state.Name, item.Seq, item.Data)
	state.Received = append(state.Received, item)

	return durable.Select(
		durable.OnCast(ItemsInbox, handleItem, state),
		durable.OnCast(DoneInbox, handleDone, state),
	), nil
}

func handleDone(ctx *durable.Context, state ConsumerState, _ DoneMsg) (*durable.Suspend, error) {
	fmt.Printf("consumer %s done, received %d items\n", state.Name, len(state.Received))
	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	workers := durable.NewWorkers()
	durable.AddRoutine(workers, produce)
	durable.AddRoutine(workers, waitForItems)

	w := durable.NewWorker("pipeline-queue", workers)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()

	// Start the consumer first so it's ready to receive.
	if err := durable.Start(client, ctx, "consumer-1",
		ConsumerArgs{Name: "my-consumer"}); err != nil {
		log.Fatal(err)
	}

	// Start the producer, pointing it at the consumer.
	if err := durable.Start(client, ctx, "producer-1", ProducerArgs{
		Items:             []string{"alpha", "bravo", "charlie", "delta"},
		ConsumerRoutineID: "consumer-1",
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("pipeline started, producer sending items to consumer")
}
