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
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/durableroutine/durable"
)

// --- Messages ---

type Item struct {
	Seq  int
	Data string
}

func (Item) Kind() string { return "items" }

type DoneMsg struct{}

func (DoneMsg) Kind() string { return "done" }

// --- Args ---

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

func (ConsumerState) Kind() string { return "consumer.processing" }

// --- Producer service ---

type ProducerService struct {
	// Injected dependencies would go here.
}

func (s *ProducerService) Handle(ctx *durable.Context, args ProducerArgs) (*durable.Suspend, error) {
	for i, data := range args.Items {
		item := Item{Seq: i, Data: data}
		if err := durable.Cast(ctx, args.ConsumerRoutineID, item); err != nil {
			return nil, fmt.Errorf("send item %d: %w", i, err)
		}
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	if err := durable.Cast(ctx, args.ConsumerRoutineID, DoneMsg{}); err != nil {
		return nil, fmt.Errorf("send done: %w", err)
	}
	fmt.Println("producer finished")
	return durable.Done(), nil
}

// --- Consumer service ---

type ConsumerService struct {
	// Injected dependencies would go here.
}

func (s *ConsumerService) Handle(ctx *durable.Context, args ConsumerArgs) (*durable.Suspend, error) {
	state := ConsumerState{Name: args.Name}
	return durable.Select(
		durable.OnCast(s.HandleItem, state),
		durable.OnCast(s.HandleDone, state),
	), nil
}

func (s *ConsumerService) HandleItem(ctx *durable.Context, state ConsumerState, item Item) (*durable.Suspend, error) {
	fmt.Printf("consumer %s received item %d: %s\n", state.Name, item.Seq, item.Data)
	state.Received = append(state.Received, item)

	return durable.Select(
		durable.OnCast(s.HandleItem, state),
		durable.OnCast(s.HandleDone, state),
	), nil
}

func (s *ConsumerService) HandleDone(ctx *durable.Context, state ConsumerState, _ DoneMsg) (*durable.Suspend, error) {
	fmt.Printf("consumer %s done, received %d items\n", state.Name, len(state.Received))
	return durable.Done(), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	producerSvc := &ProducerService{}
	consumerSvc := &ConsumerService{}

	w := durable.NewWorker("pipeline-queue")
	durable.AddRoutineHandler(w, producerSvc.Handle)
	durable.AddRoutineHandler(w, consumerSvc.Handle)
	durable.AddCastHandler(w, consumerSvc.HandleItem)
	durable.AddCastHandler(w, consumerSvc.HandleDone)

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
