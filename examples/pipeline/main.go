// Command pipeline demonstrates a producer-consumer pattern between two
// durable stateroutines. The producer generates items one at a time and sends
// each to the consumer via stateroutine.Send — exactly like a goroutine
// writing to a channel:
//
//	ch := make(chan Item)
//	go producer(ch)           // sends many items
//	for item := range ch {    // consumer reads one at a time
//	    process(item)
//	}
//
// The producer and consumer are fully independent durable stateroutines. Either
// can crash and resume without losing messages (Temporal signals are durable).
// Uses struct-based handlers for dependency injection.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- Messages ---

type Item struct {
	Seq  int
	Data string
}

func (Item) Kind() string { return "items" }

type DoneMsg struct{}

func (DoneMsg) Kind() string { return "done" }

// --- State ---

type ProducerState struct {
	Items             []string
	ConsumerStateroutineID string
}

func (ProducerState) Kind() string { return "producer" }

type ConsumerState struct {
	Name     string
	Received []Item
}

func (ConsumerState) Kind() string { return "consumer" }

// --- Results ---

type ConsumerResult struct {
	Received []Item
}

// --- Producer service ---

type ProducerService struct {
	// Injected dependencies would go here.
}

func (s *ProducerService) Produce(ctx *stateroutine.Context, state ProducerState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	for i, data := range state.Items {
		item := Item{Seq: i, Data: data}
		if err := stateroutine.Send(ctx, state.ConsumerStateroutineID, item); err != nil {
			return nil, fmt.Errorf("send item %d: %w", i, err)
		}
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	if err := stateroutine.Send(ctx, state.ConsumerStateroutineID, DoneMsg{}); err != nil {
		return nil, fmt.Errorf("send done: %w", err)
	}
	fmt.Println("producer finished")
	return stateroutine.Done(stateroutine.Unit{}), nil
}

// --- Consumer service ---

type ConsumerService struct {
	// Injected dependencies would go here.
}

func (s *ConsumerService) StartConsumer(ctx *stateroutine.Context, state ConsumerState) (*stateroutine.Suspend[ConsumerResult], error) {
	return stateroutine.Select[ConsumerResult](
		stateroutine.OnSend(s.ReceiveItem, state),
		stateroutine.OnSend(s.ReceiveDone, state),
	), nil
}

func (s *ConsumerService) ReceiveItem(ctx *stateroutine.Context, state ConsumerState, item Item) (*stateroutine.Suspend[ConsumerResult], error) {
	fmt.Printf("consumer %s received item %d: %s\n", state.Name, item.Seq, item.Data)
	state.Received = append(state.Received, item)

	return stateroutine.Select[ConsumerResult](
		stateroutine.OnSend(s.ReceiveItem, state),
		stateroutine.OnSend(s.ReceiveDone, state),
	), nil
}

func (s *ConsumerService) ReceiveDone(ctx *stateroutine.Context, state ConsumerState, _ DoneMsg) (*stateroutine.Suspend[ConsumerResult], error) {
	fmt.Printf("consumer %s done, received %d items\n", state.Name, len(state.Received))
	return stateroutine.Done(ConsumerResult{Received: state.Received}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	producerSvc := &ProducerService{}
	consumerSvc := &ConsumerService{}

	w := stateroutine.NewWorker("pipeline-queue")
	stateroutine.AddHandler(w, producerSvc.Produce, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, consumerSvc.StartConsumer, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, consumerSvc.ReceiveItem, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, consumerSvc.ReceiveDone, stateroutine.HandlerOptions{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	// Start the consumer first so it's ready to receive.
	consumerH, err := stateroutine.Start(client, ctx, "consumer-1",
		consumerSvc.StartConsumer, ConsumerState{Name: "my-consumer"})
	if err != nil {
		log.Fatal(err)
	}

	// Start the producer, pointing it at the consumer.
	if _, err := stateroutine.Start(client, ctx, "producer-1", producerSvc.Produce, ProducerState{
		Items:             []string{"alpha", "bravo", "charlie", "delta"},
		ConsumerStateroutineID: "consumer-1",
	}); err != nil {
		log.Fatal(err)
	}

	// Wait for the consumer to finish processing all items.
	result, err := consumerH.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("consumer received %d items\n", len(result.Received))
}
