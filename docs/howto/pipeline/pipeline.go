// Package pipeline demonstrates a producer-consumer pattern between two
// durable stateroutines. The producer generates items one at a time and sends
// each to the consumer via stateroutine.BufferSend.
// Uses struct-based handlers for dependency injection.
package pipeline

import (
	"fmt"

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
	Items                  []string
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
	var stub *ConsumerService
	for i, data := range state.Items {
		item := Item{Seq: i, Data: data}
		stateroutine.BufferSend(ctx, state.ConsumerStateroutineID, stub.ReceiveItem, item)
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	stateroutine.BufferSend(ctx, state.ConsumerStateroutineID, stub.ReceiveDone, DoneMsg{})
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

// RegisterHandlers registers all pipeline handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, producerSvc *ProducerService, consumerSvc *ConsumerService) {
	stateroutine.RegisterHandler(w, producerSvc.Produce, stateroutine.HandlerOptions{})
	stateroutine.RegisterHandler(w, consumerSvc.StartConsumer, stateroutine.HandlerOptions{})
	stateroutine.RegisterSendHandler(w, consumerSvc.ReceiveItem, stateroutine.HandlerOptions{})
	stateroutine.RegisterSendHandler(w, consumerSvc.ReceiveDone, stateroutine.HandlerOptions{})
}
