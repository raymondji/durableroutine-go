// Package pipeline demonstrates a producer-consumer pattern between two
// durable routines. The producer generates items one at a time and sends
// each to the consumer via durable.BufferSend.
// Uses struct-based handlers for dependency injection.
package pipeline

import (
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
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
	ConsumerRoutineID string
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

func (s *ProducerService) Produce(ctx *durable.Context, state ProducerState) (*durable.Continuation[durable.Unit], error) {
	var stub *ConsumerService
	for i, data := range state.Items {
		item := Item{Seq: i, Data: data}
		durable.BufferSend(ctx, state.ConsumerRoutineID, stub.ReceiveItem, item)
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	durable.BufferSend(ctx, state.ConsumerRoutineID, stub.ReceiveDone, DoneMsg{})
	fmt.Println("producer finished")
	return durable.Done(durable.Unit{}), nil
}

// --- Consumer service ---

type ConsumerService struct {
	// Injected dependencies would go here.
}

func (s *ConsumerService) StartConsumer(ctx *durable.Context, state ConsumerState) (*durable.Continuation[ConsumerResult], error) {
	return durable.Select(
		durable.ReceiveSend(s.ReceiveItem, state),
		durable.ReceiveSend(s.ReceiveDone, state),
	), nil
}

func (s *ConsumerService) ReceiveItem(ctx *durable.Context, state ConsumerState, item Item) (*durable.Continuation[ConsumerResult], error) {
	fmt.Printf("consumer %s received item %d: %s\n", state.Name, item.Seq, item.Data)
	state.Received = append(state.Received, item)

	return durable.Select(
		durable.ReceiveSend(s.ReceiveItem, state),
		durable.ReceiveSend(s.ReceiveDone, state),
	), nil
}

func (s *ConsumerService) ReceiveDone(ctx *durable.Context, state ConsumerState, _ DoneMsg) (*durable.Continuation[ConsumerResult], error) {
	fmt.Printf("consumer %s done, received %d items\n", state.Name, len(state.Received))
	return durable.Done(ConsumerResult{Received: state.Received}), nil
}

// RegisterHandlers registers all pipeline handlers with the worker.
func RegisterHandlers(w *durable.Worker, producerSvc *ProducerService, consumerSvc *ConsumerService) {
	durable.RegisterHandler(w, producerSvc.Produce, durable.HandlerOptions{})
	durable.RegisterHandler(w, consumerSvc.StartConsumer, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, consumerSvc.ReceiveItem, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, consumerSvc.ReceiveDone, durable.HandlerOptions{})
}
