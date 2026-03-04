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

func (Item) DurableKind() string { return "items" }

type DoneMsg struct{}

func (DoneMsg) DurableKind() string { return "done" }

// --- State ---

type ProducerInput struct {
	Items             []string
	ConsumerRoutineID string
}

func (ProducerInput) DurableKind() string { return "producer" }

type ConsumerInput struct {
	Name     string
	Received []Item
}

func (ConsumerInput) DurableKind() string { return "consumer" }

// --- Results ---

type ConsumerResult struct {
	Received []Item
}

func (ConsumerResult) DurableKind() string { return "consumer-result" }

// --- Producer service ---

type ProducerService struct {
	// Injected dependencies would go here.
}

func (s *ProducerService) Produce(ctx *durable.Context, input ProducerInput) (*durable.Continuation[durable.Unit], error) {
	var stub *ConsumerService
	for i, data := range input.Items {
		item := Item{Seq: i, Data: data}
		durable.BufferSend(ctx, input.ConsumerRoutineID, stub.ReceiveItem, item)
		fmt.Printf("produced item %d: %s\n", i, data)
	}

	durable.BufferSend(ctx, input.ConsumerRoutineID, stub.ReceiveDone, DoneMsg{})
	fmt.Println("producer finished")
	return durable.Done(durable.Unit{}), nil
}

// --- Consumer service ---

type ConsumerService struct {
	// Injected dependencies would go here.
}

func (s *ConsumerService) StartConsumer(ctx *durable.Context, input ConsumerInput) (*durable.Continuation[ConsumerResult], error) {
	return durable.Select(
		durable.ReceiveSend(s.ReceiveItem, input),
		durable.ReceiveSend(s.ReceiveDone, input),
	), nil
}

func (s *ConsumerService) ReceiveItem(ctx *durable.Context, input ConsumerInput, externalInput Item) (*durable.Continuation[ConsumerResult], error) {
	fmt.Printf("consumer %s received item %d: %s\n", input.Name, externalInput.Seq, externalInput.Data)
	input.Received = append(input.Received, externalInput)

	return durable.Select(
		durable.ReceiveSend(s.ReceiveItem, input),
		durable.ReceiveSend(s.ReceiveDone, input),
	), nil
}

func (s *ConsumerService) ReceiveDone(ctx *durable.Context, input ConsumerInput, _ DoneMsg) (*durable.Continuation[ConsumerResult], error) {
	fmt.Printf("consumer %s done, received %d items\n", input.Name, len(input.Received))
	return durable.Done(ConsumerResult{Received: input.Received}), nil
}

// RegisterHandlers registers all pipeline handlers with the worker.
func RegisterHandlers(w *durable.Worker, producerSvc *ProducerService, consumerSvc *ConsumerService) {
	durable.RegisterHandler(w, producerSvc.Produce, durable.HandlerOptions{})
	durable.RegisterHandler(w, consumerSvc.StartConsumer, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, consumerSvc.ReceiveItem, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, consumerSvc.ReceiveDone, durable.HandlerOptions{})
}
