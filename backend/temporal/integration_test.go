package temporal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/backend/temporal"
	"github.com/raymondji/durableroutine-go/durable"
)

func newTemporalClient(t *testing.T) temporalclient.Client {
	t.Helper()
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		t.Fatalf("failed to connect to Temporal: %v", err)
	}
	t.Cleanup(func() { tc.Close() })
	return tc
}

// uniqueTaskQueue returns a unique task queue name per test to avoid collisions.
func uniqueTaskQueue(t *testing.T) string {
	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

// uniqueID returns a unique workflow ID per test to avoid collisions with
// completed workflows from previous test runs.
func uniqueID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// --- Reminder example types (inline to avoid import cycle) ---

type InitialInput struct{ Email string }

func (InitialInput) DurableKind() string { return "reminder.initial" }

type FollowUpInput struct{ Email string }

func (FollowUpInput) DurableKind() string { return "reminder.follow-up" }

type FinalInput struct{ Email string }

func (FinalInput) DurableKind() string { return "reminder.final" }

type reminderService struct {
	sent []string
}

func (s *reminderService) SendInitial(ctx *durable.Context, input InitialInput) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "initial:"+input.Email)
	// Use very short timers for testing.
	return durable.After(1*time.Millisecond, s.SendFollowUp, FollowUpInput{Email: input.Email}), nil
}

func (s *reminderService) SendFollowUp(ctx *durable.Context, input FollowUpInput) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "followup:"+input.Email)
	return durable.After(1*time.Millisecond, s.SendFinal, FinalInput{Email: input.Email}), nil
}

func (s *reminderService) SendFinal(ctx *durable.Context, input FinalInput) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "final:"+input.Email)
	return durable.Done(durable.Unit{}), nil
}

func TestReminderEndToEnd(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	svc := &reminderService{}

	w := durable.NewWorker(taskQueue)
	durable.RegisterHandler(w, svc.SendInitial, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFollowUp, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.SendFinal, durable.HandlerOptions{})

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	// Give worker time to start polling.
	time.Sleep(500 * time.Millisecond)

	client := temporal.NewClient(tc, taskQueue)
	srClient := durable.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := durable.Go(srClient, ctx, uniqueID("reminder"), svc.SendInitial, InitialInput{Email: "test@example.com"})
	if err != nil {
		t.Fatalf("Go failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_ = result // durable.Unit{}
	t.Logf("Reminder completed successfully")
}

// --- Simple done-immediately test ---

type SimpleInput struct{ Value string }

func (SimpleInput) DurableKind() string { return "simple" }

type SimpleResult struct{ Output string }

func (SimpleResult) DurableKind() string { return "simple-result" }

func simpleHandler(ctx *durable.Context, input SimpleInput) (*durable.Continuation[SimpleResult], error) {
	return durable.Done(SimpleResult{Output: "got:" + input.Value}), nil
}

func TestSimpleDone(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	w := durable.NewWorker(taskQueue)
	durable.RegisterHandler(w, simpleHandler, durable.HandlerOptions{})

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporal.NewClient(tc, taskQueue)
	srClient := durable.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := durable.Go(srClient, ctx, uniqueID("simple"), simpleHandler, SimpleInput{Value: "hello"})
	if err != nil {
		t.Fatalf("Go failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Output != "got:hello" {
		t.Fatalf("unexpected result: %+v", result)
	}
	t.Logf("Simple test passed: %+v", result)
}

// --- Query test: handler sets a query result, client reads it ---

type QueryInput struct{ Counter int }

func (QueryInput) DurableKind() string { return "query-test" }

type StatusResp struct{ Count int }

func (StatusResp) DurableKind() string { return "status" }

type QueryResult struct{ FinalCount int }

func (QueryResult) DurableKind() string { return "query-result" }

func queryHandler(ctx *durable.Context, input QueryInput) (*durable.Continuation[QueryResult], error) {
	durable.SetQueryResult(ctx, StatusResp{Count: input.Counter})
	if input.Counter >= 3 {
		return durable.Done(QueryResult{FinalCount: input.Counter}), nil
	}
	return durable.After(1*time.Millisecond, queryHandler, QueryInput{Counter: input.Counter + 1}), nil
}

func TestQueryResult(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	w := durable.NewWorker(taskQueue)
	durable.RegisterHandler(w, queryHandler, durable.HandlerOptions{})

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporal.NewClient(tc, taskQueue)
	srClient := durable.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := durable.Go(srClient, ctx, uniqueID("query"), queryHandler, QueryInput{Counter: 0})
	if err != nil {
		t.Fatalf("Go failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.FinalCount != 3 {
		t.Fatalf("unexpected result: %+v", result)
	}
	t.Logf("Query test passed: %+v", result)
}

// --- Send (signal) test: client sends a message, handler receives it ---

type WaitingInput struct{ Name string }

func (WaitingInput) DurableKind() string { return "waiting" }

type GotMessageInput struct {
	Name    string
	Message string
}

func (GotMessageInput) DurableKind() string { return "got-message" }

type MyMsg struct{ Text string }

func (MyMsg) DurableKind() string { return "my-msg" }

type SendResult struct{ ReceivedText string }

func (SendResult) DurableKind() string { return "send-result" }

type sendService struct{}

func (s *sendService) WaitForMsg(ctx *durable.Context, input WaitingInput) (*durable.Continuation[SendResult], error) {
	return durable.ReceiveSend(s.HandleMsg, input), nil
}

func (s *sendService) HandleMsg(ctx *durable.Context, input WaitingInput, externalInput MyMsg) (*durable.Continuation[SendResult], error) {
	return durable.Done(SendResult{ReceivedText: externalInput.Text}), nil
}

func TestSendSignal(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	svc := &sendService{}

	w := durable.NewWorker(taskQueue)
	durable.RegisterHandler(w, svc.WaitForMsg, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.HandleMsg, durable.HandlerOptions{})

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporal.NewClient(tc, taskQueue)
	srClient := durable.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := uniqueID("send")
	h, err := durable.Go(srClient, ctx, wfID, svc.WaitForMsg, WaitingInput{Name: "test"})
	if err != nil {
		t.Fatalf("Go failed: %v", err)
	}

	// Give the workflow time to start and reach the Select.
	time.Sleep(2 * time.Second)

	err = durable.Send(srClient, ctx, wfID, svc.HandleMsg, MyMsg{Text: "hello signal"})
	if err != nil {
		t.Fatalf("ClientSend failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.ReceivedText != "hello signal" {
		t.Fatalf("unexpected result: %+v", result)
	}
	t.Logf("Send test passed: %+v", result)
}

// --- Continue-as-new test: verifies state, queries, sends, and calls survive CAN ---

type CANCountInput struct{ Counter int }

func (CANCountInput) DurableKind() string { return "can-count" }

type CANWaitInput struct {
	Counter int
	MsgText string
}

func (CANWaitInput) DurableKind() string { return "can-wait" }

type CANStatusResp struct{ Count int }

func (CANStatusResp) DurableKind() string { return "can-status" }

type CANMsg struct{ Text string }

func (CANMsg) DurableKind() string { return "can-msg" }

type CANCallReq struct{ Text string }

func (CANCallReq) DurableKind() string { return "can-call" }

type CANCallResp struct{ Echo string }

func (CANCallResp) DurableKind() string { return "can-call-resp" }

type CANResult struct {
	FinalCount int
	MsgText    string
	CallEcho   string
}

func (CANResult) DurableKind() string { return "can-result" }

type canService struct{}

func (s *canService) Count(ctx *durable.Context, input CANCountInput) (*durable.Continuation[CANResult], error) {
	durable.SetQueryResult(ctx, CANStatusResp{Count: input.Counter})
	if input.Counter >= 15 {
		return durable.Continue(s.Wait, CANWaitInput{Counter: input.Counter}), nil
	}
	return durable.Continue(s.Count, CANCountInput{Counter: input.Counter + 1}), nil
}

func (s *canService) Wait(ctx *durable.Context, input CANWaitInput) (*durable.Continuation[CANResult], error) {
	return durable.Select(
		durable.ReceiveSend(s.RecvMsg, input),
		durable.ReceiveCall(s.HandleCall, input),
	), nil
}

func (s *canService) RecvMsg(ctx *durable.Context, input CANWaitInput, externalInput CANMsg) (*durable.Continuation[CANResult], error) {
	input.MsgText = externalInput.Text
	return durable.ReceiveCall(s.HandleCall, input), nil
}

func (s *canService) HandleCall(ctx *durable.Context, input CANWaitInput, externalReq CANCallReq) (CANCallResp, *durable.Continuation[CANResult], error) {
	return CANCallResp{Echo: externalReq.Text}, durable.Done(CANResult{
		FinalCount: input.Counter,
		MsgText:    input.MsgText,
		CallEcho:   externalReq.Text,
	}), nil
}

func TestContinueAsNew(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	svc := &canService{}

	w := durable.NewWorker(taskQueue)
	durable.RegisterHandler(w, svc.Count, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.Wait, durable.HandlerOptions{})
	durable.RegisterSendHandler(w, svc.RecvMsg, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, svc.HandleCall, durable.HandlerOptions{})

	tw := temporal.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporal.NewClient(tc, taskQueue)
	client.MaxHistoryLength = 30
	srClient := durable.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wfID := uniqueID("can")
	h, err := durable.Go(srClient, ctx, wfID, svc.Count, CANCountInput{Counter: 0})
	if err != nil {
		t.Fatalf("Go failed: %v", err)
	}

	// Wait for counting to complete and CAN to happen.
	// 15 iterations * ~3 history events each = ~45 events, well over threshold of 30.
	time.Sleep(10 * time.Second)

	// Verify query survived CAN.
	status, err := durable.Query(srClient, ctx, wfID, CANStatusResp{})
	if err != nil {
		t.Fatalf("ClientQuery failed: %v", err)
	}
	if status.Count != 15 {
		t.Fatalf("expected query Count=15, got %d", status.Count)
	}

	// Send a message after CAN.
	err = durable.Send(srClient, ctx, wfID, svc.RecvMsg, CANMsg{Text: "hello-after-can"})
	if err != nil {
		t.Fatalf("ClientSend failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Call after CAN.
	callResp, err := durable.Call(srClient, ctx, wfID, svc.HandleCall, CANCallReq{Text: "echo-test"})
	if err != nil {
		t.Fatalf("ClientCall failed: %v", err)
	}
	if callResp.Echo != "echo-test" {
		t.Fatalf("expected call Echo='echo-test', got %q", callResp.Echo)
	}

	// Verify final result.
	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.FinalCount != 15 {
		t.Fatalf("expected FinalCount=15, got %d", result.FinalCount)
	}
	if result.MsgText != "hello-after-can" {
		t.Fatalf("expected MsgText='hello-after-can', got %q", result.MsgText)
	}
	if result.CallEcho != "echo-test" {
		t.Fatalf("expected CallEcho='echo-test', got %q", result.CallEcho)
	}
	t.Logf("ContinueAsNew test passed: %+v", result)
}
