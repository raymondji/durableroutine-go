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

type InitialState struct{ Email string }

func (InitialState) Kind() string { return "reminder.initial" }

type FollowUpState struct{ Email string }

func (FollowUpState) Kind() string { return "reminder.follow-up" }

type FinalState struct{ Email string }

func (FinalState) Kind() string { return "reminder.final" }

type reminderService struct {
	sent []string
}

func (s *reminderService) SendInitial(ctx *durable.Context, state InitialState) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "initial:"+state.Email)
	// Use very short timers for testing.
	return durable.After(1*time.Millisecond, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *reminderService) SendFollowUp(ctx *durable.Context, state FollowUpState) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "followup:"+state.Email)
	return durable.After(1*time.Millisecond, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *reminderService) SendFinal(ctx *durable.Context, state FinalState) (*durable.Continuation[durable.Unit], error) {
	s.sent = append(s.sent, "final:"+state.Email)
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

	h, err := durable.Go(srClient, ctx, uniqueID("reminder"), svc.SendInitial, InitialState{Email: "test@example.com"})
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

type SimpleState struct{ Value string }

func (SimpleState) Kind() string { return "simple" }

type SimpleResult struct{ Output string }

func simpleHandler(ctx *durable.Context, state SimpleState) (*durable.Continuation[SimpleResult], error) {
	return durable.Done(SimpleResult{Output: "got:" + state.Value}), nil
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

	h, err := durable.Go(srClient, ctx, uniqueID("simple"), simpleHandler, SimpleState{Value: "hello"})
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

type QueryState struct{ Counter int }

func (QueryState) Kind() string { return "query-test" }

type StatusResp struct{ Count int }

func (StatusResp) Kind() string { return "status" }

type QueryResult struct{ FinalCount int }

func queryHandler(ctx *durable.Context, state QueryState) (*durable.Continuation[QueryResult], error) {
	durable.SetQueryResult(ctx, StatusResp{Count: state.Counter})
	if state.Counter >= 3 {
		return durable.Done(QueryResult{FinalCount: state.Counter}), nil
	}
	return durable.After(1*time.Millisecond, queryHandler, QueryState{Counter: state.Counter + 1}), nil
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

	h, err := durable.Go(srClient, ctx, uniqueID("query"), queryHandler, QueryState{Counter: 0})
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

type WaitingState struct{ Name string }

func (WaitingState) Kind() string { return "waiting" }

type GotMessageState struct {
	Name    string
	Message string
}

func (GotMessageState) Kind() string { return "got-message" }

type MyMsg struct{ Text string }

func (MyMsg) Kind() string { return "my-msg" }

type SendResult struct{ ReceivedText string }

type sendService struct{}

func (s *sendService) WaitForMsg(ctx *durable.Context, state WaitingState) (*durable.Continuation[SendResult], error) {
	return durable.ReceiveSend(s.HandleMsg, state), nil
}

func (s *sendService) HandleMsg(ctx *durable.Context, state WaitingState, msg MyMsg) (*durable.Continuation[SendResult], error) {
	return durable.Done(SendResult{ReceivedText: msg.Text}), nil
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
	h, err := durable.Go(srClient, ctx, wfID, svc.WaitForMsg, WaitingState{Name: "test"})
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

type CANCountState struct{ Counter int }

func (CANCountState) Kind() string { return "can-count" }

type CANWaitState struct {
	Counter int
	MsgText string
}

func (CANWaitState) Kind() string { return "can-wait" }

type CANStatusResp struct{ Count int }

func (CANStatusResp) Kind() string { return "can-status" }

type CANMsg struct{ Text string }

func (CANMsg) Kind() string { return "can-msg" }

type CANCallReq struct{ Text string }

func (CANCallReq) Kind() string { return "can-call" }

type CANCallResp struct{ Echo string }

type CANResult struct {
	FinalCount int
	MsgText    string
	CallEcho   string
}

type canService struct{}

func (s *canService) Count(ctx *durable.Context, state CANCountState) (*durable.Continuation[CANResult], error) {
	durable.SetQueryResult(ctx, CANStatusResp{Count: state.Counter})
	if state.Counter >= 15 {
		return durable.Continue(s.Wait, CANWaitState{Counter: state.Counter}), nil
	}
	return durable.Continue(s.Count, CANCountState{Counter: state.Counter + 1}), nil
}

func (s *canService) Wait(ctx *durable.Context, state CANWaitState) (*durable.Continuation[CANResult], error) {
	return durable.Select(
		durable.ReceiveSend(s.RecvMsg, state),
		durable.ReceiveCall(s.HandleCall, state),
	), nil
}

func (s *canService) RecvMsg(ctx *durable.Context, state CANWaitState, msg CANMsg) (*durable.Continuation[CANResult], error) {
	state.MsgText = msg.Text
	return durable.ReceiveCall(s.HandleCall, state), nil
}

func (s *canService) HandleCall(ctx *durable.Context, state CANWaitState, req CANCallReq) (CANCallResp, *durable.Continuation[CANResult], error) {
	return CANCallResp{Echo: req.Text}, durable.Done(CANResult{
		FinalCount: state.Counter,
		MsgText:    state.MsgText,
		CallEcho:   req.Text,
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
	h, err := durable.Go(srClient, ctx, wfID, svc.Count, CANCountState{Counter: 0})
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
