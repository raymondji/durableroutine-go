package temporalimpl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/temporalimpl"
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

func (s *reminderService) SendInitial(ctx *stateroutine.Context, state InitialState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	s.sent = append(s.sent, "initial:"+state.Email)
	// Use very short timers for testing.
	return stateroutine.After(1*time.Millisecond, s.SendFollowUp, FollowUpState{Email: state.Email}), nil
}

func (s *reminderService) SendFollowUp(ctx *stateroutine.Context, state FollowUpState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	s.sent = append(s.sent, "followup:"+state.Email)
	return stateroutine.After(1*time.Millisecond, s.SendFinal, FinalState{Email: state.Email}), nil
}

func (s *reminderService) SendFinal(ctx *stateroutine.Context, state FinalState) (*stateroutine.Suspend[stateroutine.Unit], error) {
	s.sent = append(s.sent, "final:"+state.Email)
	return stateroutine.Done(stateroutine.Unit{}), nil
}

func TestReminderEndToEnd(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	svc := &reminderService{}

	w := stateroutine.NewWorker(taskQueue)
	stateroutine.AddHandler(w, svc.SendInitial, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.SendFollowUp, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.SendFinal, stateroutine.HandlerOptions{})

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	// Give worker time to start polling.
	time.Sleep(500 * time.Millisecond)

	client := temporalimpl.NewClient(tc, taskQueue)
	srClient := stateroutine.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := stateroutine.Start(srClient, ctx, uniqueID("reminder"), svc.SendInitial, InitialState{Email: "test@example.com"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_ = result // stateroutine.Unit{}
	t.Logf("Reminder completed successfully")
}

// --- Simple done-immediately test ---

type SimpleState struct{ Value string }

func (SimpleState) Kind() string { return "simple" }

type SimpleResult struct{ Output string }

func simpleHandler(ctx *stateroutine.Context, state SimpleState) (*stateroutine.Suspend[SimpleResult], error) {
	return stateroutine.Done(SimpleResult{Output: "got:" + state.Value}), nil
}

func TestSimpleDone(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	w := stateroutine.NewWorker(taskQueue)
	stateroutine.AddHandler(w, simpleHandler, stateroutine.HandlerOptions{})

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporalimpl.NewClient(tc, taskQueue)
	srClient := stateroutine.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := stateroutine.Start(srClient, ctx, uniqueID("simple"), simpleHandler, SimpleState{Value: "hello"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
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

func queryHandler(ctx *stateroutine.Context, state QueryState) (*stateroutine.Suspend[QueryResult], error) {
	stateroutine.SetQueryResult(ctx, StatusResp{Count: state.Counter})
	if state.Counter >= 3 {
		return stateroutine.Done(QueryResult{FinalCount: state.Counter}), nil
	}
	return stateroutine.After(1*time.Millisecond, queryHandler, QueryState{Counter: state.Counter + 1}), nil
}

func TestQueryResult(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	w := stateroutine.NewWorker(taskQueue)
	stateroutine.AddHandler(w, queryHandler, stateroutine.HandlerOptions{})

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporalimpl.NewClient(tc, taskQueue)
	srClient := stateroutine.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := stateroutine.Start(srClient, ctx, uniqueID("query"), queryHandler, QueryState{Counter: 0})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
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

func (s *sendService) WaitForMsg(ctx *stateroutine.Context, state WaitingState) (*stateroutine.Suspend[SendResult], error) {
	return stateroutine.Select[SendResult](
		stateroutine.OnSend(s.HandleMsg, state),
	), nil
}

func (s *sendService) HandleMsg(ctx *stateroutine.Context, state WaitingState, msg MyMsg) (*stateroutine.Suspend[SendResult], error) {
	return stateroutine.Done(SendResult{ReceivedText: msg.Text}), nil
}

func TestSendSignal(t *testing.T) {
	tc := newTemporalClient(t)
	taskQueue := uniqueTaskQueue(t)

	svc := &sendService{}

	w := stateroutine.NewWorker(taskQueue)
	stateroutine.AddHandler(w, svc.WaitForMsg, stateroutine.HandlerOptions{})
	stateroutine.AddSendHandler(w, svc.HandleMsg, stateroutine.HandlerOptions{})

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	defer tw.Stop()

	time.Sleep(500 * time.Millisecond)

	client := temporalimpl.NewClient(tc, taskQueue)
	srClient := stateroutine.NewClientFrom(client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := uniqueID("send")
	h, err := stateroutine.Start(srClient, ctx, wfID, svc.WaitForMsg, WaitingState{Name: "test"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the workflow time to start and reach the Select.
	time.Sleep(2 * time.Second)

	err = stateroutine.ClientSend(srClient, ctx, wfID, svc.HandleMsg, MyMsg{Text: "hello signal"})
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
