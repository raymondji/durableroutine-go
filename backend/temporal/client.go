// Package temporal implements routine backed by Temporal.
package temporal

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/internal/durablecore"
)

// Client implements durable.ClientImpl backed by Temporal.
type Client struct {
	temporal  temporalclient.Client
	taskQueue string

	// MaxHistoryLength, when > 0, triggers continue-as-new when the workflow
	// history exceeds this many events. Useful for testing CAN logic with a
	// small threshold. When 0, only Temporal's GetContinueAsNewSuggested is used.
	MaxHistoryLength int32
}

var _ durable.ClientImpl = (*Client)(nil)

// NewClient creates a durable Client backed by the given Temporal client.
func NewClient(tc temporalclient.Client, taskQueue string) *Client {
	return &Client{temporal: tc, taskQueue: taskQueue}
}

func (c *Client) Go(ctx context.Context, id string, kind string, resultKind string, input any) error {
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}
	opts := temporalclient.StartWorkflowOptions{
		ID:                    id,
		TaskQueue:             c.taskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}
	wfInput := WorkflowInput{
		HandlerKey:       durablecore.HandlerKey(kind, resultKind),
		Input:            inputBytes,
		MaxHistoryLength: c.MaxHistoryLength,
	}
	var wh *workflowHandler
	_, err = c.temporal.ExecuteWorkflow(ctx, opts, wh.RoutineWorkflow, wfInput)
	return err
}

func (c *Client) Send(ctx context.Context, id string, inputKind string, externalInputKind string, resultKind string, externalInput any) error {
	signalName := durablecore.SendKey(inputKind, externalInputKind, resultKind)
	return c.temporal.SignalWorkflow(ctx, id, "", signalName, externalInput)
}

func (c *Client) Call(ctx context.Context, id string, inputKind string, externalReqKind string, externalRespKind string, resultKind string, externalReq any) (any, error) {
	updateName := durablecore.CallKey(inputKind, externalReqKind, externalRespKind, resultKind)
	handle, err := c.temporal.UpdateWorkflow(ctx, temporalclient.UpdateWorkflowOptions{
		WorkflowID:   id,
		UpdateName:   updateName,
		Args:         []any{externalReq},
		WaitForStage: temporalclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, fmt.Errorf("update workflow: %w", err)
	}
	var result any
	if err := handle.Get(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Query(ctx context.Context, id string, queryName string) (any, error) {
	resp, err := c.temporal.QueryWorkflow(ctx, id, "", queryName)
	if err != nil {
		return nil, err
	}
	var result any
	if err := resp.Get(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Get(ctx context.Context, id string) (any, error) {
	run := c.temporal.GetWorkflow(ctx, id, "")
	var result any
	if err := run.Get(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}
