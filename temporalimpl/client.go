// Package temporalimpl implements stateroutine backed by Temporal.
package temporalimpl

import (
	"context"
	"fmt"

	"go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/stateroutine"
)

// Client implements stateroutine.ClientImpl backed by Temporal.
type Client struct {
	temporal  temporalclient.Client
	taskQueue string
}

var _ stateroutine.ClientImpl = (*Client)(nil)

// NewClient creates a stateroutine Client backed by the given Temporal client.
func NewClient(tc temporalclient.Client, taskQueue string) *Client {
	return &Client{temporal: tc, taskQueue: taskQueue}
}

func (c *Client) Start(ctx context.Context, id string, kind string, state any) error {
	opts := temporalclient.StartWorkflowOptions{
		ID:                    id,
		TaskQueue:             c.taskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}
	input := WorkflowInput{
		HandlerKey: "handler:" + kind,
		State:      state,
	}
	_, err := c.temporal.ExecuteWorkflow(ctx, opts, StateroutineWorkflow, input)
	return err
}

func (c *Client) Send(ctx context.Context, id string, stateKind string, msgKind string, msg any) error {
	signalName := "send:" + stateKind + ":" + msgKind
	return c.temporal.SignalWorkflow(ctx, id, "", signalName, msg)
}

func (c *Client) Call(ctx context.Context, id string, stateKind string, reqKind string, req any) (any, error) {
	updateName := "call:" + stateKind + ":" + reqKind
	handle, err := c.temporal.UpdateWorkflow(ctx, temporalclient.UpdateWorkflowOptions{
		WorkflowID:   id,
		UpdateName:   updateName,
		Args:         []any{req},
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
