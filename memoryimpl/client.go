package memoryimpl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/raymondji/stateroutine/stateroutine"
)

type client struct {
	runtime *Runtime
}

var _ stateroutine.ClientImpl = (*client)(nil)

func (c *client) Start(_ context.Context, id string, kind string, state any) error {
	return c.runtime.start(id, kind, state)
}

func (c *client) Send(_ context.Context, id string, stateKind string, msgKind string, msg any) error {
	signalKey := "send:" + stateKind + ":" + msgKind
	inst, ok := c.runtime.instances[id]
	if !ok {
		return fmt.Errorf("stateroutine %s not found", id)
	}
	inst.signals[signalKey] = append(inst.signals[signalKey], msg)
	return nil
}

func (c *client) Call(_ context.Context, id string, stateKind string, reqKind string, req any) (any, error) {
	inst, ok := c.runtime.instances[id]
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}

	callName := reqKind
	handlerKey := "call:" + stateKind + ":" + reqKind

	// Find matching OnCall case.
	for _, cas := range inst.cases {
		if cas.CallName() != callName {
			continue
		}

		entry, ok := c.runtime.handlers[handlerKey]
		if !ok {
			return nil, fmt.Errorf("no handler registered for key: %s", handlerKey)
		}

		rawState, err := json.Marshal(cas.State())
		if err != nil {
			return nil, fmt.Errorf("marshal state: %w", err)
		}
		rawReq, err := json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("marshal req: %w", err)
		}

		sctx := stateroutine.NewContext(context.Background(), inst.id)
		output, err := entry.Runner(sctx, rawState, rawReq, "")
		if err != nil {
			return nil, err
		}

		// Unmarshal the call response before processing output (which may modify inst).
		var callResp any
		if output.CallResponse != nil {
			if err := json.Unmarshal(output.CallResponse, &callResp); err != nil {
				return nil, fmt.Errorf("unmarshal call response: %w", err)
			}
		}

		c.runtime.processOutput(inst, output, sctx)
		return callResp, nil
	}

	return nil, fmt.Errorf("no matching OnCall case for %s on stateroutine %s", handlerKey, id)
}

func (c *client) Query(_ context.Context, id string, queryName string) (any, error) {
	inst, ok := c.runtime.instances[id]
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}
	result, ok := inst.queryResults[queryName]
	if !ok {
		return nil, fmt.Errorf("no query result for %s on stateroutine %s", queryName, id)
	}
	return result, nil
}

func (c *client) Get(_ context.Context, id string) (any, error) {
	inst, ok := c.runtime.instances[id]
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}
	if inst.done {
		if inst.err != nil {
			return nil, inst.err
		}
		return inst.result, nil
	}

	// Block until done.
	ch := make(chan struct{})
	inst.waiters = append(inst.waiters, ch)
	<-ch

	if inst.err != nil {
		return nil, inst.err
	}
	return inst.result, nil
}
