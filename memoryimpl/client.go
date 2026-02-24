package memoryimpl

import (
	"context"
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
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return fmt.Errorf("stateroutine %s not found", id)
	}
	inst.signalChan <- signal{key: signalKey, msg: msg}
	return nil
}

func (c *client) Call(_ context.Context, id string, stateKind string, reqKind string, req any) (any, error) {
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}

	callName := reqKind
	handlerKey := "call:" + stateKind + ":" + reqKind
	respCh := make(chan callResp, 1)

	inst.callChan <- callReq{
		callName:   callName,
		handlerKey: handlerKey,
		req:        req,
		respCh:     respCh,
	}

	resp := <-respCh
	return resp.result, resp.err
}

func (c *client) Query(_ context.Context, id string, queryName string) (any, error) {
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}

	inst.mu.Lock()
	result, ok := inst.queryResults[queryName]
	inst.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no query result for %s on stateroutine %s", queryName, id)
	}
	return result, nil
}

func (c *client) Get(_ context.Context, id string) (any, error) {
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("stateroutine %s not found", id)
	}

	// Block until the instance is done.
	<-inst.doneChan

	if inst.err != nil {
		return nil, inst.err
	}
	return inst.result, nil
}
