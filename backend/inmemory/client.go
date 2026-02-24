package inmemory

import (
	"context"
	"fmt"

	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/internal/durablecore"
)

type client struct {
	runtime *Runtime
}

var _ durable.ClientImpl = (*client)(nil)

func (c *client) Go(_ context.Context, id string, kind string, resultKind string, state any) error {
	return c.runtime.start(id, kind, resultKind, state)
}

func (c *client) Send(_ context.Context, id string, stateKind string, msgKind string, resultKind string, msg any) error {
	sendKey := durablecore.SendKey(stateKind, msgKind, resultKind)
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return fmt.Errorf("routine %s not found", id)
	}
	inst.sendCh <- sendMsg{key: sendKey, msg: msg}
	return nil
}

func (c *client) Call(_ context.Context, id string, stateKind string, reqKind string, respKind string, resultKind string, req any) (any, error) {
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("routine %s not found", id)
	}

	callName := reqKind
	handlerKey := durablecore.CallKey(stateKind, reqKind, respKind, resultKind)
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
		return nil, fmt.Errorf("routine %s not found", id)
	}

	inst.mu.Lock()
	result, ok := inst.queryResults[queryName]
	inst.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no query result for %s on routine %s", queryName, id)
	}
	return result, nil
}

func (c *client) Get(_ context.Context, id string) (any, error) {
	c.runtime.mu.Lock()
	inst, ok := c.runtime.instances[id]
	c.runtime.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("routine %s not found", id)
	}

	// Block until the instance is done.
	<-inst.doneChan

	if inst.err != nil {
		return nil, inst.err
	}
	return inst.result, nil
}
