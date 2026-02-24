package stateroutine

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
)

// Client starts and interacts with durable stateroutines.
// Methods are unexported because callers use the typed top-level
// wrapper functions (Start, ClientSend, ClientCall, ClientQuery, ClientGet).
type Client interface {
	start(ctx context.Context, id string, kind string, state any) error
	send(ctx context.Context, id string, stateKind string, msgKind string, msg any) error
	call(ctx context.Context, id string, stateKind string, reqKind string, req any) (any, error)
	query(ctx context.Context, id string, queryName string) (any, error)
	get(ctx context.Context, id string) (any, error)
}

// ClientImpl is the exported interface that implementation packages
// (e.g., temporalimpl) implement. Use NewClientFrom to wrap a ClientImpl
// into a Client usable with the typed wrapper functions.
type ClientImpl interface {
	Start(ctx context.Context, id string, kind string, state any) error
	Send(ctx context.Context, id string, stateKind string, msgKind string, msg any) error
	Call(ctx context.Context, id string, stateKind string, reqKind string, req any) (any, error)
	Query(ctx context.Context, id string, queryName string) (any, error)
	Get(ctx context.Context, id string) (any, error)
}

// NewClientFrom wraps a ClientImpl into a Client.
func NewClientFrom(impl ClientImpl) Client {
	return &clientBridge{impl: impl}
}

type clientBridge struct {
	impl ClientImpl
}

func (b *clientBridge) start(ctx context.Context, id string, kind string, state any) error {
	return b.impl.Start(ctx, id, kind, state)
}
func (b *clientBridge) send(ctx context.Context, id string, stateKind string, msgKind string, msg any) error {
	return b.impl.Send(ctx, id, stateKind, msgKind, msg)
}
func (b *clientBridge) call(ctx context.Context, id string, stateKind string, reqKind string, req any) (any, error) {
	return b.impl.Call(ctx, id, stateKind, reqKind, req)
}
func (b *clientBridge) query(ctx context.Context, id string, queryName string) (any, error) {
	return b.impl.Query(ctx, id, queryName)
}
func (b *clientBridge) get(ctx context.Context, id string) (any, error) {
	return b.impl.Get(ctx, id)
}

// Handle is a typed reference to a running stateroutine. It is returned by Start
// and carries the result type T so that Get does not require manual type
// specification.
type Handle[T any] struct {
	client Client
	id     string
}

// Get retrieves the result of the stateroutine. Blocks until the stateroutine completes.
// Maps to Temporal's WorkflowRun.Get.
func (h Handle[T]) Get(ctx context.Context) (T, error) {
	raw, err := h.client.get(ctx, h.id)
	if err != nil {
		var zero T
		return zero, err
	}
	return convertResult[T](raw)
}

// Start begins a new instance of a stateroutine. The state.Kind() determines which
// registered handler runs (looked up by "handler:{kind}"). The handler
// parameter is used only for type inference of the result type T — it is not
// called. Pass the same function registered with AddHandler.
//
// If id is empty, a random UUID is generated.
// Returns a typed Handle for retrieving the result.
func Start[S HandlerState, T any](c Client, ctx context.Context, id string, handler HandlerFunc[S, T], state S) (Handle[T], error) {
	if id == "" {
		id = newUUID()
	}
	err := c.start(ctx, id, state.Kind(), state)
	if err != nil {
		var zero Handle[T]
		return zero, err
	}
	return Handle[T]{client: c, id: id}, nil
}

// ClientSend sends a fire-and-forget message to a stateroutine's inbox.
// The handler parameter is used only for type inference of the target state
// type — it is not called. Pass the same function registered with
// AddSendHandler. The state kind and message kind are derived from the handler's
// type parameters to build the correct routing key.
func ClientSend[S HandlerState, M Message, T any](c Client, ctx context.Context, id string,
	handler SendFunc[S, M, T], msg M) error {
	var zeroS S
	return c.send(ctx, id, zeroS.Kind(), msg.Kind(), msg)
}

// ClientCall sends a synchronous request to a stateroutine's method and waits
// for the response. The handler parameter is used only for type inference of
// the target state and response types — it is not called. Pass the same
// function registered with AddCallHandler. Both stateKind and reqKind are
// derived from the handler's type parameters for correct routing.
func ClientCall[S HandlerState, Req Message, Resp any, T any](c Client, ctx context.Context, id string,
	handler CallFunc[S, Req, Resp, T], req Req) (Resp, error) {
	var zeroS S
	raw, err := c.call(ctx, id, zeroS.Kind(), req.Kind(), req)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return convertResult[Resp](raw)
}

// ClientQuery retrieves a static query result from a stateroutine.
// The query name is derived from resp.Kind(). Pass a zero value of the
// response type for routing and type inference.
func ClientQuery[Resp Message](c Client, ctx context.Context, id string, resp Resp) (Resp, error) {
	raw, err := c.query(ctx, id, resp.Kind())
	if err != nil {
		var zero Resp
		return zero, err
	}
	return convertResult[Resp](raw)
}

// ClientGet retrieves the result of a completed stateroutine by ID. Blocks until
// the stateroutine completes. Prefer using Handle.Get when you have a Handle from
// Start. This function is useful when you only have the stateroutine ID (e.g.,
// from a config or database). Maps to Temporal's WorkflowRun.Get.
func ClientGet[T any](c Client, ctx context.Context, id string) (T, error) {
	raw, err := c.get(ctx, id)
	if err != nil {
		var zero T
		return zero, err
	}
	return convertResult[T](raw)
}

// convertResult converts a raw value (which may be map[string]interface{} from
// JSON deserialization) into the target type T via JSON round-trip if needed.
func convertResult[T any](raw any) (T, error) {
	if typed, ok := raw.(T); ok {
		return typed, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("marshal result: %w", err)
	}
	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		var zero T
		return zero, fmt.Errorf("unmarshal result into %T: %w", result, err)
	}
	return result, nil
}

// newUUID generates a random v4 UUID string.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 1
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
