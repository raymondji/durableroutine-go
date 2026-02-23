package durable

import "context"

// Client starts and interacts with durable routines.
// Methods are unexported because callers use the typed top-level
// wrapper functions (Start, ClientCast, ClientCall, ClientQuery).
type Client interface {
	start(ctx context.Context, id string, kind string, args any) error
	cast(ctx context.Context, id string, inboxName string, msg any) error
	call(ctx context.Context, id string, methodName string, req any) (any, error)
	query(ctx context.Context, id string, queryName string, req any) (any, error)
}

// NewClient creates a Client backed by Temporal.
// TODO: accept Temporal connection options.
func NewClient() Client {
	panic("not implemented")
}

// Start begins a new instance of a routine. The args.Kind() determines which
// registered handler runs.
func Start[Args RoutineArgs](c Client, ctx context.Context, id string, args Args) error {
	return c.start(ctx, id, args.Kind(), args)
}

// ClientCast sends a fire-and-forget message to a routine's inbox.
func ClientCast[M any](c Client, ctx context.Context, id string,
	inbox Inbox[M], msg M) error {
	return c.cast(ctx, id, inbox.Name, msg)
}

// ClientCall sends a synchronous request to a routine's method and waits
// for the response.
func ClientCall[Req, Resp any](c Client, ctx context.Context, id string,
	method Method[Req, Resp], req Req) (Resp, error) {
	raw, err := c.call(ctx, id, method.Name, req)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return raw.(Resp), nil
}

// ClientQuery sends a synchronous read-only query to a routine.
func ClientQuery[Req, Resp any](c Client, ctx context.Context, id string,
	query Query[Req, Resp], req Req) (Resp, error) {
	raw, err := c.query(ctx, id, query.Name, req)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return raw.(Resp), nil
}
