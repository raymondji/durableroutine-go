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
// The inbox name is derived from msg.Kind().
func ClientCast[M Message](c Client, ctx context.Context, id string, msg M) error {
	return c.cast(ctx, id, msg.Kind(), msg)
}

// ClientCall sends a synchronous request to a routine's method and waits
// for the response. The method name is derived from req.Kind(). The handler
// parameter is used only for type inference of the response type — it is not
// called. Pass the same function registered with AddCallHandler.
func ClientCall[S HandlerState, Req Message, Resp any](c Client, ctx context.Context, id string,
	handler CallFunc[S, Req, Resp], req Req) (Resp, error) {
	raw, err := c.call(ctx, id, req.Kind(), req)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return raw.(Resp), nil
}

// ClientQuery sends a synchronous read-only query to a routine.
// The query name is derived from req.Kind(). The handler parameter is used
// only for type inference of the response type — it is not called.
// Pass the same function registered with AddQueryHandler.
func ClientQuery[S HandlerState, Req Message, Resp any](c Client, ctx context.Context, id string,
	handler QueryFunc[S, Req, Resp], req Req) (Resp, error) {
	raw, err := c.query(ctx, id, req.Kind(), req)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return raw.(Resp), nil
}
