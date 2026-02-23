package durable

import "context"

// Context provides durable routine capabilities to handler functions.
type Context struct {
	context.Context

	routineID     string
	spawnRequests []spawnRequest
	queryHandlers []queryEntry
}

type spawnRequest struct {
	routineID string
	state     HandlerState
}

// queryEntry is a type-erased query handler registration.
type queryEntry struct {
	queryName  string
	handler    any
	state      any
	handlerKey string
}

// RoutineID returns the current routine's unique identifier.
func (c *Context) RoutineID() string {
	return c.routineID
}

// Spawn requests that a child routine be started when the current handler
// completes. The child runs as an independent durable routine (Temporal child
// workflow). The state.Kind() determines which registered handler runs.
func (c *Context) Spawn(routineID string, state HandlerState) {
	c.spawnRequests = append(c.spawnRequests, spawnRequest{
		routineID: routineID,
		state:     state,
	})
}

// Cast sends a fire-and-forget message to another routine's inbox.
// The inbox name is derived from msg.Kind(). It can be called from within
// any handler to communicate with other running routines.
// Maps to a Temporal Signal.
func Cast[M Message](ctx *Context, routineID string, msg M) error {
	panic("not implemented")
}

// SetQueryHandler registers a query handler that persists across state
// transitions until overridden by another call to SetQueryHandler with the
// same Req type. Query handlers run concurrently with the routine (they are
// read-only and do not advance the state machine). The handler takes effect
// after the current handler returns its Suspend.
// Maps to a Temporal Query handler.
func SetQueryHandler[S HandlerState, Req Message, Resp any](ctx *Context, handler QueryFunc[S, Req, Resp], state S) {
	var zeroReq Req
	qn := zeroReq.Kind()

	// Replace existing handler for the same query name, or append.
	for i, e := range ctx.queryHandlers {
		if e.queryName == qn {
			ctx.queryHandlers[i] = queryEntry{
				queryName:  qn,
				handler:    handler,
				state:      state,
				handlerKey: "query:" + state.Kind() + ":" + qn,
			}
			return
		}
	}
	ctx.queryHandlers = append(ctx.queryHandlers, queryEntry{
		queryName:  qn,
		handler:    handler,
		state:      state,
		handlerKey: "query:" + state.Kind() + ":" + qn,
	})
}
