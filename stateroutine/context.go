package stateroutine

import "context"

// Context provides durable routine capabilities to handler functions.
type Context struct {
	context.Context

	routineID     string
	spawnRequests []spawnRequest
	queryResults  []queryEntry
}

type spawnRequest struct {
	routineID string
	state     HandlerState
}

// queryEntry stores a static query result keyed by the response's Kind().
type queryEntry struct {
	queryName string
	result    any
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

// Send sends a fire-and-forget message to another routine's inbox.
// The inbox name is derived from msg.Kind(). It can be called from within
// any handler to communicate with other running routines.
// Maps to a Temporal Signal.
func Send[M Message](ctx *Context, routineID string, msg M) error {
	panic("not implemented")
}

// SetQueryResult stores a static query result that persists across state
// transitions until overridden by another call to SetQueryResult with the
// same Resp type. The result is keyed by resp.Kind(). Clients retrieve the
// value via ClientQuery. Takes effect after the current handler returns its
// Suspend. Maps to a Temporal Query handler that returns the stored value.
func SetQueryResult[Resp Message](ctx *Context, resp Resp) {
	qn := resp.Kind()

	// Replace existing result for the same query name, or append.
	for i, e := range ctx.queryResults {
		if e.queryName == qn {
			ctx.queryResults[i] = queryEntry{
				queryName: qn,
				result:    resp,
			}
			return
		}
	}
	ctx.queryResults = append(ctx.queryResults, queryEntry{
		queryName: qn,
		result:    resp,
	})
}
