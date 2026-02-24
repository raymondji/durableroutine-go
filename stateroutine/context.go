package stateroutine

import "context"

// Context provides durable stateroutine capabilities to handler functions.
type Context struct {
	context.Context

	stateroutineID string
	spawnRequests  []spawnRequest
	sendRequests   []sendRequest
	queryResults   []queryEntry
}

type sendRequest struct {
	stateroutineID string
	stateKind      string
	msgKind        string
	msg            any
}

type spawnRequest struct {
	stateroutineID string
	state          HandlerState
}

// queryEntry stores a static query result keyed by the response's Kind().
type queryEntry struct {
	queryName string
	result    any
}

// StateroutineID returns the current stateroutine's unique identifier.
func (c *Context) StateroutineID() string {
	return c.stateroutineID
}

// Spawn requests that a child stateroutine be started when the current handler
// completes. The child runs as an independent durable stateroutine (Temporal child
// workflow). The state.Kind() determines which registered handler runs.
func (c *Context) Spawn(stateroutineID string, state HandlerState) {
	c.spawnRequests = append(c.spawnRequests, spawnRequest{
		stateroutineID: stateroutineID,
		state:          state,
	})
}

// Send buffers a fire-and-forget message to another stateroutine's inbox.
// The message is delivered by the runtime after the current handler returns its
// Suspend value, not immediately. The handler parameter is used only for type
// inference of the target state type — it is not called. Pass the same function
// registered with AddSendHandler, or pass nil with explicit type parameters when
// the sender doesn't have access to the receiver's handler function.
// Maps to a Temporal Signal.
func Send[S HandlerState, M Message, T any](ctx *Context, stateroutineID string,
	handler SendFunc[S, M, T], msg M) error {
	var zeroS S
	ctx.sendRequests = append(ctx.sendRequests, sendRequest{
		stateroutineID: stateroutineID,
		stateKind:      zeroS.Kind(),
		msgKind:        msg.Kind(),
		msg:            msg,
	})
	return nil
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
