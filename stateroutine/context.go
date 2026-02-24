package stateroutine

import "context"

// Context provides durable stateroutine capabilities to handler functions.
type Context struct {
	context.Context

	stateroutineID string
	startRequests  []startRequest
	sendRequests   []sendRequest
	queryResults   []queryEntry
}

type sendRequest struct {
	stateroutineID string
	stateKind      string
	msgKind        string
	msg            any
}

type startRequest struct {
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

// BufferStart requests that a child stateroutine be started when the current handler
// completes. The child runs as an independent durable stateroutine (Temporal child
// workflow). The state.Kind() determines which registered handler runs.
func (c *Context) BufferStart(stateroutineID string, state HandlerState) {
	c.startRequests = append(c.startRequests, startRequest{
		stateroutineID: stateroutineID,
		state:          state,
	})
}

// BufferSend buffers a fire-and-forget message to another stateroutine's inbox.
// The message is delivered by the runtime after the current handler returns its
// Suspend value, not immediately. The handler parameter is used only for type
// inference of the target state type — it is not called. Pass the same function
// registered with AddSendHandler, or use a nil stub for type inference when
// the sender doesn't have access to the receiver's handler function.
// Maps to a Temporal Signal.
func BufferSend[S HandlerState, M Message, T any](ctx *Context, stateroutineID string,
	handler SendFunc[S, M, T], msg M) {
	var zeroS S
	ctx.sendRequests = append(ctx.sendRequests, sendRequest{
		stateroutineID: stateroutineID,
		stateKind:      zeroS.Kind(),
		msgKind:        msg.Kind(),
		msg:            msg,
	})
}

// NewContext creates a new stateroutine Context. Exported for use by
// implementation packages (temporalimpl).
func NewContext(ctx context.Context, stateroutineID string) *Context {
	return &Context{Context: ctx, stateroutineID: stateroutineID}
}

// QueryResults returns the accumulated query results.
func (c *Context) QueryResults() []QueryEntry {
	out := make([]QueryEntry, len(c.queryResults))
	for i, e := range c.queryResults {
		out[i] = QueryEntry{QueryName: e.queryName, Result: e.result}
	}
	return out
}

// StartRequests returns the accumulated start requests.
func (c *Context) StartRequests() []StartEntry {
	out := make([]StartEntry, len(c.startRequests))
	for i, e := range c.startRequests {
		out[i] = StartEntry{StateroutineID: e.stateroutineID, StateKind: e.state.Kind(), State: e.state}
	}
	return out
}

// SendRequests returns the accumulated send requests.
func (c *Context) SendRequests() []SendEntry {
	out := make([]SendEntry, len(c.sendRequests))
	for i, e := range c.sendRequests {
		out[i] = SendEntry{
			StateroutineID: e.stateroutineID,
			StateKind:      e.stateKind,
			MsgKind:        e.msgKind,
			Msg:            e.msg,
		}
	}
	return out
}

// QueryEntry is the exported view of a query result.
type QueryEntry struct {
	QueryName string
	Result    any
}

// StartEntry is the exported view of a start request.
type StartEntry struct {
	StateroutineID string
	StateKind      string
	State          any
}

// SendEntry is the exported view of a send request.
type SendEntry struct {
	StateroutineID string
	StateKind      string
	MsgKind        string
	Msg            any
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
