package durable

import "context"

// Context provides durable routine capabilities to handler functions.
type Context struct {
	context.Context

	routineID    string
	startRequests []startRequest
	sendRequests  []sendRequest
	queryResults  []queryEntry
}

type sendRequest struct {
	routineID        string
	inputKind        string
	externalInputKind string
	resultKind       string
	msg              any
}

type startRequest struct {
	routineID  string
	state      Payload
	resultKind string
}

// queryEntry stores a static query result keyed by the response's DurableKind().
type queryEntry struct {
	queryName string
	result    any
}

// RoutineID returns the current routine's unique identifier.
func (c *Context) RoutineID() string {
	return c.routineID
}

// BufferStart requests that a child routine be started when the current handler
// completes. The child runs as an independent durable routine (Temporal child
// workflow). The handler parameter is used only for type inference of the result
// type — it is not called. The input.DurableKind() and result DurableKind()
// determine which registered handler runs.
func BufferStart[I Payload, T Payload](ctx *Context, routineID string, handler Handler[I, T], input I) {
	var zeroT T
	ctx.startRequests = append(ctx.startRequests, startRequest{
		routineID:  routineID,
		state:      input,
		resultKind: zeroT.DurableKind(),
	})
}

// BufferSend buffers a fire-and-forget message to another routine's inbox.
// The message is delivered by the runtime after the current handler returns its
// Continuation value, not immediately. The handler parameter is used only for type
// inference of the target input type — it is not called. Pass the same function
// registered with RegisterSendHandler, or use a nil stub for type inference when
// the sender doesn't have access to the receiver's handler function.
// Maps to a Temporal Signal.
func BufferSend[I Payload, E Payload, T Payload](ctx *Context, routineID string,
	handler SendHandler[I, E, T], externalInput E) {
	var zeroI I
	var zeroT T
	ctx.sendRequests = append(ctx.sendRequests, sendRequest{
		routineID:        routineID,
		inputKind:        zeroI.DurableKind(),
		externalInputKind: externalInput.DurableKind(),
		resultKind:       zeroT.DurableKind(),
		msg:              externalInput,
	})
}

// NewContext creates a new routine Context. Exported for use by
// backend implementation packages.
func NewContext(ctx context.Context, routineID string) *Context {
	return &Context{Context: ctx, routineID: routineID}
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
		out[i] = StartEntry{RoutineID: e.routineID, InputKind: e.state.DurableKind(), ResultKind: e.resultKind, Input: e.state}
	}
	return out
}

// SendRequests returns the accumulated send requests.
func (c *Context) SendRequests() []SendEntry {
	out := make([]SendEntry, len(c.sendRequests))
	for i, e := range c.sendRequests {
		out[i] = SendEntry{
			RoutineID:        e.routineID,
			InputKind:        e.inputKind,
			ExternalInputKind: e.externalInputKind,
			ResultKind:       e.resultKind,
			Msg:              e.msg,
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
	RoutineID  string
	InputKind  string
	ResultKind string
	Input      any
}

// SendEntry is the exported view of a send request.
type SendEntry struct {
	RoutineID        string
	InputKind        string
	ExternalInputKind string
	ResultKind       string
	Msg              any
}

// SetQueryResult stores a static query result that persists across state
// transitions until overridden by another call to SetQueryResult with the
// same Resp type. The result is keyed by resp.DurableKind(). Clients retrieve the
// value via Query. Takes effect after the current handler returns its
// Continuation. Maps to a Temporal Query handler that returns the stored value.
func SetQueryResult[Resp Payload](ctx *Context, resp Resp) {
	qn := resp.DurableKind()

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
