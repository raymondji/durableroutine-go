package durable

import "context"

// Context provides durable routine capabilities to handler functions.
type Context struct {
	context.Context

	routineID     string
	spawnRequests []spawnRequest
}

type spawnRequest struct {
	routineID string
	args      RoutineArgs
}

// RoutineID returns the current routine's unique identifier.
func (c *Context) RoutineID() string {
	return c.routineID
}

// Spawn requests that a child routine be started when the current handler
// completes. The child runs as an independent durable routine (Temporal child
// workflow). The args.Kind() determines which registered handler runs.
func (c *Context) Spawn(routineID string, args RoutineArgs) {
	c.spawnRequests = append(c.spawnRequests, spawnRequest{
		routineID: routineID,
		args:      args,
	})
}

// Cast sends a fire-and-forget message to a named inbox on another routine.
// It can be called from within any handler to communicate with other running
// routines. Maps to a Temporal Signal.
func Cast[M any](ctx *Context, routineID string, inbox Inbox[M], msg M) error {
	panic("not implemented")
}
