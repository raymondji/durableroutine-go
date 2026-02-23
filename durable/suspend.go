package durable

import "time"

// Suspend describes what a process should wait for before invoking the next
// handler. It is an opaque value built via the After and Select constructors.
// Optionally, it can also carry child process spawns via WithSpawns.
type Suspend[S any] struct {
	cases       []Case[S]
	childSpawns []Spawn
}

// Case is a single wait condition inside a Suspend (a timer or a channel
// receive) paired with the handler to invoke when that condition fires.
type Case[S any] struct {
	// Exactly one of the following is set.
	timerDuration *time.Duration
	channelName   string

	// handler is called when this case fires.
	// It is stored as an any because message-receive cases have a different
	// signature (MessageHandlerFunc) that is type-erased at this level.
	handler any
}

// Spawn describes a child process to start.
type Spawn struct {
	// ProcessID is the unique identifier for this child process instance.
	ProcessID string
	// Process is the process definition (a Process[C] for some state type C).
	Process any
	// Args is passed to the child's InitState function.
	Args any
}

// After builds a Suspend that waits for the given duration and then calls
// handler. This is the simple "sleep then continue" primitive.
func After[S any](d time.Duration, handler HandlerFunc[S]) *Suspend[S] {
	return &Suspend[S]{
		cases: []Case[S]{AfterFunc[S](d, handler)},
	}
}

// Select builds a Suspend that waits for the first of several cases to fire,
// similar to Go's select statement.
func Select[S any](cases ...Case[S]) *Suspend[S] {
	return &Suspend[S]{cases: cases}
}

// AfterFunc returns a Case that fires after the given duration.
// Use this inside a Select when you want a timer alongside channel receives.
func AfterFunc[S any](d time.Duration, handler HandlerFunc[S]) Case[S] {
	return Case[S]{
		timerDuration: &d,
		handler:       handler,
	}
}

// Receive returns a Case that fires when a message arrives on the named
// channel. The message is deserialised into type M before the handler is called.
func Receive[S any, M any](channel string, handler MessageHandlerFunc[S, M]) Case[S] {
	return Case[S]{
		channelName: channel,
		handler:     handler,
	}
}

// WithSpawns attaches child process spawns to a Suspend. The runtime starts
// all children when this Suspend is entered, then waits for the Suspend's
// cases as usual. Children communicate back to the parent (or to any other
// process) by calling durable.SendMessage from their handlers.
func (s *Suspend[S]) WithSpawns(spawns ...Spawn) *Suspend[S] {
	s.childSpawns = append(s.childSpawns, spawns...)
	return s
}
