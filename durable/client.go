package durable

import "context"

// Client starts and interacts with durable processes.
type Client interface {
	// Start begins a new instance of the given process. args is passed to the
	// process's InitState function to construct the initial state.
	Start(ctx context.Context, processID string, process any, args any) error

	// SendMessage sends a typed message to a named channel within a running process.
	SendMessage(ctx context.Context, processID string, channel string, msg any) error

	// GetResult blocks until the process completes and unmarshals the final
	// state into result. result must be a pointer to the process's state type.
	GetResult(ctx context.Context, processID string, result any) error
}

// NewClient creates a Client backed by Temporal.
// TODO: accept Temporal connection options.
func NewClient() Client {
	panic("not implemented")
}
