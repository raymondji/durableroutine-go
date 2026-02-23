package durable

import "context"

// SendMessage sends a message to a named channel on another process. It can
// be called from within any handler to communicate with other running
// processes — like sending a value on a Go channel.
//
// Under the hood this signals the target process's Temporal workflow.
func SendMessage(ctx context.Context, processID string, channel string, msg any) error {
	panic("not implemented")
}

// ProcessID returns the current process's ID from the handler context.
// Use this to tell other processes (e.g. children you spawn) where to send
// messages back — like passing a channel to a goroutine.
func ProcessID(ctx context.Context) string {
	panic("not implemented")
}
