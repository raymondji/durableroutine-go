package temporalimpl

import (
	"reflect"

	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/raymondji/stateroutine/stateroutine"
)

// Worker wraps a Temporal worker with the stateroutine handler registry.
type Worker struct {
	inner worker.Worker
}

// NewWorker creates a Temporal worker from a stateroutine.Worker definition.
func NewWorker(tc temporalclient.Client, w *stateroutine.Worker) *Worker {
	taskQueue := w.TaskQueue()
	tw := worker.New(tc, taskQueue, worker.Options{})

	// Build the handler registry.
	reg := &registry{entries: make(map[string]registryEntry)}
	for key, entry := range w.Handlers() {
		re := registryEntry{
			handler:   entry.Handler,
			options:   entry.Options,
			stateType: extractStateType(entry.Handler),
			msgType:   extractMsgType(entry.Handler),
		}
		reg.entries[key] = re
	}
	globalRegistry = reg

	// Register the workflow.
	tw.RegisterWorkflowWithOptions(StateroutineWorkflow, workflow.RegisterOptions{
		Name: "StateroutineWorkflow",
	})

	// Register the activity.
	tw.RegisterActivity(RunHandler)

	return &Worker{inner: tw}
}

// Start begins polling for work. Blocks until Stop is called.
func (w *Worker) Start() error {
	return w.inner.Start()
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() {
	w.inner.Stop()
}

// extractStateType returns the concrete State type from a handler function.
// Handler functions have signatures like func(*Context, State) or func(*Context, State, Msg).
func extractStateType(handler any) reflect.Type {
	ft := reflect.TypeOf(handler)
	// Parameter 0 is *Context, parameter 1 is State.
	if ft.NumIn() < 2 {
		return nil
	}
	return ft.In(1)
}

// extractMsgType returns the concrete Message type from a handler function,
// or nil if the handler doesn't take a message (HandlerFunc/TerminalErrorFunc).
func extractMsgType(handler any) reflect.Type {
	ft := reflect.TypeOf(handler)
	if ft.NumIn() < 3 {
		return nil
	}
	// Parameter 2 is either Msg or error (for terminal error handlers).
	paramType := ft.In(2)
	errInterface := reflect.TypeOf((*error)(nil)).Elem()
	if paramType.Implements(errInterface) {
		return nil
	}
	return paramType
}
