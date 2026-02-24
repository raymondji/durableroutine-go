package temporalimpl

import (
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
		reg.entries[key] = registryEntry{
			runner:  entry.Runner,
			options: entry.Options,
		}
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
