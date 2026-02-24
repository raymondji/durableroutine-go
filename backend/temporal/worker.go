package temporal

import (
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/raymondji/durableroutine-go/durable"
)

// Worker wraps a Temporal worker with the routine handler registry.
type Worker struct {
	inner worker.Worker
}

// NewWorker creates a Temporal worker from a durable.Worker definition.
func NewWorker(tc temporalclient.Client, w *durable.Worker) *Worker {
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

	// Register the workflow and activity using struct-based dependency injection.
	ha := &handlerActivity{reg: reg}
	wh := &workflowHandler{reg: reg}
	tw.RegisterWorkflow(wh.RoutineWorkflow)
	tw.RegisterActivity(ha)

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
