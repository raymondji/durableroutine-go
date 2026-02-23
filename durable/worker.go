package durable

// Worker registers routine definitions and polls Temporal for work.
type Worker interface {
	// Start begins polling. It blocks until Stop is called or an error occurs.
	Start() error

	// Stop gracefully shuts down the worker.
	Stop()
}

// NewWorker creates a Worker that polls the given task queue using the
// provided routine handler registry.
// TODO: accept Temporal connection options.
func NewWorker(taskQueue string, workers *Workers) Worker {
	panic("not implemented")
}
