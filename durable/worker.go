package durable

// Worker registers process definitions and polls Temporal for work.
type Worker interface {
	// Register adds a process definition to this worker.
	// process should be a *Process[S] for some state type S.
	Register(process any)

	// Start begins polling. It blocks until Stop is called or an error occurs.
	Start() error

	// Stop gracefully shuts down the worker.
	Stop()
}

// NewWorker creates a Worker that polls the given task queue.
// TODO: accept Temporal connection options.
func NewWorker(taskQueue string) Worker {
	panic("not implemented")
}
