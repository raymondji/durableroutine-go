package stateroutine

// Worker registers routine definitions and polls Temporal for work.
type Worker struct {
	taskQueue string
	handlers  map[string]handlerEntry
}

// NewWorker creates a Worker that polls the given task queue.
// Register handlers using the Add* package-level functions before calling Start.
// TODO: accept Temporal connection options.
func NewWorker(taskQueue string) *Worker {
	return &Worker{
		taskQueue: taskQueue,
		handlers:  make(map[string]handlerEntry),
	}
}

// Start begins polling. It blocks until Stop is called or an error occurs.
func (w *Worker) Start() error {
	panic("not implemented")
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() {
	panic("not implemented")
}
