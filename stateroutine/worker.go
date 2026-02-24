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

// TaskQueue returns the task queue name.
func (w *Worker) TaskQueue() string {
	return w.taskQueue
}

// Handlers returns the handler registry.
func (w *Worker) Handlers() map[string]HandlerEntry {
	out := make(map[string]HandlerEntry, len(w.handlers))
	for k, e := range w.handlers {
		out[k] = HandlerEntry{Handler: e.handler, Options: e.options}
	}
	return out
}

// HandlerEntry is the exported view of a registered handler with its options.
type HandlerEntry struct {
	Handler any
	Options HandlerOptions
}
