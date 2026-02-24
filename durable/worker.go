package durable

// Worker registers routine definitions and polls Temporal for work.
type Worker struct {
	taskQueue string
	handlers  map[string]handlerEntry
}

// NewWorker creates a Worker that polls the given task queue.
// Register handlers using the Register* package-level functions before starting.
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
		out[k] = HandlerEntry{Handler: e.handler, Options: e.options, Runner: e.runner}
	}
	return out
}

// HandlerEntry is the exported view of a registered handler with its options.
type HandlerEntry struct {
	Handler any
	Options HandlerOptions
	Runner  HandlerRunner
}
