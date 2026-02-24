package testenv

import (
	"fmt"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/backend/inmemory"
	"github.com/raymondji/durableroutine-go/durable"
)

// SetupMemory creates an in-memory runtime, registers handlers, and returns
// an Env ready for testing.
func SetupMemory(t *testing.T, registerFn func(w *durable.Worker)) *Env {
	t.Helper()

	w := durable.NewWorker("test")
	registerFn(w)
	rt := inmemory.NewRuntime(w)

	return &Env{
		Client: rt.Client(),
		UniqueID: func(prefix string) string {
			return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
		},
	}
}
