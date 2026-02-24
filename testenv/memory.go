package testenv

import (
	"fmt"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/memoryimpl"
	"github.com/raymondji/stateroutine/stateroutine"
)

// SetupMemory creates an in-memory runtime, registers handlers, and returns
// an Env ready for testing.
func SetupMemory(t *testing.T, registerFn func(w *stateroutine.Worker)) *Env {
	t.Helper()

	w := stateroutine.NewWorker("test")
	registerFn(w)
	rt := memoryimpl.NewRuntime(w)

	return &Env{
		Client: rt.Client(),
		UniqueID: func(prefix string) string {
			return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
		},
	}
}
