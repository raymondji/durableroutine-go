package testenv

import (
	"fmt"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/temporalimpl"
)

// SetupTemporal creates a Temporal client, unique task queue, registers handlers via
// registerFn, starts a worker, and returns an Env ready for testing.
func SetupTemporal(t *testing.T, registerFn func(w *stateroutine.Worker)) *Env {
	t.Helper()

	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		t.Fatalf("failed to connect to Temporal: %v", err)
	}
	t.Cleanup(func() { tc.Close() })

	taskQueue := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())

	w := stateroutine.NewWorker(taskQueue)
	registerFn(w)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			t.Logf("worker start error: %v", err)
		}
	}()
	t.Cleanup(func() { tw.Stop() })

	// Give worker time to start polling.
	time.Sleep(500 * time.Millisecond)

	client := temporalimpl.NewClient(tc, taskQueue)
	srClient := stateroutine.NewClientFrom(client)

	return &Env{
		Client: srClient,
		UniqueID: func(prefix string) string {
			return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
		},
	}
}
