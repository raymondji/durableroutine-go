// Package demorunner provides shared setup for howto example CLIs.
// It handles --backend flag parsing and backend initialization so each
// example only needs to register handlers and write its demo logic.
package demorunner

import (
	"flag"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/durableroutine-go/backend/inmemory"
	"github.com/raymondji/durableroutine-go/backend/temporal"
	"github.com/raymondji/durableroutine-go/durable"
)

// Run sets up the backend based on the --backend flag (default: "memory"),
// then calls fn with the resulting client. Cleanup is handled automatically.
func Run(w *durable.Worker, fn func(client durable.Client)) {
	backend := flag.String("backend", "memory", "backend to use: memory or temporal")
	flag.Parse()

	switch *backend {
	case "memory":
		rt := inmemory.NewRuntime(w)
		fn(rt.Client())
	case "temporal":
		tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
		if err != nil {
			log.Fatal(err)
		}
		defer tc.Close()

		tw := temporal.NewWorker(tc, w)
		go func() {
			if err := tw.Start(); err != nil {
				log.Fatal(err)
			}
		}()
		defer tw.Stop()

		fn(durable.NewClientFrom(temporal.NewClient(tc, w.TaskQueue())))
	default:
		log.Fatalf("unknown backend: %s", *backend)
	}
}
