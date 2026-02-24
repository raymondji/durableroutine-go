// Package testenv provides shared test setup for running howto integration
// tests against multiple backends (Temporal and in-memory).
package testenv

import (
	"testing"

	"github.com/raymondji/stateroutine/stateroutine"
)

// Env is the test environment returned by setup helpers.
type Env struct {
	Client   stateroutine.Client
	UniqueID func(prefix string) string
}

// RunAll runs testFn against both the memory and Temporal backends.
func RunAll(t *testing.T, registerFn func(w *stateroutine.Worker), testFn func(t *testing.T, env *Env)) {
	t.Run("memory", func(t *testing.T) {
		testFn(t, SetupMemory(t, registerFn))
	})
	t.Run("temporal", func(t *testing.T) {
		testFn(t, SetupTemporal(t, registerFn))
	})
}
