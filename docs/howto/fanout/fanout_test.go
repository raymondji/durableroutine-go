package fanout_test

import (
	"testing"
)

func TestFanoutCollectAllResults(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: spawn 3 children → each sends result back → parent collects all 3
}
