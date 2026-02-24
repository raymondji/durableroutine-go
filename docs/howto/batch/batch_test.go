package batch_test

import (
	"testing"
)

func TestBatchCompleteAll(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: start batch of 350 items → process all chunks → result has 350 processed
}

func TestBatchCancelMidBatch(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: start batch of 350 items → after first chunk, send cancel signal →
	// result has 100 processed and Cancelled=true
}
