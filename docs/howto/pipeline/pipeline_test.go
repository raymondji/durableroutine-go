package pipeline_test

import (
	"testing"
)

func TestPipelineFullSequence(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: start consumer → start producer with 4 items →
	// producer sends all items + done → consumer receives all → result has 4 items
}
