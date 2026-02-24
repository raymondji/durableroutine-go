package order_test

import (
	"testing"
)

func TestOrderHappyPath(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: create → place → ship (via timer) → result is "shipped"
}

func TestOrderCancel(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: create → place → cancel → result is "cancelled"
}

func TestOrderExpire(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: create → advance time 30min → result is "timed_out"
}
