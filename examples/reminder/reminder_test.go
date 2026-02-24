package reminder_test

import (
	"testing"
)

func TestReminderFullSequence(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: send initial → advance 24h → send follow-up →
	// advance 7 days → send final → done
}
