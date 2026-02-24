package booking_test

import (
	"testing"
)

// Tests require the memoryimpl package (in-memory runtime) which does not exist yet.
// Each test is skipped until that implementation is available.

func TestBookingHappyPath(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: reserve → pay → ship → result is "shipped"
}

func TestBookingCancel(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: reserve → cancel via ClientCall → result is "cancelled"
}

func TestBookingExpireReservation(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: reserve → advance time 15min → result is "expired"
}

func TestBookingPaymentFailure(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: reserve → send payment → payment fails after all retries →
	// terminal error handler releases reservation → result is "payment_failed"
}

func TestBookingQueryStatus(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: reserve → query status (should be "reserved") →
	// pay → query status (should be "paid")
}
