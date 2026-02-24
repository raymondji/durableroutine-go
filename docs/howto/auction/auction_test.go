package auction_test

import (
	"testing"
)

func TestAuctionBidAcceptReject(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: open → bid $150 (accept) → bid $120 (reject) → bid $200 (accept) →
	// close via timer → winner is bob at $200
}

func TestAuctionNoBids(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: open → advance time past duration → close with no winner
}

func TestAuctionBidTerminalError(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: open → bid fails after all retries → BidFailed returns error
	// response to caller → auction continues accepting bids
}
