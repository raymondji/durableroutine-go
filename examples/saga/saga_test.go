package saga_test

import (
	"testing"
)

func TestSagaHappyPath(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: book flight → book hotel → book car → result has all confirmations
}

func TestSagaHotelFailure(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: book flight → hotel fails after retries →
	// CompensateHotel cancels flight → stateroutine fails with compensation error
}

func TestSagaCarFailure(t *testing.T) {
	t.Skip("requires memoryimpl")
	// Scenario: book flight → book hotel → car fails after retries →
	// CompensateCar cancels hotel and flight → stateroutine fails with compensation error
}
