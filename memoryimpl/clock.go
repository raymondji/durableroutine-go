package memoryimpl

import "time"

// Clock is a controllable clock for deterministic testing.
type Clock struct {
	now time.Time
}

// NewClock creates a Clock starting at 2000-01-01T00:00:00Z.
func NewClock() *Clock {
	return &Clock{now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the current time.
func (c *Clock) Now() time.Time {
	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}
