package memoryimpl_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/auction"
	"github.com/raymondji/stateroutine/docs/howto/batch"
	"github.com/raymondji/stateroutine/docs/howto/booking"
	"github.com/raymondji/stateroutine/docs/howto/fanout"
	"github.com/raymondji/stateroutine/docs/howto/order"
	"github.com/raymondji/stateroutine/docs/howto/pipeline"
	"github.com/raymondji/stateroutine/docs/howto/reminder"
	"github.com/raymondji/stateroutine/docs/howto/saga"
	"github.com/raymondji/stateroutine/memoryimpl"
	"github.com/raymondji/stateroutine/stateroutine"
)

// ─── Reminder ───

func TestReminderFullSequence(t *testing.T) {
	svc := &reminder.ReminderService{
		InitialDelay:  1 * time.Hour,
		FollowUpDelay: 24 * time.Hour,
	}
	w := stateroutine.NewWorker("test")
	reminder.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()

	h, err := stateroutine.Start(c, context.Background(), "reminder-1", svc.SendInitial, reminder.InitialState{Email: "test@example.com"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// After Start, the initial handler ran and returned After(1h, SendFollowUp).
	if rt.Step() {
		t.Fatal("expected no progress before advancing time")
	}

	rt.AdvanceTime(1 * time.Hour)

	if rt.Step() {
		t.Fatal("expected no progress before advancing time for follow-up")
	}

	rt.AdvanceTime(24 * time.Hour)

	result, err := h.Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	_ = result
}

// ─── Order ───

func TestOrderHappyPath(t *testing.T) {
	svc := &order.OrderService{
		ShipTimeout: 1 * time.Hour,
	}
	w := stateroutine.NewWorker("test")
	order.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "order-happy", svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Send PlaceOrder.
	err = stateroutine.ClientSend(c, ctx, "order-happy", svc.PlaceOrder, order.PlaceOrderReq{
		OrderID: "ORD-1", Items: []string{"widget"}, PaymentMethod: "card", Total: 99.99,
	})
	if err != nil {
		t.Fatalf("ClientSend PlaceOrder failed: %v", err)
	}

	// Step processes the buffered signal → PlaceOrder handler runs.
	rt.StepAll()

	// Advance time to fire ShipOrder timer.
	rt.AdvanceTime(1 * time.Hour)

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "shipped" {
		t.Fatalf("expected status 'shipped', got %q", result.Status)
	}
}

func TestOrderCancel(t *testing.T) {
	svc := &order.OrderService{}
	w := stateroutine.NewWorker("test")
	order.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "order-cancel", svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Place the order first.
	err = stateroutine.ClientSend(c, ctx, "order-cancel", svc.PlaceOrder, order.PlaceOrderReq{
		OrderID: "ORD-2", Items: []string{"gadget"}, PaymentMethod: "card", Total: 49.99,
	})
	if err != nil {
		t.Fatalf("ClientSend PlaceOrder failed: %v", err)
	}
	rt.StepAll()

	// Now cancel.
	err = stateroutine.ClientSend(c, ctx, "order-cancel", svc.CancelOrder, order.CancelOrderReq{Reason: "changed mind"})
	if err != nil {
		t.Fatalf("ClientSend CancelOrder failed: %v", err)
	}
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "cancelled" {
		t.Fatalf("expected status 'cancelled', got %q", result.Status)
	}
}

func TestOrderExpire(t *testing.T) {
	svc := &order.OrderService{
		ExpireTimeout: 30 * time.Minute,
	}
	w := stateroutine.NewWorker("test")
	order.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "order-expire", svc.CreateOrder, order.OrderState{})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Advance past expire timeout without sending any message.
	rt.AdvanceTime(30 * time.Minute)

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "timed_out" {
		t.Fatalf("expected status 'timed_out', got %q", result.Status)
	}
}

// ─── Auction ───

func TestAuctionBidAcceptReject(t *testing.T) {
	svc := &auction.AuctionService{}
	w := stateroutine.NewWorker("test")
	auction.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "auction-bids", svc.OpenAuction, auction.AuctionState{
		ItemName:    "Vintage Watch",
		StartingBid: 100.0,
		Duration:    30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Bid $150 — should be accepted.
	resp, err := stateroutine.ClientCall(c, ctx, "auction-bids", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "alice", Amount: 150.0,
	})
	if err != nil {
		t.Fatalf("PlaceBid $150 failed: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("expected bid $150 accepted, got: %+v", resp)
	}

	// Bid $120 — should be rejected.
	resp, err = stateroutine.ClientCall(c, ctx, "auction-bids", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "bob", Amount: 120.0,
	})
	if err != nil {
		t.Fatalf("PlaceBid $120 failed: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("expected bid $120 rejected, got: %+v", resp)
	}

	// Bid $200 — should be accepted.
	resp, err = stateroutine.ClientCall(c, ctx, "auction-bids", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "bob", Amount: 200.0,
	})
	if err != nil {
		t.Fatalf("PlaceBid $200 failed: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("expected bid $200 accepted, got: %+v", resp)
	}

	// Advance time to close the auction.
	rt.AdvanceTime(30 * time.Second)

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Winner != "bob" {
		t.Fatalf("expected winner 'bob', got %q", result.Winner)
	}
	if result.Amount != 200.0 {
		t.Fatalf("expected amount 200, got %v", result.Amount)
	}
	if result.BidCount != 2 {
		t.Fatalf("expected 2 accepted bids, got %d", result.BidCount)
	}
}

func TestAuctionNoBids(t *testing.T) {
	svc := &auction.AuctionService{}
	w := stateroutine.NewWorker("test")
	auction.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "auction-nobids", svc.OpenAuction, auction.AuctionState{
		ItemName:    "Empty Auction",
		StartingBid: 50.0,
		Duration:    1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rt.AdvanceTime(1 * time.Minute)

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Winner != "" {
		t.Fatalf("expected no winner, got %q", result.Winner)
	}
	if result.BidCount != 0 {
		t.Fatalf("expected 0 bids, got %d", result.BidCount)
	}
}

// ─── Saga ───

func TestSagaHappyPath(t *testing.T) {
	svc := &saga.TripService{}
	w := stateroutine.NewWorker("test")
	saga.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "saga-happy", svc.BookFlight, saga.TripState{
		TripID:      "TRIP-1",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Continue chains fire during Start, so the saga completes inline.
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.FlightConfirmation == "" {
		t.Fatal("expected flight confirmation")
	}
	if result.HotelConfirmation == "" {
		t.Fatal("expected hotel confirmation")
	}
	if result.CarConfirmation == "" {
		t.Fatal("expected car confirmation")
	}
}

func TestSagaHotelFailure(t *testing.T) {
	svc := &saga.TripService{
		BookHotelFn: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("hotel fully booked")
		},
	}
	w := stateroutine.NewWorker("test")
	saga.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "saga-hotel-fail", svc.BookFlight, saga.TripState{
		TripID:      "TRIP-2",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rt.StepAll()

	_, err = h.Get(ctx)
	if err == nil {
		t.Fatal("expected error from saga compensation")
	}
	if !strings.Contains(err.Error(), "compensated flight") {
		t.Fatalf("expected compensation error, got: %v", err)
	}
}

func TestSagaCarFailure(t *testing.T) {
	svc := &saga.TripService{
		BookCarFn: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("no cars available")
		},
	}
	w := stateroutine.NewWorker("test")
	saga.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "saga-car-fail", svc.BookFlight, saga.TripState{
		TripID:      "TRIP-3",
		FlightID:    "FL-100",
		HotelID:     "HT-200",
		CarRentalID: "CR-300",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rt.StepAll()

	_, err = h.Get(ctx)
	if err == nil {
		t.Fatal("expected error from saga compensation")
	}
	if !strings.Contains(err.Error(), "compensated hotel and flight") {
		t.Fatalf("expected compensation error, got: %v", err)
	}
}

// ─── Batch ───

func makeItems(n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	return items
}

func TestBatchCompleteAll(t *testing.T) {
	svc := &batch.BatchService{}
	w := stateroutine.NewWorker("test")
	batch.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "batch-all", svc.StartBatch, batch.BatchState{
		Items: makeItems(350),
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Default cases drive chunk processing to completion.
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Processed != 350 {
		t.Fatalf("expected 350 processed, got %d", result.Processed)
	}
	if result.Cancelled {
		t.Fatal("expected not cancelled")
	}
}

func TestBatchCancelMidBatch(t *testing.T) {
	svc := &batch.BatchService{}
	w := stateroutine.NewWorker("test")
	batch.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	id := "batch-cancel"
	h, err := stateroutine.Start(c, ctx, id, svc.StartBatch, batch.BatchState{
		Items: makeItems(10000),
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Let a few chunks process via Default.
	rt.Step() // processes chunk 100-200
	rt.Step() // processes chunk 200-300

	// Send cancel signal.
	err = stateroutine.ClientSend(c, ctx, id, svc.CancelBatch, batch.CancelMsg{Reason: "test cancel"})
	if err != nil {
		t.Fatalf("ClientSend CancelMsg failed: %v", err)
	}

	// Next step picks up OnSend(cancel) instead of Default.
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Cancelled {
		t.Fatal("expected cancelled=true")
	}
	if result.Processed < 100 {
		t.Fatalf("expected at least 100 items processed, got %d", result.Processed)
	}
	if result.Processed >= 10000 {
		t.Fatal("expected cancel to stop processing before completion")
	}
}

// ─── Booking ───

func TestBookingHappyPath(t *testing.T) {
	svc := &booking.BookingService{}
	w := stateroutine.NewWorker("test")
	booking.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	id := "booking-happy"
	h, err := stateroutine.Start(c, ctx, id, svc.ReserveItem, booking.BookingState{
		UserID: "user-1", ItemID: "item-1",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Send payment.
	err = stateroutine.ClientSend(c, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
		CardNumber: "4111111111111111", Expiry: "12/26",
	})
	if err != nil {
		t.Fatalf("ClientSend PaymentInfo failed: %v", err)
	}
	rt.StepAll()

	// Send shipping.
	err = stateroutine.ClientSend(c, ctx, id, svc.ProcessShipping, booking.ShippingInfo{
		Address: "123 Main St", City: "Springfield", Zip: "62701",
	})
	if err != nil {
		t.Fatalf("ClientSend ShippingInfo failed: %v", err)
	}
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "shipped" {
		t.Fatalf("expected status 'shipped', got %q", result.Status)
	}
}

func TestBookingCancel(t *testing.T) {
	svc := &booking.BookingService{}
	w := stateroutine.NewWorker("test")
	booking.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	id := "booking-cancel"
	h, err := stateroutine.Start(c, ctx, id, svc.ReserveItem, booking.BookingState{
		UserID: "user-1", ItemID: "item-1",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	resp, err := stateroutine.ClientCall(c, ctx, id, svc.CancelBooking, booking.CancelReq{Reason: "changed mind"})
	if err != nil {
		t.Fatalf("ClientCall CancelBooking failed: %v", err)
	}
	if !resp.Confirmed {
		t.Fatal("expected cancel confirmed")
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "cancelled" {
		t.Fatalf("expected status 'cancelled', got %q", result.Status)
	}
}

func TestBookingExpireReservation(t *testing.T) {
	svc := &booking.BookingService{
		ReservationTimeout: 15 * time.Minute,
	}
	w := stateroutine.NewWorker("test")
	booking.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "booking-expire", svc.ReserveItem, booking.BookingState{
		UserID: "user-1", ItemID: "item-1",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rt.AdvanceTime(15 * time.Minute)

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "expired" {
		t.Fatalf("expected status 'expired', got %q", result.Status)
	}
}

func TestBookingPaymentFailure(t *testing.T) {
	svc := &booking.BookingService{
		ChargeCardFn: func(cardNumber string) error {
			return errors.New("payment gateway unavailable")
		},
	}
	w := stateroutine.NewWorker("test")
	booking.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	id := "booking-pay-fail"
	h, err := stateroutine.Start(c, ctx, id, svc.ReserveItem, booking.BookingState{
		UserID: "user-1", ItemID: "item-1",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err = stateroutine.ClientSend(c, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
		CardNumber: "4111111111111111", Expiry: "12/26",
	})
	if err != nil {
		t.Fatalf("ClientSend PaymentInfo failed: %v", err)
	}
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Status != "payment_failed" {
		t.Fatalf("expected status 'payment_failed', got %q", result.Status)
	}
}

func TestBookingQueryStatus(t *testing.T) {
	svc := &booking.BookingService{}
	w := stateroutine.NewWorker("test")
	booking.RegisterHandlers(w, svc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	id := "booking-query"
	_, err := stateroutine.Start(c, ctx, id, svc.ReserveItem, booking.BookingState{
		UserID: "user-1", ItemID: "item-1",
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Query should return "reserved" immediately after Start.
	status, err := stateroutine.ClientQuery(c, ctx, id, booking.StatusResp{})
	if err != nil {
		t.Fatalf("ClientQuery failed: %v", err)
	}
	if status.Status != "reserved" {
		t.Fatalf("expected status 'reserved', got %q", status.Status)
	}

	// Send payment.
	err = stateroutine.ClientSend(c, ctx, id, svc.ProcessPayment, booking.PaymentInfo{
		CardNumber: "4111111111111111", Expiry: "12/26",
	})
	if err != nil {
		t.Fatalf("ClientSend PaymentInfo failed: %v", err)
	}
	rt.StepAll()

	// Query should now return "paid".
	status, err = stateroutine.ClientQuery(c, ctx, id, booking.StatusResp{})
	if err != nil {
		t.Fatalf("ClientQuery after payment failed: %v", err)
	}
	if status.Status != "paid" {
		t.Fatalf("expected status 'paid', got %q", status.Status)
	}

	// Complete with shipping.
	err = stateroutine.ClientSend(c, ctx, id, svc.ProcessShipping, booking.ShippingInfo{
		Address: "123 Main St", City: "Springfield", Zip: "62701",
	})
	if err != nil {
		t.Fatalf("ClientSend ShippingInfo failed: %v", err)
	}
	rt.StepAll()

	result, err := stateroutine.ClientGet[booking.BookingResult](c, ctx, id)
	if err != nil {
		t.Fatalf("ClientGet failed: %v", err)
	}
	if result.Status != "shipped" {
		t.Fatalf("expected status 'shipped', got %q", result.Status)
	}
}

// ─── Pipeline ───

func TestPipelineFullSequence(t *testing.T) {
	producerSvc := &pipeline.ProducerService{}
	consumerSvc := &pipeline.ConsumerService{}
	w := stateroutine.NewWorker("test")
	pipeline.RegisterHandlers(w, producerSvc, consumerSvc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	consumerID := "consumer-1"

	// Start consumer — waits for items.
	_, err := stateroutine.Start(c, ctx, consumerID, consumerSvc.StartConsumer, pipeline.ConsumerState{
		Name: "test-consumer",
	})
	if err != nil {
		t.Fatalf("Start consumer failed: %v", err)
	}

	// Start producer — sends items to consumer via BufferSend, completes inline.
	producerH, err := stateroutine.Start(c, ctx, "producer-1", producerSvc.Produce, pipeline.ProducerState{
		Items:                  []string{"one", "two", "three", "four"},
		ConsumerStateroutineID: consumerID,
	})
	if err != nil {
		t.Fatalf("Start producer failed: %v", err)
	}

	// Producer already done.
	_, err = producerH.Get(ctx)
	if err != nil {
		t.Fatalf("Producer Get failed: %v", err)
	}

	// Drive consumer to process all buffered signals.
	rt.StepAll()

	consumerResult, err := stateroutine.ClientGet[pipeline.ConsumerResult](c, ctx, consumerID)
	if err != nil {
		t.Fatalf("Consumer Get failed: %v", err)
	}
	if len(consumerResult.Received) != 4 {
		t.Fatalf("expected 4 items received, got %d", len(consumerResult.Received))
	}
}

// ─── Fanout ───

func TestFanoutCollectAllResults(t *testing.T) {
	fanoutSvc := &fanout.FanoutService{}
	itemSvc := &fanout.ItemService{}
	w := stateroutine.NewWorker("test")
	fanout.RegisterHandlers(w, fanoutSvc, itemSvc)

	rt := memoryimpl.NewRuntime(w)
	c := rt.Client()
	ctx := context.Background()

	h, err := stateroutine.Start(c, ctx, "fanout-1", fanoutSvc.StartItems, fanout.FanoutState{
		Items: []struct {
			ID   string
			Data string
		}{
			{ID: "a", Data: "alpha"},
			{ID: "b", Data: "beta"},
			{ID: "c", Data: "gamma"},
		},
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// BufferStart + BufferSend during Start already buffered signals.
	// StepAll drives the parent to collect all results.
	rt.StepAll()

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(result.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.Results))
	}
}
