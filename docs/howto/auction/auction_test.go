package auction_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/auction"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestAuctionBidAcceptReject(t *testing.T) {
	svc := auction.NewAuctionRegistry()
	stub := auction.NewAuctionServiceStub()
	testenv.RunAll(t, func(w *durable.Worker) {
		svc.RegisterHandlers(w)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		id := env.UniqueID("auction-bids")
		h, err := durable.Go(env.Client, ctx, id, stub.OpenAuction, auction.AuctionInput{
			ItemName:    "Vintage Watch",
			StartingBid: 100.0,
			Duration:    30 * time.Second,
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Bid $150 — should be accepted
		resp, err := durable.Call(env.Client, ctx, id, stub.PlaceBid, auction.PlaceBidReq{
			BidderID: "alice", Amount: 150.0,
		})
		if err != nil {
			t.Fatalf("PlaceBid $150 failed: %v", err)
		}
		if !resp.Accepted {
			t.Fatalf("expected bid $150 accepted, got: %+v", resp)
		}

		time.Sleep(1 * time.Second)

		// Bid $120 — should be rejected (lower than current $150)
		resp, err = durable.Call(env.Client, ctx, id, stub.PlaceBid, auction.PlaceBidReq{
			BidderID: "bob", Amount: 120.0,
		})
		if err != nil {
			t.Fatalf("PlaceBid $120 failed: %v", err)
		}
		if resp.Accepted {
			t.Fatalf("expected bid $120 rejected, got: %+v", resp)
		}

		time.Sleep(1 * time.Second)

		// Bid $200 — should be accepted
		resp, err = durable.Call(env.Client, ctx, id, stub.PlaceBid, auction.PlaceBidReq{
			BidderID: "bob", Amount: 200.0,
		})
		if err != nil {
			t.Fatalf("PlaceBid $200 failed: %v", err)
		}
		if !resp.Accepted {
			t.Fatalf("expected bid $200 accepted, got: %+v", resp)
		}

		// Wait for auction timer to close
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
		t.Logf("Auction result: %+v", result)
	})
}

func TestAuctionNoBids(t *testing.T) {
	svc := auction.NewAuctionRegistry()
	stub := auction.NewAuctionServiceStub()
	testenv.RunAll(t, func(w *durable.Worker) {
		svc.RegisterHandlers(w)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h, err := durable.Go(env.Client, ctx, env.UniqueID("auction-nobids"), stub.OpenAuction, auction.AuctionInput{
			ItemName:    "Empty Auction",
			StartingBid: 50.0,
			Duration:    1 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

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
		t.Logf("Auction no bids: %+v", result)
	})
}

func TestAuctionBidRecoveryHandler(t *testing.T) {
	t.Skip("needs injectable failure in PlaceBid")
}
