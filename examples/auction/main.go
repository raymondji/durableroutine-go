// Command auction demonstrates ClientCall for synchronous request-response.
// Bidders place bids via ClientCall and immediately learn whether their bid
// was accepted or outbid. The auction runs until a timer expires, then
// completes with the winning bid. Current status is available via ClientQuery.
//
// Also demonstrates OnCallTerminalError: if bid processing fails after all
// retries, the terminal error handler returns an error response to the blocked
// caller instead of failing the entire routine.
//
// This shows the key difference between Send and Call:
//   - Send (fire-and-forget): the caller doesn't wait for a response.
//   - Call (request-response): the caller blocks until the handler responds.
//
// Bidders need to know if their bid was accepted, making Call the right choice.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/stateroutine/stateroutine"
)

// --- State ---

type AuctionState struct {
	ItemName    string
	StartingBid float64
	Duration    time.Duration
}

func (AuctionState) Kind() string { return "auction" }

// --- Messages ---

type PlaceBidReq struct {
	BidderID string
	Amount   float64
}

func (PlaceBidReq) Kind() string { return "place-bid" }

type PlaceBidResp struct {
	Accepted   bool
	HighestBid float64
	Message    string
}

type AuctionStatusResp struct {
	ItemName   string
	HighestBid float64
	Leader     string
	BidCount   int
}

func (AuctionStatusResp) Kind() string { return "auction-status" }

// --- Result ---

type AuctionResult struct {
	ItemName string
	Winner   string
	Amount   float64
	BidCount int
}

// --- Per-step state types ---

type BiddingState struct {
	ItemName   string
	HighestBid float64
	Leader     string
	BidCount   int
}

func (BiddingState) Kind() string { return "auction.bidding" }

// --- Service struct ---

type AuctionService struct {
	// Injected dependencies would go here (e.g., notification service).
}

func (s *AuctionService) OpenAuction(ctx *stateroutine.Context, state AuctionState) (*stateroutine.Suspend[AuctionResult], error) {
	fmt.Printf("auction opened for %q, starting bid: $%.2f\n", state.ItemName, state.StartingBid)

	bidding := BiddingState{
		ItemName:   state.ItemName,
		HighestBid: state.StartingBid,
	}

	stateroutine.SetQueryResult(ctx, AuctionStatusResp{
		ItemName:   bidding.ItemName,
		HighestBid: bidding.HighestBid,
	})

	return stateroutine.Select[AuctionResult](
		stateroutine.OnCall(s.PlaceBid, bidding),
		stateroutine.OnTimer(state.Duration, s.CloseAuction, bidding),
	), nil
}

// PlaceBid handles a synchronous bid. The caller blocks until this returns,
// so they immediately know whether their bid was accepted. This is why Call
// is the right primitive here — Send would not give the bidder feedback.
func (s *AuctionService) PlaceBid(ctx *stateroutine.Context, state BiddingState, req PlaceBidReq) (PlaceBidResp, *stateroutine.Suspend[AuctionResult], error) {
	if req.Amount <= state.HighestBid {
		// Reject the bid but don't advance state — stay in the same bidding
		// state waiting for more bids. Return nil Suspend to keep waiting.
		return PlaceBidResp{
			Accepted:   false,
			HighestBid: state.HighestBid,
			Message:    fmt.Sprintf("bid too low, current highest is $%.2f", state.HighestBid),
		}, nil, nil
	}

	fmt.Printf("new high bid: $%.2f by %s (was $%.2f by %s)\n",
		req.Amount, req.BidderID, state.HighestBid, state.Leader)

	state.HighestBid = req.Amount
	state.Leader = req.BidderID
	state.BidCount++

	stateroutine.SetQueryResult(ctx, AuctionStatusResp{
		ItemName:   state.ItemName,
		HighestBid: state.HighestBid,
		Leader:     state.Leader,
		BidCount:   state.BidCount,
	})

	// Accept the bid and continue waiting for more bids or the timer.
	return PlaceBidResp{
		Accepted:   true,
		HighestBid: state.HighestBid,
		Message:    "bid accepted, you are the highest bidder",
	}, stateroutine.Select[AuctionResult](
		stateroutine.OnCall(s.PlaceBid, state),
		stateroutine.OnTimer(30*time.Minute, s.CloseAuction, state),
	), nil
}

// BidFailed is the terminal error handler for PlaceBid. If bid processing
// fails after all retries (e.g., validation service unreachable), return an
// error response to the blocked caller so they can retry manually, without
// crashing the auction.
func (s *AuctionService) BidFailed(ctx *stateroutine.Context, state BiddingState, req PlaceBidReq, err error) (PlaceBidResp, *stateroutine.Suspend[AuctionResult], error) {
	fmt.Printf("bid by %s for $%.2f failed after all retries: %v\n",
		req.BidderID, req.Amount, err)

	// Return an error response to the caller but keep the auction running.
	return PlaceBidResp{
		Accepted: false,
		Message:  fmt.Sprintf("bid processing failed: %v", err),
	}, stateroutine.Select[AuctionResult](
		stateroutine.OnCall(s.PlaceBid, state),
		stateroutine.OnTimer(30*time.Minute, s.CloseAuction, state),
	), nil
}

func (s *AuctionService) CloseAuction(ctx *stateroutine.Context, state BiddingState) (*stateroutine.Suspend[AuctionResult], error) {
	if state.Leader == "" {
		fmt.Printf("auction for %q closed with no bids\n", state.ItemName)
	} else {
		fmt.Printf("auction for %q closed, winner: %s at $%.2f\n",
			state.ItemName, state.Leader, state.HighestBid)
	}

	return stateroutine.Done(AuctionResult{
		ItemName: state.ItemName,
		Winner:   state.Leader,
		Amount:   state.HighestBid,
		BidCount: state.BidCount,
	}), nil
}

// --- main ---

func main() {
	ctx := context.Background()

	svc := &AuctionService{}

	w := stateroutine.NewWorker("auction-queue")
	stateroutine.AddHandler(w, svc.OpenAuction, stateroutine.ErrorPolicy{})
	stateroutine.AddCallHandler(w, svc.PlaceBid, stateroutine.ErrorPolicy{MaxAttempts: 3},
		svc.BidFailed)
	stateroutine.AddHandler(w, svc.CloseAuction, stateroutine.ErrorPolicy{})

	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := stateroutine.NewClient()

	h, err := stateroutine.Start(client, ctx, "auction-001", svc.OpenAuction, AuctionState{
		ItemName:    "Vintage Guitar",
		StartingBid: 100.00,
		Duration:    1 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Alice bids $150 — should be accepted.
	resp, err := stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, PlaceBidReq{
		BidderID: "alice",
		Amount:   150.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Bob bids $120 — should be rejected (too low).
	resp, err = stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, PlaceBidReq{
		BidderID: "bob",
		Amount:   120.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Bob bids $200 — should be accepted.
	resp, err = stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, PlaceBidReq{
		BidderID: "bob",
		Amount:   200.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Check current auction status via query.
	status, err := stateroutine.ClientQuery(client, ctx, "auction-001", AuctionStatusResp{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("auction status: leader=%s, highest=$%.2f, bids=%d\n",
		status.Leader, status.HighestBid, status.BidCount)

	// Wait for the auction to close (in production, the timer would fire).
	result, err := h.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("auction result: winner=%s, amount=$%.2f\n", result.Winner, result.Amount)
}
