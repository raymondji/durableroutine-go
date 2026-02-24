// Package auction demonstrates ClientCall for synchronous request-response.
// Bidders place bids via ClientCall and immediately learn whether their bid
// was accepted or outbid. The auction runs until a timer expires, then
// completes with the winning bid. Current status is available via ClientQuery.
//
// Also demonstrates OnCallTerminalError: if bid processing fails after all
// retries, the terminal error handler returns an error response to the blocked
// caller instead of failing the entire stateroutine.
package auction

import (
	"fmt"
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
	Duration   time.Duration
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
		Duration:   state.Duration,
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

func (s *AuctionService) PlaceBid(ctx *stateroutine.Context, state BiddingState, req PlaceBidReq) (PlaceBidResp, *stateroutine.Suspend[AuctionResult], error) {
	if req.Amount <= state.HighestBid {
		return PlaceBidResp{
			Accepted:   false,
			HighestBid: state.HighestBid,
			Message:    fmt.Sprintf("bid too low, current highest is $%.2f", state.HighestBid),
		}, stateroutine.Select[AuctionResult](
			stateroutine.OnCall(s.PlaceBid, state),
			stateroutine.OnTimer(state.Duration, s.CloseAuction, state),
		), nil
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

	return PlaceBidResp{
		Accepted:   true,
		HighestBid: state.HighestBid,
		Message:    "bid accepted, you are the highest bidder",
	}, stateroutine.Select[AuctionResult](
		stateroutine.OnCall(s.PlaceBid, state),
		stateroutine.OnTimer(state.Duration, s.CloseAuction, state),
	), nil
}

func (s *AuctionService) BidFailed(ctx *stateroutine.Context, state BiddingState, req PlaceBidReq, err error) (PlaceBidResp, *stateroutine.Suspend[AuctionResult], error) {
	fmt.Printf("bid by %s for $%.2f failed after all retries: %v\n",
		req.BidderID, req.Amount, err)

	return PlaceBidResp{
		Accepted: false,
		Message:  fmt.Sprintf("bid processing failed: %v", err),
	}, stateroutine.Select[AuctionResult](
		stateroutine.OnCall(s.PlaceBid, state),
		stateroutine.OnTimer(state.Duration, s.CloseAuction, state),
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

// RegisterHandlers registers all auction handlers with the worker.
func RegisterHandlers(w *stateroutine.Worker, svc *AuctionService) {
	stateroutine.AddHandler(w, svc.OpenAuction, stateroutine.HandlerOptions{})
	stateroutine.AddCallHandler(w, svc.PlaceBid, stateroutine.HandlerOptions{
		RetryPolicy: stateroutine.RetryPolicy{MaxAttempts: 3},
	}).OnTerminalError(svc.BidFailed, stateroutine.HandlerOptions{})
	stateroutine.AddHandler(w, svc.CloseAuction, stateroutine.HandlerOptions{})
}
