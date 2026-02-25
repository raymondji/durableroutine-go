// Package auction demonstrates ClientCall for synchronous request-response.
// Bidders place bids via ClientCall and immediately learn whether their bid
// was accepted or outbid. The auction runs until a timer expires, then
// completes with the winning bid. Current status is available via ClientQuery.
//
// Also demonstrates CallRecoveryHandler: if bid processing fails after all
// retries, the recovery handler returns an error response to the blocked
// caller instead of failing the entire routine.
package auction

import (
	"fmt"
	"time"

	"github.com/raymondji/durableroutine-go/durable"
)

// --- State ---

type AuctionInput struct {
	ItemName    string
	StartingBid float64
	Duration    time.Duration
}

func (AuctionInput) DurableKind() string { return "auction" }

// --- Messages ---

type PlaceBidReq struct {
	BidderID string
	Amount   float64
}

func (PlaceBidReq) DurableKind() string { return "place-bid" }

type PlaceBidResp struct {
	Accepted   bool
	HighestBid float64
	Message    string
}

func (PlaceBidResp) DurableKind() string { return "place-bid-resp" }

type AuctionStatusResp struct {
	ItemName   string
	HighestBid float64
	Leader     string
	BidCount   int
}

func (AuctionStatusResp) DurableKind() string { return "auction-status" }

// --- Result ---

type AuctionResult struct {
	ItemName string
	Winner   string
	Amount   float64
	BidCount int
}

func (AuctionResult) DurableKind() string { return "auction-result" }

// --- Per-step state types ---

type BiddingInput struct {
	ItemName   string
	Duration   time.Duration
	HighestBid float64
	Leader     string
	BidCount   int
}

func (BiddingInput) DurableKind() string { return "auction.bidding" }

// --- Service struct ---

type AuctionService struct {
	// Injected dependencies would go here (e.g., notification service).
}

func (s *AuctionService) OpenAuction(ctx *durable.Context, input AuctionInput) (*durable.Continuation[AuctionResult], error) {
	fmt.Printf("auction opened for %q, starting bid: $%.2f\n", input.ItemName, input.StartingBid)

	bidding := BiddingInput{
		ItemName:   input.ItemName,
		Duration:   input.Duration,
		HighestBid: input.StartingBid,
	}

	durable.SetQueryResult(ctx, AuctionStatusResp{
		ItemName:   bidding.ItemName,
		HighestBid: bidding.HighestBid,
	})

	return durable.Select(
		durable.ReceiveCall(s.PlaceBid, bidding),
		durable.After(input.Duration, s.CloseAuction, bidding),
	), nil
}

func (s *AuctionService) PlaceBid(ctx *durable.Context, input BiddingInput, externalReq PlaceBidReq) (PlaceBidResp, *durable.Continuation[AuctionResult], error) {
	if externalReq.Amount <= input.HighestBid {
		return PlaceBidResp{
				Accepted:   false,
				HighestBid: input.HighestBid,
				Message:    fmt.Sprintf("bid too low, current highest is $%.2f", input.HighestBid),
			}, durable.Select(
				durable.ReceiveCall(s.PlaceBid, input),
				durable.After(input.Duration, s.CloseAuction, input),
			), nil
	}

	fmt.Printf("new high bid: $%.2f by %s (was $%.2f by %s)\n",
		externalReq.Amount, externalReq.BidderID, input.HighestBid, input.Leader)

	input.HighestBid = externalReq.Amount
	input.Leader = externalReq.BidderID
	input.BidCount++

	durable.SetQueryResult(ctx, AuctionStatusResp{
		ItemName:   input.ItemName,
		HighestBid: input.HighestBid,
		Leader:     input.Leader,
		BidCount:   input.BidCount,
	})

	return PlaceBidResp{
			Accepted:   true,
			HighestBid: input.HighestBid,
			Message:    "bid accepted, you are the highest bidder",
		}, durable.Select(
			durable.ReceiveCall(s.PlaceBid, input),
			durable.After(input.Duration, s.CloseAuction, input),
		), nil
}

func (s *AuctionService) BidFailed(ctx *durable.Context, input BiddingInput, externalReq PlaceBidReq, err error) (PlaceBidResp, *durable.Continuation[AuctionResult], error) {
	fmt.Printf("bid by %s for $%.2f failed after all retries: %v\n",
		externalReq.BidderID, externalReq.Amount, err)

	return PlaceBidResp{
			Accepted: false,
			Message:  fmt.Sprintf("bid processing failed: %v", err),
		}, durable.Select(
			durable.ReceiveCall(s.PlaceBid, input),
			durable.After(input.Duration, s.CloseAuction, input),
		), nil
}

func (s *AuctionService) CloseAuction(ctx *durable.Context, input BiddingInput) (*durable.Continuation[AuctionResult], error) {
	if input.Leader == "" {
		fmt.Printf("auction for %q closed with no bids\n", input.ItemName)
	} else {
		fmt.Printf("auction for %q closed, winner: %s at $%.2f\n",
			input.ItemName, input.Leader, input.HighestBid)
	}

	return durable.Done(AuctionResult{
		ItemName: input.ItemName,
		Winner:   input.Leader,
		Amount:   input.HighestBid,
		BidCount: input.BidCount,
	}), nil
}

// RegisterHandlers registers all auction handlers with the worker.
func RegisterHandlers(w *durable.Worker, svc *AuctionService) {
	durable.RegisterHandler(w, svc.OpenAuction, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, svc.PlaceBid, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	}).WithRecoveryHandler(svc.BidFailed, durable.HandlerOptions{})
	durable.RegisterHandler(w, svc.CloseAuction, durable.HandlerOptions{})
}
