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
	Deadline   time.Time
	HighestBid float64
	Leader     string
	BidCount   int
}

func (BiddingInput) DurableKind() string { return "auction.bidding" }

// --- Public interface ---

// Auction exposes only the methods that external callers should interact with:
// starting an auction and placing bids.
type Auction interface {
	OpenAuction(ctx *durable.Context, input AuctionInput) (*durable.Continuation[AuctionResult], error)
	PlaceBid(ctx *durable.Context, input BiddingInput, externalReq PlaceBidReq) (PlaceBidResp, *durable.Continuation[AuctionResult], error)
}

// AuctionRegistry exposes only the methods needed to register handlers with a worker.
type AuctionRegistry interface {
	RegisterHandlers(w *durable.Worker)
}

// NewAuctionRegistry creates a fully functional AuctionRegistry for the worker side.
func NewAuctionRegistry() AuctionRegistry {
	return &AuctionService{}
}

// NewAuctionServiceStub creates a stub Auction whose methods are only used as
// typed references for durable.Go and durable.Call (which use them for type
// inference, not invocation). The stub should not be used to register handlers.
func NewAuctionServiceStub() Auction {
	return &auctionStub{}
}

// --- Stub ---

type auctionStub struct{}

func (s *auctionStub) OpenAuction(ctx *durable.Context, input AuctionInput) (*durable.Continuation[AuctionResult], error) {
	panic("auctionStub: OpenAuction should not be called directly; use as a typed reference only")
}

func (s *auctionStub) PlaceBid(ctx *durable.Context, input BiddingInput, externalReq PlaceBidReq) (PlaceBidResp, *durable.Continuation[AuctionResult], error) {
	panic("auctionStub: PlaceBid should not be called directly; use as a typed reference only")
}

// --- Service struct ---

type AuctionService struct {
	// Injected dependencies would go here (e.g., notification service).
}

func (s *AuctionService) OpenAuction(ctx *durable.Context, input AuctionInput) (*durable.Continuation[AuctionResult], error) {
	fmt.Printf("auction opened for %q, starting bid: $%.2f\n", input.ItemName, input.StartingBid)

	bidding := BiddingInput{
		ItemName:   input.ItemName,
		Deadline:   time.Now().Add(input.Duration),
		HighestBid: input.StartingBid,
	}

	durable.SetQueryResult(ctx, AuctionStatusResp{
		ItemName:   bidding.ItemName,
		HighestBid: bidding.HighestBid,
	})

	return durable.Select(
		durable.ReceiveCall(s.PlaceBid, bidding),
		durable.After(time.Until(bidding.Deadline), s.CloseAuction, bidding),
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
				durable.After(time.Until(input.Deadline), s.CloseAuction, input),
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
			durable.After(time.Until(input.Deadline), s.CloseAuction, input),
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
			durable.After(time.Until(input.Deadline), s.CloseAuction, input),
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
func (s *AuctionService) RegisterHandlers(w *durable.Worker) {
	durable.RegisterHandler(w, s.OpenAuction, durable.HandlerOptions{})
	durable.RegisterCallHandler(w, s.PlaceBid, durable.HandlerOptions{
		RetryPolicy: durable.RetryPolicy{MaxAttempts: 3},
	}).WithRecoveryHandler(s.BidFailed, durable.HandlerOptions{})
	durable.RegisterHandler(w, s.CloseAuction, durable.HandlerOptions{})
}
