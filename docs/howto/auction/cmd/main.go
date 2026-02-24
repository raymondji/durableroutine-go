package main

import (
	"context"
	"fmt"
	"log"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/raymondji/stateroutine/docs/howto/auction"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/temporalimpl"
)

func main() {
	ctx := context.Background()

	tc, err := temporalclient.Dial(temporalclient.Options{HostPort: "localhost:7233"})
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Close()

	svc := &auction.AuctionService{}

	w := stateroutine.NewWorker("auction-queue")
	auction.RegisterHandlers(w, svc)

	tw := temporalimpl.NewWorker(tc, w)
	go func() {
		if err := tw.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer tw.Stop()

	client := stateroutine.NewClientFrom(temporalimpl.NewClient(tc, "auction-queue"))

	h, err := stateroutine.Start(client, ctx, "auction-001", svc.OpenAuction, auction.AuctionState{
		ItemName:    "Vintage Guitar",
		StartingBid: 100.00,
		Duration:    1 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Alice bids $150 — should be accepted.
	resp, err := stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "alice",
		Amount:   150.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Bob bids $120 — should be rejected (too low).
	resp, err = stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "bob",
		Amount:   120.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Bob bids $200 — should be accepted.
	resp, err = stateroutine.ClientCall(client, ctx, "auction-001", svc.PlaceBid, auction.PlaceBidReq{
		BidderID: "bob",
		Amount:   200.00,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob's bid: accepted=%v, message=%q\n", resp.Accepted, resp.Message)

	// Check current auction status via query.
	status, err := stateroutine.ClientQuery(client, ctx, "auction-001", auction.AuctionStatusResp{})
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
