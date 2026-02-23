// Command reminder demonstrates a simple durable process that sends a
// sequence of emails with durable sleeps between them.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/raymondji/durableroutine/durable"
)

// --- Args & state ---

// StartArgs is passed to Client.Start and used to initialize the state.
type StartArgs struct {
	Email string
}

// State holds only what persists across steps.
type State struct {
	Email string
	Step  string
}

// --- Process definition ---

var process = durable.Process[State]{
	Name: "reminder",
	InitState: func(args any) State {
		a := args.(StartArgs)
		return State{Email: a.Email}
	},
	Initial: sendInitial,
}

func sendInitial(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Printf("sending initial email to %s\n", state.Email)
	state.Step = "initial_sent"

	return durable.After(24*time.Hour, sendFollowUp), nil
}

func sendFollowUp(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Printf("sending follow-up email to %s\n", state.Email)
	state.Step = "followup_sent"

	return durable.After(7*24*time.Hour, sendFinal), nil
}

func sendFinal(ctx context.Context, state *State) (*durable.Suspend[State], error) {
	fmt.Printf("sending final email to %s\n", state.Email)
	state.Step = "complete"

	return nil, nil
}

// --- main ---

func main() {
	ctx := context.Background()

	w := durable.NewWorker("reminder-queue")
	w.Register(process)
	go func() {
		if err := w.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	defer w.Stop()

	client := durable.NewClient()
	if err := client.Start(ctx, "reminder-user-42", process, StartArgs{Email: "user@example.com"}); err != nil {
		log.Fatal(err)
	}

	var result State
	if err := client.GetResult(ctx, "reminder-user-42", &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("reminder complete: step=%s\n", result.Step)
}
