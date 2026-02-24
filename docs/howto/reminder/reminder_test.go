package reminder_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/durableroutine-go/docs/howto/reminder"
	"github.com/raymondji/durableroutine-go/durable"
	"github.com/raymondji/durableroutine-go/testenv"
)

func TestReminderFullSequence(t *testing.T) {
	svc := &reminder.ReminderService{
		InitialDelay:  1 * time.Millisecond,
		FollowUpDelay: 1 * time.Millisecond,
	}
	testenv.RunAll(t, func(w *durable.Worker) {
		reminder.RegisterHandlers(w, svc)
	}, func(t *testing.T, env *testenv.Env) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h, err := durable.Go(env.Client, ctx, env.UniqueID("reminder"), svc.SendInitial, reminder.InitialState{Email: "test@example.com"})
		if err != nil {
			t.Fatalf("Go failed: %v", err)
		}

		result, err := h.Get(ctx)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		_ = result // durable.Unit{}
		t.Logf("Reminder completed successfully")
	})
}
