package reminder_test

import (
	"context"
	"testing"
	"time"

	"github.com/raymondji/stateroutine/docs/howto/reminder"
	"github.com/raymondji/stateroutine/stateroutine"
	"github.com/raymondji/stateroutine/testenv"
)

func TestReminderFullSequence(t *testing.T) {
	svc := &reminder.ReminderService{
		InitialDelay:  1 * time.Millisecond,
		FollowUpDelay: 1 * time.Millisecond,
	}
	env := testenv.Setup(t, func(w *stateroutine.Worker) {
		reminder.RegisterHandlers(w, svc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := stateroutine.Start(env.Client, ctx, env.UniqueID("reminder"), svc.SendInitial, reminder.InitialState{Email: "test@example.com"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	result, err := h.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_ = result // stateroutine.Unit{}
	t.Logf("Reminder completed successfully")
}
