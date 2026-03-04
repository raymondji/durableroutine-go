E.g. if you want to race a signal to do something and a cancellation timeout. 

If the timeout happens, we need to cancel the signal (or clear the queued up signals?) so that later on the signal does not get "reused". How do make sure we consume the signal and not just let it sit in the queue?

If the signal happens, we need to make sure we clear the timer. (I think this may happen already, since I don't think we queue up timers the same way we queue up signals).

Maybe just make this a user problem. Make it clear to the user that if you have loops in your state machine or consume from a signal channel multiple times, you need to account for hadnling stale signals. But still need to provide a "Purge" function.