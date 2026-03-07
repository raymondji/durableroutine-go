# Make ClientSend / Send take in a function param

Just like how Go does. ClientSend and Send now accept a handler function param for type inference of the target state kind, ensuring correct routing key construction. Pass nil with explicit type params when the sender doesn't have the receiver's handler.
