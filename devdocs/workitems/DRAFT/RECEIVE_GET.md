# Direct child workflow result access (ReceiveGet)

A parent can't `Get()` a child's result. Children must explicitly send results back via `BufferSend()`, adding extra message types. Could add a `durable.ReceiveGet()` continuation that waits for a child routine to complete and feeds its result into the next handler.
