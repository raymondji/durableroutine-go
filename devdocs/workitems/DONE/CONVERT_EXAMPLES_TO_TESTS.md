# Convert examples to library packages + tests

Restructured all 8 examples (auction, batch, booking, fanout, order, pipeline, reminder, saga) from `package main` to library packages with `RegisterHandlers()`, `cmd/main.go` for runnable demos, and `*_test.go` with skipped test cases (awaiting inmemory backend).
