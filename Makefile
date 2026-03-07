.PHONY: temporal-start test test-integration

## Start a local Temporal dev server (port 7233, UI on 8233).
temporal-start:
	temporal server start-dev

## Run all tests (requires a running Temporal server for integration tests).
test:
	go test ./...

## Run only the Temporal integration tests.
test-integration:
	go test ./backend/temporal/...
