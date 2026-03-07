# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Run all tests (in-memory only, no external dependencies)
go test ./...

# Run a single test
go test ./docs/howto/booking/ -run TestBookingHappyPath

# Run tests with the Temporal backend (requires local Temporal server on localhost:7233)
go test ./... -tags temporal

# Run integration tests specifically
go test ./backend/temporal/ -run TestIntegration

# Build / vet
go build ./...
go vet ./...
```

## Architecture

This is a Go library (`durable/` package) that provides "durable goroutines" — actor-like routines powered by Temporal.io that eliminate replay-safety and continue-as-new concerns from user code.

### Core Idea

User handler functions run as **Temporal activities** (no determinism constraints). Instead of blocking when waiting for timers/messages, handlers return a declarative `Continuation` value describing what to wait for next. A library-owned Temporal workflow interprets continuations to set up timers, signal handlers, and update handlers. This means:
- User code is always an activity — normal Go, no replay-safety rules
- The workflow code is 100% library-owned and never changes
- Continue-as-new is automatic and transparent

### Package Layout

- **`durable/`** — Public API. Defines `Worker`, `Client`, `Context`, `Continuation`, handler types, registration functions (`RegisterHandler`, `RegisterSendHandler`, `RegisterCallHandler`), and client operations (`Go`, `Send`, `Call`, `Query`, `Get`).
- **`backend/inmemory/`** — In-memory implementation for unit testing. Provides `Runtime` with deterministic step-by-step execution (`Step()`, `StepAll()`, `AdvanceTime()`) and a controllable `Clock`.
- **`backend/temporal/`** — Temporal implementation. Translates the `durable/` abstractions into Temporal workflows, activities, signals, updates, and queries.
- **`backend/durablecore/`** — Shared internal types between backends.
- **`internal/`** — Internal utilities.
- **`testenv/`** — Test helpers. `testenv.RunAll(t, registerFn, testFn)` runs each test against both in-memory and Temporal backends. `SetupMemory` and `SetupTemporal` create test environments independently.
- **`docs/howto/`** — Example patterns (booking, auction, fanout, saga, batch, etc.) with tests.

### Key Types and Patterns

- **`Payload`** — Interface (`DurableKind() string`) implemented by all input/message/result types. Used for handler registration keys and serialization.
- **Handler types**: `Handler[I, T]`, `SendHandler[I, E, T]`, `CallHandler[I, Req, Resp, T]` — each has a corresponding recovery handler variant that receives the final error after retries are exhausted.
- **Continuation constructors**: `Done`, `Continue`, `ContinueAfter`, `Select` (with `ReceiveSend`, `ReceiveCall`, `After`, `Default` cases).
- **Context operations**: `SetQueryResult`, `BufferStart` (start child routine), `BufferSend` (fire-and-forget to another routine). These are buffered and execute after the handler returns.
- **Registration returns a builder** with `.WithRecoveryHandler()` for chaining recovery handlers (at most one per handler, enforced by types).

### Testing Pattern

Tests use `testenv.RunAll` to run against both backends. In-memory tests need no external services. Temporal tests require a local Temporal server. The in-memory runtime provides deterministic execution — `StepAll()` processes all pending work, `AdvanceTime(d)` simulates timer expiry.

## Design Principles

- Return errors as early as possible: prefer compile-time errors (via generics), then startup-time validation, then runtime errors.
- Handler registration keys are composite strings built from `DurableKind()` values (e.g., `handler:{inputKind}:{resultKind}`, `send:{inputKind}:{externalInputKind}:{resultKind}`).
