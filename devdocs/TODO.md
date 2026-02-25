# TODO

These are ready for Claude to work on.

## Write top-level README.md for the repo

In particular, give a good pitch for the repo. Mention two ways to think about this:
1. Bottom up: Take native goroutines and channels, but add durability and distribued computing
2. Top down: Most of the Temporal goodness, but remove the burden of managing replay safety and continue-as-new

## Acknowledge inspiration from continuation-passing-style

## Acknowledge inspiration from indeed workflow framework and add comparison

https://github.com/indeedeng/iwf

# Draft TODOs

These are not ready for Claude to work on yet.

## Consider making SetQueryResult a way to set a KV-store

## Cancel signals/timeouts

E.g. if you want to race a signal to do something and a cancellation timeout. 

If the timeout happens, we need to cancel the signal (or clear the queued up signals?) so that later on the signal does not get "reused". How do make sure we consume the signal and not just let it sit in the queue?

If the signal happens, we need to make sure we clear the timer. (I think this may happen already, since I don't think we queue up timers the same way we queue up signals).

## Activity heartbeats

Long-running handlers can't report progress or detect cancellation mid-execution. Would need to plumb heartbeat capability through `durable.Context`.

## Search attributes

No way to tag routines with custom searchable metadata. Currently must use external indexing. Could expose via `HandlerOptions` or `Go()` options.

## Workflow cancellation

No `Cancel()` API. Currently must simulate via signals + handler cooperation. Could add `durable.Cancel(client, ctx, id)` that maps to Temporal's workflow cancellation.

## Workflow execution timeouts

Routines run indefinitely until `Done()`. No way to set an overall deadline. Could expose via `Go()` options.

## Cron / Schedules

No periodic execution support. Need external scheduler. Could integrate with Temporal's Schedule feature.

## Interceptors

Can't hook into workflow/activity lifecycle for observability, auth, etc.

## Memos / metadata

Can't attach arbitrary metadata to workflow executions.

## Custom data converters

JSON only. Can't use protobuf or custom serialization.

## Local activities

All activities are regular Temporal activities (full scheduling overhead). Could expose for lightweight handlers.

## Workflow ID reuse policy configuration

Hardcoded to `ALLOW_DUPLICATE_FAILED_ONLY`. Could expose via `Go()` options.

## Correlation ID / request tracing

No built-in correlation ID propagation. Must thread through every state struct manually. Could add to `durable.Context`.

## Direct child workflow result access (ReceiveGet)

A parent can't `Get()` a child's result. Children must explicitly send results back via `BufferSend()`, adding extra message types. Could add a `durable.ReceiveGet()` continuation that waits for a child routine to complete and feeds its result into the next handler.

## Explore splitting the durable routine client API and actual routine handler API into two packages for clarity

Right now we have e.g. ClientSend vs Send. Would two separate packages allow the funciton names to be simpler? Also, would that make it clearer which functions are available to use in which context?

If we do this though, what do we name the respective packages? Also do we need to

Write the output under RFCs/SINGLE_VS_MULTI_PACKAGE.md

# DONE

## Buffered side effects (BufferStart/BufferSend) naming

Explored 5 options (rename methods, fold into Continuation, docs only, pending tokens, keep imperative + enhanced docs). Recommendation: **Option E** — keep imperative calls, make deferred nature prominent in doc comments and design doc. Loop-based usage in fanout/pipeline strongly favors imperative calls.

## Reflection vs Kind()

Analyzed reflection as alternative to explicit `Kind()` methods. Recommendation: **Keep explicit `Kind()`.** Renaming a struct silently breaks routing keys for in-flight routines (killer issue for durable workflows). One-line-per-type cost is low vs risks.

## Create Temporal implementation plan

Written to `DESIGN_DOC_TEMPORAL_IMPL.md`. Covers `backend/temporal/` package structure, client mapping, workflow loop, activity model, worker registration, continue-as-new, and signal buffering.

## Create in-memory implementation plan

Written to `DESIGN_DOC_IN_MEMORY_IMPL.md`. Covers `backend/inmemory/` package structure, runtime, client, step-by-step execution, timer simulation, signal buffering, and concurrency model.

## Convert examples to library packages + tests

Restructured all 8 examples (auction, batch, booking, fanout, order, pipeline, reminder, saga) from `package main` to library packages with `RegisterHandlers()`, `cmd/main.go` for runnable demos, and `*_test.go` with skipped test cases (awaiting inmemory backend).

## Make ClientSend / Send take in a function param

Just like how Go does. ClientSend and Send now accept a handler function param for type inference of the target state kind, ensuring correct routing key construction. Pass nil with explicit type params when the sender doesn't have the receiver's handler.

## Change how Queries behave. Instead of SetQueryHandler, we should do SetQueryResult.

Temporal's query handlers cannot invoke activities, so they cannot actually call the SetQueryHandler handler that we define.

Instead we should let handlers SetQueryResult to a static value, and then ClientQuery can retrieve that value.

## Rename Cast -> Send

Cast comes straight from genserver, but I think Send is a slightly more intuitive name for people who haven't worked with Genserver before.

## Rename the library to durable routine

Emphasizes the durable execution model

## Make this easy to install as a go library

Not sure if any changes are needed for this

## Add support for defining an error state handler on RetryPolicy

By default, after all retries are exhausted, the entire routine fails.

However I want to add support for saying, once all retries are exhausted, transition to this error state handler instead.

The error state handler itself should then behave like any other state handler, and can return a continuation/etc.

The error state handler should take the exact same inputs as the original state handler it's attached to.

Please pay attention to handler registration and make sure all the keys we generate are unique.

Then use the error state handler in the SAGA example to implement compensations.

## Update the docs to discuss what this project was inspired by and compare them

Inspirations:
- Temporal
- Elixir Genserver
- Go's native goroutines & channels
- Riverqueue for type safety ergonomics around invoking handlers

Compare concepts (send, call, get result, etc.) with:
- Temporal
- Elixir Genserver
- Go's native goroutines & channels

The comparison should help people who are familiar with those other projects to quickly get an intuition on how durable routine works.