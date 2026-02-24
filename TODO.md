# TODO

These are ready for Claude to work on.

# Draft TODOs

These are not ready for Claude to work on yet.

## Rename Spawn to Start or something similar for consistency

Let's aim for maximum consistency on naming with the APIs.

## Explore splitting the stateroutine client API and actual Stateroutine handler API into two packages for clarity

Right now we have e.g. ClientSend vs Send. Would two separate packages allow the funciton names to be simpler? Also, would that make it clearer which functions are available to use in which context?

If we do this though, what do we name the respective packages? Also do we need to

Write the output under RFCs/SINGLE_VS_MULTI_PACKAGE.md

# DONE

## Deferred Spawn/Send clarity

Explored 5 options (rename methods, fold into Suspend, docs only, pending tokens, keep imperative + enhanced docs). Recommendation: **Option E** — keep imperative calls, make deferred nature prominent in doc comments and design doc. Loop-based usage in fanout/pipeline strongly favors imperative calls.

## Reflection vs Kind()

Analyzed reflection as alternative to explicit `Kind()` methods. Recommendation: **Keep explicit `Kind()`.** Renaming a struct silently breaks routing keys for in-flight stateroutines (killer issue for durable workflows). One-line-per-type cost is low vs risks.

## Create Temporal implementation plan

Written to `DESIGN_DOC_TEMPORAL_IMPL.md`. Covers `temporalimpl/` package structure, client mapping, workflow loop, activity model, worker registration, continue-as-new, and signal buffering.

## Create in-memory implementation plan

Written to `DESIGN_DOC_IN_MEMORY_IMPL.md`. Covers `memoryimpl/` package structure, runtime, client, step-by-step execution, timer simulation, signal buffering, and concurrency model.

## Convert examples to library packages + tests

Restructured all 8 examples (auction, batch, booking, fanout, order, pipeline, reminder, saga) from `package main` to library packages with `RegisterHandlers()`, `cmd/main.go` for runnable demos, and `*_test.go` with skipped test cases (awaiting memoryimpl).

## Make ClientSend / Send take in a function param

Just like how Start does. ClientSend and Send now accept a handler function param for type inference of the target state kind, ensuring correct routing key construction. Pass nil with explicit type params when the sender doesn't have the receiver's handler.

## Change how Queries behave. Instead of SetQueryHandler, we should do SetQueryResult.

Temporal's query handlers cannot invoke activities, so they cannot actually call the SetQueryHandler handler that we define.

Instead we should let handlers SetQueryResult to a static value, and then ClientQuery can retrieve that value.

## Rename Cast -> Send

Cast comes straight from genserver, but I think Send is a slightly more intuitive name for people who haven't worked with Genserver before.

## Rename the library to stateroutine

A portmanteau of "state machine" + "goroutine"

## Make this easy to install as a go library

Not sure if any changes are needed for this

## Add support for defining an error state handler on RetryPolicy

By default, after all retries are exhausted, the entire stateroutine fails.

However I want to add support for saying, once all retries are exhausted, transition to this error state handler instead.

The error state handler itself should then behave like any other state handler, and can suspend/etc.

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

The comparison should help people who are familiar with those other projects to quickly get an intuition on how stateroutine works.