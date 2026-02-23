# TODOs to implement

## Change how Queries behave. Instead of SetQueryHandler, we should do SetQueryResult.

Temporal's query handlers cannot invoke activities, so they cannot actually call the SetQueryHandler handler that we define.

Instead we should let handlers SetQueryResult to a static value, and then ClientQuery can retrieve that value.

## Rename Cast -> Send

Cast comes straight from genserver, but I think Send is a slightly more intuitive name for people who haven't worked with Genserver before.

## Rename the library to stateroutine

A portmanteau of "state machine" + "goroutine"

## Make this easy to install as a go library

Not sure if any changes are needed for this

## How do we make it more clear that spawning/casting to other Routines within a Routine handler does not happen immediately?

Some possible ideas:
1. Make the names of the methods like SpawnAsync or BufferSpawn
2. Change the return signature to something like Suspend(Select(..), Spawn(...), Cast(...))

Please explore these and other options before proceeding.

## Add support for defining an error state handler on RetryPolicy

By default, after all retries are exhausted, the entire Routine fails.

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