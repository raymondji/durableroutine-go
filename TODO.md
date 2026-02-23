# TODOs to implement

## Change how Queries behave. Instead of SetQueryHandler, we should do SetQueryResult.

Temporal's query handlers cannot invoke activities, so they cannot actually call the SetQueryHandler handler that we define.

Instead we should let handlers SetQueryResult to a static value, and then ClientQuery can retrieve that value.

## Rename Cast -> Send

Cast comes straight from genserver, but I think Send is a slightly more intuitive name for people who haven't worked with Genserver before.

## Add support for defining an error state handler on RetryPolicy

By default, after all retries are exhausted, the entire Routine fails.

However I want to add support for saying, once all retries are exhausted, transition to this error state handler instead.

The error state handler itself should then behave like any other state handler, and can suspend/etc.

The error state handler should take the exact same inputs as the original state handler it's attached to.

Please pay attention to handler registration and make sure all the keys we generate are unique.

Then use the error state handler in the SAGA example to implement compensations.
