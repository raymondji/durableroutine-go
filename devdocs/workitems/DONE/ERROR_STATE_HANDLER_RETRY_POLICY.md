# Add support for defining an error state handler on RetryPolicy

By default, after all retries are exhausted, the entire routine fails.

However I want to add support for saying, once all retries are exhausted, transition to this error state handler instead.

The error state handler itself should then behave like any other state handler, and can return a continuation/etc.

The error state handler should take the exact same inputs as the original state handler it's attached to.

Please pay attention to handler registration and make sure all the keys we generate are unique.

Then use the error state handler in the SAGA example to implement compensations.
