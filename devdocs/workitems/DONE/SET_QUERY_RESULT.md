# Change how Queries behave. Instead of SetQueryHandler, we should do SetQueryResult.

Temporal's query handlers cannot invoke activities, so they cannot actually call the SetQueryHandler handler that we define.

Instead we should let handlers SetQueryResult to a static value, and then ClientQuery can retrieve that value.
