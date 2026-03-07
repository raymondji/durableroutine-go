# Workflow cancellation

No `Cancel()` API. Currently must simulate via signals + handler cooperation. Could add `durable.Cancel(client, ctx, id)` that maps to Temporal's workflow cancellation.
