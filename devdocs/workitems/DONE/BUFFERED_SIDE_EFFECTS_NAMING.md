# Buffered side effects (BufferStart/BufferSend) naming

Explored 5 options (rename methods, fold into Continuation, docs only, pending tokens, keep imperative + enhanced docs). Recommendation: **Option E** — keep imperative calls, make deferred nature prominent in doc comments and design doc. Loop-based usage in fanout/pipeline strongly favors imperative calls.
