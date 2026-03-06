I want to provide a nice, strongly typed interface for interacting with a durable routine from the outside.

The external interface of a durable routine is:
- Start
- Get(id) to get a handle to an existing durable routine
- CallFoo
- CallBar
- SendMessageBaz
- SendMessageFoo
- QueryFoo
- QueryBar

It would be nice to be able to have a strongly typed interface to know which calls, messages and queries are available
on a given type of durable routine.

Maybe protobufs + codegen?

There is an internal and external interface to a durable routine
- External as described above
- Internal which has all the handlers you can suspend to