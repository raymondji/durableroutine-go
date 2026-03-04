What are the core probelms that durable execution solves? i.e. what is the motivation for durable execution in the first place?

What are the core problems that need to be solved to enable durable execution?

Required elements:
- serializable payloads
- persistent storage
- router (maps payloads to handler functions)
- retry safe handlers (ideally idempotent)
- What's wrong with having a single handler that does everything? You may want the ability to split a single payload/handler into multiple steps. This is helpful when a handler has many steps that take a long time OR that are cost to run, so you don't always want to retry from the beginning. It also helps if your steps are not 100% idempotent (which is hard to do!), so avoiding extremely delayed retries can be very helpful. It's also useful if you want to customize the retry policy for different steps (e.g. you may not be able to make sending an email idempotent! so you probably want to limit # of retries. Versus you can make a database write idempotent, so you may want infinite retries).
- Queueing controls (rate limiting, concurrency limits, etc.)
- Transactionl enqueue and commit
- Signals/messages/etc?

Show the evolution: start with the simplest possible model, find where the holes are, and show to solve those holes
1. Just use a goroutine. Holes: It crashes, oh no
2. Push a serializable payload onto a message queue. Have a worker that pulls from the message queue and executes a handler function. Holes: handler function must be retry safe. What about multiple steps?

Try to separate inherent/unavoidable complexity and optional/avoidable complexity.
- This isn't to say that the optional complexity serves no purpose. It can be used to solve a problem, there just might be other ways to solve that problem with different tradeoffs.

Problems with Temporal's model:
- Replay safety of workflow code
- Continue as new
- Confusing concurrency model (temporal go sdk uses cooperative multithreading within a workflow, which A) feels at odds with Go's goroutine concurrency model b/c you often don't need to use mutexes/etc where you think you should AND B) does still require that you use synchronization primitives some of the time. hard to tell which)

Stackful vs stackless programming
- Show how preemption and crashing are somewhat isomorphic

Also: imagine a hypothetical programming language where you could fully serialize the stack frames/program counter/local values/etc. What problems remain?
- You still need to write code that is retry safe/idempotent. Down to the single instruction level, you still need to retry that instruction to account for side effects (like network requests). Classic distribued systems problem.