If a routine is suspended in an incorrect configuration, do we need a way to patch this?

E.g. lets say a routine set a wait for 10 years by accident, when it shoudl have been 10 days. How do we fix this?

Does Temporal have this problem?

What about something like XState?
- I think no b/c XState tracks the current state. So you can update the out transitions dynamically from the current state.
- Our library doesn't do this, hmm.

Hmmmmmmm is this a big problem?