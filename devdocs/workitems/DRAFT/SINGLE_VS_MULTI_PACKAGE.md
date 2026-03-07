# Explore splitting the durable routine client API and actual routine handler API into two packages for clarity

Right now we have e.g. ClientSend vs Send. Would two separate packages allow the function names to be simpler? Also, would that make it clearer which functions are available to use in which context?

If we do this though, what do we name the respective packages? Also do we need to

Write the output under RFCs/SINGLE_VS_MULTI_PACKAGE.md
