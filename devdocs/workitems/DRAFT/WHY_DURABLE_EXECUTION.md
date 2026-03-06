What are the core probelms that durable execution solves? i.e. what is the motivation for durable execution in the first place?

Imagine a simplified use case of purchasing flight tickets. The backend needs to:
1. Charge a payment to your card (external RPC)
2. Record purchase in a database
3. Send a confirmation email
4. Allow cancellation within 24hours
5. Send a remainder email to check-in
