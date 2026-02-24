### Analysis

Currently, every `HandlerState` and `Message` type must implement `Kind() string`. Across the 8 examples, this adds ~34 one-line methods. The alternative: use `reflect.TypeOf(v).Name()` (or `Elem().Name()` for pointers) to derive the kind string automatically.

**Pro reflection:**
- Eliminates ~34 boilerplate `Kind()` methods across examples.
- Simpler interface — types just need to be structs, no method required.
- Lower barrier to entry for new users.

**Con reflection:**
1. **Renaming a struct silently breaks routing keys for in-flight stateroutines.** This is the killer issue. Handler registration keys like `"send:booking.reserved:payment"` are derived from `Kind()` strings. With reflection, these keys become Go struct names (`"send:ReservedState:PaymentInfo"`). Renaming `ReservedState` → `ReservationState` silently changes the routing key, breaking any stateroutine that was suspended waiting for a message on the old key. For durable workflows that run for days/weeks, this is a data-loss scenario.
2. **No semantic naming.** Stuck with Go struct names, which may not match the desired routing semantics. `BookingState.Kind()` returns `"booking"`, but `reflect.TypeOf(BookingState{}).Name()` returns `"BookingState"`.
3. **No versioning support.** Explicit `Kind()` allows `"booking.v2"` for gradual migration. Reflection ties you to the struct name.
4. **Package path sensitivity.** Two packages with a `State` struct would collide under `Name()`. Using `PkgPath()` + `Name()` avoids this but creates keys like `"github.com/foo/bar.State"` which are fragile across module renames.
5. **Cross-package visibility.** If a sender in package A sends to a receiver in package B, the sender needs to know the receiver's struct name — leaking implementation details across package boundaries.

**Middle ground: Reflection as default with `Kind()` override**
- Use reflection unless the type implements `Kind()`.
- Adds complexity: two ways to do the same thing, ambiguity about which is in effect, harder to reason about routing keys.

### Recommendation

**Keep explicit `Kind()`.** The one-line-per-type cost is low compared to the risks. For a durable workflow system where routing keys are persisted across process restarts and continue-as-new boundaries, stability of those keys is paramount. A `Kind()` method is a deliberate declaration: "this is my stable identity." Reflection makes that identity an accident of naming.
