# bubus-ts: Python vs JS Differences (and the tricky parts)

This README only covers the differences between the Python implementation and this TypeScript port, plus the
gotchas we uncovered while matching behavior. It intentionally does **not** re-document the full TS API surface.

## Key Differences vs Python

### 1) Awaiting events: `event.done()` instead of `await event`
- Python: `await event` waits for handlers and can jump the queue when awaited inside a handler.
- TS: use `await event.done()` for the same behavior.
- Outside a handler, `done()` just waits for completion (it does not jump the queue).
- Inside a handler, `done()` triggers immediate processing (queue jump) on **all buses** where the event is queued.

### 2) Cross-bus queue jump (forwarding)
- Python uses a global re-entrant lock to let awaited events process immediately on every bus where they appear.
- TS does **not** use AsyncLocalStorage or a global lock (browser support).
- Instead, `EventBus.instances` + `run_now_depth` pauses each runloop and processes the same event immediately across buses.

### 3) `event.bus` is a BusScopedEvent view
- In Python, `event.event_bus` is dynamic (contextvars).
- In TS, `event.bus` is provided by a **BusScopedEvent** (a Proxy over the original event).
- That proxy injects a bus-bound `emit/dispatch` to ensure correct parent/child tracking.

### 4) Monotonic timestamps
- JS `Date.now()` is not strictly monotonic at millisecond granularity.
- To keep FIFO tests stable, we generate strictly increasing ISO timestamps via `BaseEvent.nextIsoTimestamp()`.

### 5) No middleware, no WAL, no SQLite mirrors
- Those Python features were intentionally dropped for the JS version.

## Gotchas and Design Choices (What surprised us)

### A) Why we keep a handler stack (context without AsyncLocalStorage)
We need to know **which handler is currently executing** to correctly assign:
- `event_parent_id`
- `event_emitted_by_handler_id`
- and to attach child events under the correct handler in the tree.

Looking at `EventResult.status` alone is not enough because multiple handlers can be `started` at the same time
(nested awaits). The stack gives us deterministic, correct parentage without AsyncLocalStorage.

### B) Why `run_now_depth` exists
When an event is awaited inside a handler, the event must **jump the queue**. If the runloop continues normally,
it could process unrelated events ("overshoot"), breaking FIFO guarantees.

`run_now_depth` pauses the runloop while we run the awaited event immediately. Once the queue-jump completes,
the runloop resumes in FIFO order. This matches the Python behavior.

### C) BusScopedEvent: why it exists and how it works
Forwarding exposes a subtle bug: if you pass the **same event object** to another bus, a naive implementation
can mutate `event.bus` mid-handler and break parent-child tracking.

To prevent that:
- Handlers always receive a **BusScopedEvent** (Proxy of the original event).
- Its `bus` property is a proxy over the real `EventBus`.
- That proxy intercepts `emit/dispatch` to set `event_parent_id` and attach children to the correct handler.
- The original event object is still the canonical one stored in history.

### D) Cross-bus immediate processing (forwarding + awaiting)
When you `await event.done()` inside a handler:
- the system finds all buses that have this event queued (using `EventBus.instances` + `event_path`)
- pauses their runloops
- processes the event immediately on each bus
- then resumes the runloops

This gives the same "awaited events jump the queue" semantics as Python, but without a global lock.

### E) Why `event.bus` is required for `done()`
`done()` is the signal to run an event immediately when called inside a handler. Without a bus, we can't
perform the queue jump, so `done()` throws if no bus is attached.

## Summary
The core contract is preserved:
- FIFO order
- child event tracking
- forwarding
- await-inside-handler queue jump

But the **implementation details are different** because JS needs browser compatibility and lacks Python’s
contextvars + asyncio primitives. The stack, runloop pause, and BusScopedEvent proxy are the key differences
that make the behavior match in practice.
