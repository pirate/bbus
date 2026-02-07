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
- TS optionally uses `AsyncLocalStorage` on Node.js (auto-detected) to capture dispatch context, but falls back gracefully in browsers.
- `EventBus._all_instances` + the `LockManager` pause mechanism pauses each runloop and processes the same event immediately across buses.

### 3) `event.bus` is a BusScopedEvent view

- In Python, `event.event_bus` is dynamic (contextvars).
- In TS, `event.bus` is provided by a **BusScopedEvent** (a Proxy over the original event).
- That proxy injects a bus-bound `emit/dispatch` to ensure correct parent/child tracking.

### 4) Monotonic timestamps

- JS `Date.now()` is not strictly monotonic at millisecond granularity.
- To keep FIFO tests stable, we generate strictly increasing timestamps via `BaseEvent.nextTimestamp()` (returns `{ date, isostring, ts }`).

### 5) No middleware, no WAL, no SQLite mirrors

- Those Python features were intentionally dropped for the JS version.

### 6) Default timeouts come from the EventBus

- `BaseEvent.event_timeout` defaults to `null`.
- When dispatched, `EventBus` applies its default `event_timeout` (60s unless configured).
- You can set `{ event_timeout: null }` on the bus to disable timeouts entirely.
- Slow handler warnings fire after `event_handler_slow_timeout` (default: `30s`). Slow event warnings fire after `event_slow_timeout` (default: `300s`).

## EventBus Options

All options are passed to `new EventBus(name, options)`.

- `max_history_size?: number | null` (default: `100`)
  - Max number of events kept in history. Set to `null` for unlimited history.
- `event_concurrency?: "global-serial" | "bus-serial" | "parallel" | "auto"` (default: `"bus-serial"`)
  - Controls how many **events** can be processed at a time.
  - `"global-serial"` enforces FIFO across all buses.
  - `"bus-serial"` enforces FIFO per bus, allows cross-bus overlap.
  - `"parallel"` allows events to process concurrently.
  - `"auto"` uses the bus default (mostly useful for overrides).
- `handler_concurrency?: "global-serial" | "bus-serial" | "parallel" | "auto"` (default: `"bus-serial"`)
  - Controls how many **handlers** run at once for each event.
  - Same semantics as `event_concurrency`, but applied to handler execution.
- `event_timeout?: number | null` (default: `60`)
  - Default handler timeout in seconds, applied when `event.event_timeout` is `null`.
  - Set to `null` to disable timeouts globally for the bus.
- `event_handler_slow_timeout?: number | null` (default: `30`)
  - Warn after this many seconds for slow handlers.
  - Only warns when the handler's timeout is `null` or greater than this value.
  - Set to `null` to disable slow handler warnings.
- `event_slow_timeout?: number | null` (default: `300`)
  - Warn after this many seconds for slow event processing.
  - Set to `null` to disable slow event warnings.

## Concurrency Overrides and Precedence

You can override concurrency per event and per handler:

```ts
const FastEvent = BaseEvent.extend('FastEvent', {
  payload: z.string(),
})

// Per-event override (highest precedence)
const event = FastEvent({
  payload: 'x',
  event_concurrency: 'parallel',
  handler_concurrency: 'parallel',
})

// Per-handler override (lower precedence)
bus.on(FastEvent, handler, { handler_concurrency: 'parallel' })
```

Precedence order (highest → lowest):

1. Event instance overrides (`event_concurrency`, `handler_concurrency`)
2. Handler options (`handler_concurrency`)
3. Bus defaults (`event_concurrency`, `handler_concurrency`)

`"auto"` resolves to the bus default.

## Handler Options

Handlers can be configured with `HandlerOptions`:

```ts
bus.on(SomeEvent, handler, {
  handler_concurrency: 'parallel',
  handler_timeout: 10, // per-handler timeout in seconds
})
```

- `handler_concurrency` allows per-handler concurrency overrides.
- `handler_timeout` sets a per-handler timeout in seconds (overrides the bus default when lower).

## TypeScript Return Type Enforcement (Edge Cases)

TypeScript can only enforce handler return types when the event type is inferable at compile time.

- `bus.on(EventFactoryOrClass, handler)`:
  - Return values are type-checked against the event's `event_result_schema` (if defined).
  - `undefined` (or no return) is always allowed.
- `bus.on('SomeEventName', handler)`:
  - Return type checking is best-effort only (treated as unknown in typing).
  - Use class/factory keys when you want compile-time return-shape enforcement.
- `bus.on('*', handler)`:
  - Return type checking is intentionally loose (best-effort only), because wildcard handlers may receive many event types, including forwarded events from other buses.
  - In practice, wildcard handlers are expected to be side-effect/forwarding handlers and usually return `undefined`.

Runtime behavior is still consistent across all key styles:

- If an event has `event_result_schema` and a handler returns a non-`undefined` value, that value is validated at runtime.
- If the handler returns `undefined`, schema validation is skipped and the result is accepted.

## Semaphores (how concurrency is enforced)

We use four semaphores:

- `LockManager.global_event_semaphore`
- `LockManager.global_handler_semaphore`
- `bus.locks.bus_event_semaphore`
- `bus.locks.bus_handler_semaphore`

They are applied centrally when scheduling events and handlers, so concurrency is controlled without scattering
mutex checks throughout the code.

## Full lifecycle across concurrency modes

Below is the complete execution flow for nested events, including forwarding across buses, and how it behaves
under different `event_concurrency` / `handler_concurrency` configurations.

### 1) Base execution flow (applies to all modes)

**Dispatch (non-awaited):**

1. `dispatch()` normalizes to `original_event`, sets `bus` if missing.
2. Captures `_dispatch_context` (AsyncLocalStorage if available).
3. Applies `event_timeout_default` if `event.event_timeout === null`.
4. If this bus is already in `event_path` (or `bus.hasProcessedEvent()`), return a BusScopedEvent without queueing.
5. Append bus name to `event_path`, record child relationship (if `event_parent_id` is set).
6. Add to `event_history` (a `Map<string, BaseEvent>` keyed by event id).
7. Increment `event_pending_bus_count`.
8. Push to `pending_event_queue` and `startRunloop()`.

**Runloop + processing:**

1. `runloop()` drains `pending_event_queue`.
2. Adds event id to `in_flight_event_ids`.
3. Calls `scheduleEventProcessing()` (async).
4. `scheduleEventProcessing()` selects the event semaphore and runs `processEvent()`.
5. `processEvent()`:
   - `event.markStarted()`
   - `notifyFindListeners(event)`
   - creates handler results (`event_results`)
   - runs handlers (respecting handler semaphore)
   - decrements `event_pending_bus_count` and calls `event.markCompleted(false)` (completes only if all buses and children are done)

### 2) Event concurrency modes (`event_concurrency`)

- **`global-serial`**: events are serialized across _all_ buses using `LockManager.global_event_semaphore`.
- **`bus-serial`**: events are serialized per bus; different buses can overlap.
- **`parallel`**: no event semaphore; events can run concurrently on the same bus.
- **`auto`**: resolves to the bus default.

**Mixed buses:** each bus enforces its own event mode. Forwarding to another bus does not inherit the source bus’s mode.

### 3) Handler concurrency modes (`handler_concurrency`)

`handler_concurrency` controls how handlers run **for a single event**:

- **`global-serial`**: only one handler at a time across all buses using `LockManager.global_handler_semaphore`.
- **`bus-serial`**: handlers serialize per bus.
- **`parallel`**: handlers run concurrently for the event.
- **`auto`**: resolves to the bus default.

**Interaction with event concurrency:**
Even if events are parallel, handlers can still be serialized:
`event_concurrency: "parallel"` + `handler_concurrency: "bus-serial"` means events start concurrently but handler execution on a bus is serialized.

### 4) Forwarding across buses (non-awaited)

When a handler on Bus A calls `bus_b.dispatch(event)` without awaiting:

- Bus A continues running its handler.
- Bus B queues and processes the event according to **Bus B’s** concurrency settings.
- No coupling unless both buses use the global semaphores.

### 5) Queue-jump (`await event.done()` inside handlers)

When `event.done()` is awaited inside a handler, **queue-jump** happens:

1. `BaseEvent.done()` delegates to `bus.processEventImmediately()`, which detects whether we're inside a handler
   (via `getActiveHandlerResult()` / `getParentEventResultAcrossAllBusses()`). If not inside a handler, it falls back to `waitForCompletion()`.
2. `processEventImmediately()` **yields** the parent handler's concurrency semaphore (if held) so child handlers can acquire it.
3. `processEventImmediately()` removes the event from the pending queue (if present).
4. `runImmediatelyAcrossBuses()` processes the event immediately on all buses where it is queued.
5. While immediate processing is active, each affected bus's runloop is paused to prevent unrelated events from running.
6. Once immediate processing completes, `processEventImmediately()` **re-acquires** the parent handler's semaphore
   (unless the parent timed out while the child was processing).
7. Paused runloops resume.

**Important:** queue-jump bypasses event semaphores but **respects** handler semaphores via yield-and-reacquire.
This means queue-jumped handlers run serially on a `bus-serial` bus, not in parallel.

### 6) Precedence recap

Highest → lowest:

1. Event instance fields (`event_concurrency`, `handler_concurrency`)
2. Handler options (`handler_concurrency`)
3. Bus defaults

`"auto"` always resolves to the bus default.

## Gotchas and Design Choices (What surprised us)

### A) Handler attribution without AsyncLocalStorage

We need to know **which handler emitted a child** to correctly assign:

- `event_parent_id`
- `event_emitted_by_handler_id`
- and to attach child events under the correct handler in the tree.

In TS we do this by injecting a **BusScopedEvent** into handlers, which captures the active handler id and
propagates it via `event_emitted_by_handler_id`. This keeps parentage deterministic even with nested awaits.

### B) Why runloop pausing exists

When an event is awaited inside a handler, the event must **jump the queue**. If the runloop continues normally,
it could process unrelated events ("overshoot"), breaking FIFO guarantees.

The `LockManager` pause mechanism (`requestPause`/`waitUntilRunloopResumed`) pauses the runloop while we run the awaited
event immediately. Once the queue-jump completes, the runloop resumes in FIFO order. This matches the Python behavior.

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

- the system finds all buses that have this event queued (using `EventBus._all_instances` + `event_path`)
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

But the **implementation details are different** because JS needs browser compatibility and lacks Python's
contextvars + asyncio primitives. The `LockManager` (runloop pause + semaphore coordination), `HandlerLock`
(yield-and-reacquire), and `BusScopedEvent` proxy are the key differences that make the behavior match in practice.
