# bubus-ts: Python vs JS Differences (and the tricky parts)

This README only covers the differences between the Python implementation and this TypeScript port, plus the
gotchas we uncovered while matching behavior. It intentionally does **not** re-document the full TS API surface.

## Key Differences vs Python

### 1) Awaiting events: `event.done()` instead of `await event`

- Python: `await event` waits for handlers and can jump the queue when awaited inside a handler.
- TS: use `await event.done()` for the same behavior.
- Outside a handler, `done()` just waits for completion (it does not jump the queue).
- Inside a handler, `done()` triggers immediate processing (queue jump) on **all buses** where the event is queued.

### 1b) Racing handlers: `event.first()`

`event.first()` returns the first non-undefined handler result value, then cancels remaining handlers:

```ts
const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', {
  page_id: z.string(),
  event_result_schema: z.string(),
})

class ScreenshotService {
  constructor(bus: InstanceType<typeof EventBus>) {
    bus.on(ScreenshotEvent, this.on_fast.bind(this))
    bus.on(ScreenshotEvent, this.on_slow.bind(this))
  }

  // Fast path: try an immediate screenshot, return undefined if it fails
  async on_fast(event: InstanceType<typeof ScreenshotEvent>): Promise<string | undefined> {
    try {
      return await takeFastScreenshot(event.data.page_id)
    } catch {
      return undefined // signal "I can't handle this"
    }
  }

  // Slow path: retries with global semaphore to avoid VRAM contention
  @retry({ max_attempts: 3, timeout: 15, semaphore_scope: 'global', semaphore_limit: 1, semaphore_name: 'Screenshots' })
  async on_slow(event: InstanceType<typeof ScreenshotEvent>): Promise<string> {
    return await takeFlakySlowScreenshot(event.data.page_id)
  }
}

// Returns first non-undefined result, cancels losing handlers
const screenshot: string | undefined = await bus.emit(ScreenshotEvent({ page_id: 'p1' })).first()
```

**How it works with different concurrency modes:**

- **`parallel`**: All handlers start simultaneously. When one returns a non-undefined value, remaining
  started handlers are aborted (via `signalAbort()`, same mechanism as timeout cancellation) and pending
  handlers are cancelled. Any child events emitted by losing handlers are also cancelled.
- **`serial`**: Handlers run one at a time. After each handler completes, if it
  returned a non-undefined value, remaining handlers are cancelled without being started.

**`event_handler_completion` field:**

Calling `.first()` sets `event.event_handler_completion = 'first'` on the event before processing. This
field is orthogonal to `event_handler_concurrency` (which controls scheduling) — it controls the
**completion strategy**: whether to wait for all handlers (`'all'`, the default) or to stop after the
first non-undefined result (`'first'`).

The field is:

- Part of the event's Zod schema, so it's validated on construction
- Included in `event.toJSON()`, so it's visible in replay logs and serialized event streams
- Settable directly on the event data: `MyEvent({ event_handler_completion: 'first' })`

**Return value semantics:**

- Returns the **temporally first** non-undefined result (not registration order)
- `undefined` means "I don't have a result" — use it to signal pass/skip
- `null`, `0`, `''`, `false` are all valid non-undefined results
- If all handlers return undefined or throw errors, `first()` returns `undefined`

**Compared to `done()`:**

|                            | `done()`                          | `first()`                          |
| -------------------------- | --------------------------------- | ---------------------------------- |
| Waits for                  | All handlers                      | First non-undefined result         |
| Returns                    | `Promise<Event>`                  | `Promise<ResultType \| undefined>` |
| Cancels remaining          | No                                | Yes (abort + cancel descendants)   |
| `event_handler_completion` | `'all'` (default)                 | `'first'`                          |
| Use case                   | Run all handlers, inspect results | Race handlers, take winner         |

Note to run all handlers in parallel but only read the first non-undefined result you can always do:

```typescript
const first_result_from_all = await bus.emit(SomeEvent(...)).done().first_result
```

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
- `event_concurrency?: "global-serial" | "bus-serial" | "parallel" | null` (default: `"bus-serial"`)
  - Controls how many **events** can be processed at a time.
  - `"global-serial"` enforces FIFO across all buses.
  - `"bus-serial"` enforces FIFO per bus, allows cross-bus overlap.
  - `"parallel"` allows events to process concurrently.
  - `null` is treated as "unset" and falls back to the built-in default.
- `event_handler_concurrency?: "serial" | "parallel" | null` (default: `"serial"`)
  - Controls how many **handlers** run at once for each event.
  - `serial` means handlers run one at a time **per event**. Use `@retry({ semaphore_scope: 'global', semaphore_name: '...' })` if you need other locking options across multiple busses or events
  - `null` is treated as "unset" and falls back to the built-in default.
- `event_handler_completion?: "all" | "first"` (default: `"all"`)
  - Controls whether the bus waits for all handlers (`"all"`) or cancels after the first non-undefined result (`"first"`).
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

You can override concurrency per event:

```ts
const FastEvent = BaseEvent.extend('FastEvent', {
  payload: z.string(),
})

// Per-event override (highest precedence)
const event = FastEvent({
  payload: 'x',
  event_concurrency: 'parallel',
  event_handler_concurrency: 'parallel',
})
```

Precedence order (highest → lowest):

1. Event instance overrides (`event_concurrency`, `event_handler_concurrency`)
2. Bus defaults (`event_concurrency`, `event_handler_concurrency`)

`null` resolves to the bus default.

## Handler Options

Handlers can be configured at registration time:

```ts
bus.on(SomeEvent, handler, {
  handler_timeout: 10, // per-handler timeout in seconds
})
```

- `handler_timeout` sets a per-handler timeout in seconds (highest precedence for handlers).
  - Timeout resolution order: `handler_timeout` (bus.on) → `event.event_handler_timeout` → bus `event_timeout` default.
  - The **effective** timeout for a handler is `min(event.event_timeout, handler_timeout)` unless either is `null`.
  - There is no per-handler `event_handler_concurrency` override; use `@retry()` semaphores for fine-grained handler serialization.

## Handler-Level Locks via `@retry`

If you need per-handler serialization (or global locks) without changing event-level concurrency, use `@retry` semaphores:

```ts
const SomeEvent = BaseEvent.extend('SomeEvent', {
  event_handler_concurrency: 'parallel',
})

class Handlers {
  // Serialize these two handlers per event (instance scope + event_id key)
  @retry({ semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: (event) => event.event_id })
  async step1(event: InstanceType<typeof SomeEvent>) {
    console.log(1)
  }

  @retry({ semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: (event) => event.event_id })
  async step2(event: InstanceType<typeof SomeEvent>) {
    console.log(2)
  }

  // This handler remains parallel
  async parallel(event: InstanceType<typeof SomeEvent>) {
    console.log('parallel')
  }
}

const handlers = new Handlers()
bus.on(SomeEvent, handlers.step1.bind(handlers))
bus.on(SomeEvent, handlers.step2.bind(handlers))
bus.on(SomeEvent, handlers.parallel.bind(handlers))
```

Notes:

- `semaphore_name` can be a function; it receives the same arguments as the wrapped function.
- Use `semaphore_scope: 'global'` to serialize across all instances/buses.

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

## Throughput + Memory Behavior (Current)

This section documents the current runtime profile and the important edge cases. It is intentionally conservative:
we describe what is enforced today, not theoretical best-case behavior.

### Throughput model

- Baseline throughput in tests is gated at `<30s` for:
  - `50k events within reasonable time`
  - `50k events with ephemeral on/off handler registration across 2 buses`
  - `500 ephemeral buses with 100 events each`
- The major hot-path operations are linear in collection sizes:
  - Per event, handler matching is `O(total handlers on bus)` (`exact` scan + `*` scan).
  - `.off()` is `O(total handlers on bus)` for matching/removal.
- Queue-jump (`await event.done()` inside handlers) does cross-bus discovery by walking `event_path` (bus labels) and iterating `EventBus._all_instances`, so cost grows with buses and forwarding depth.
- `waitUntilIdle()` is best used at batch boundaries, not per event:
  - Idle checks call `isIdle()`, which scans `event_history` and handler results.
  - There is a fast-path that skips idle scans when no idle waiters exist, which keeps normal dispatch/complete flows fast even with large history.
- Concurrency settings are a direct throughput limiter:
  - `global-serial` / `bus-serial` (events) and `serial` (handlers) intentionally serialize work.
  - `parallel` increases throughput but can increase transient memory if producers outpace consumers.

### Memory model

- Per bus, strong references are held for:
  - `handlers`
  - `pending_event_queue`
  - `in_flight_event_ids`
  - `event_history` (bounded by `max_history_size`, or unbounded if `null`)
  - active `find()` waiters until match/timeout
- Per event, retained state includes:
  - `event_results` (per-handler result objects)
  - descendant links in `event_results[].event_children`
- History trimming behavior:
  - Completed events are evicted first (oldest first).
  - If still over limit, oldest remaining events are dropped even if pending, and a warning is logged.
  - Eviction calls `event._gc()` to clear internal references (`event_results`, child arrays, bus/context pointers).
- Memory is not strictly bounded by only `pending_queue_size + max_history_size`:
  - A retained parent event can hold references to many children/grandchildren via `event_children`.
  - So effective retained memory can exceed a simple `event_count * avg_event_size` bound in high fan-out trees.
- `destroy()` is recommended for deterministic cleanup, but not required for GC safety:
  - `_all_instances` is WeakRef-based, so unreferenced buses can be collected without calling `.destroy()`.
  - There is a GC regression test for this (`unreferenced buses with event history are garbage collected without destroy()`).
- `heapUsed` vs `rss`:
  - `heapUsed` returning near baseline after GC is the primary leak signal in tests.
  - `rss` can stay elevated due to V8 allocator high-water behavior and is not, by itself, a proof of leak.

### Practical guidance for high-load deployments

- Keep `max_history_size` finite in production.
- Avoid very large wildcard handler sets on hot event types.
- Avoid calling `waitUntilIdle()` for every single event in large streams; prefer periodic/batch waits.
- Be aware that very deep/high-fan-out parent-child graphs increase retained memory until parent events are evicted.
- Use `.destroy()` for explicit lifecycle control in request-scoped or short-lived bus patterns.

## Semaphores (how concurrency is enforced)

We use two public semaphores and one per-event handler semaphore:

- `LockManager.global_event_semaphore`
- `bus.locks.bus_event_semaphore`
- per-event handler semaphores (created on demand for each event when `event_handler_concurrency` is `serial`)

They are applied centrally when scheduling events and handlers, so concurrency is controlled without scattering
mutex checks throughout the code.

## Full lifecycle across concurrency modes

Below is the complete execution flow for nested events, including forwarding across buses, and how it behaves
under different `event_concurrency` / `event_handler_concurrency` configurations.

### 1) Base execution flow (applies to all modes)

**Dispatch (non-awaited):**

1. `dispatch()` normalizes to `original_event`, sets `bus` if missing.
2. Captures `_dispatch_context` (AsyncLocalStorage if available).
3. Applies `event_timeout_default` if `event.event_timeout === null`.
4. If this bus is already in `event_path` (or `bus.hasProcessedEvent()`), return a BusScopedEvent without queueing.
5. Append bus label (`name#id`) to `event_path`, record child relationship (if `event_parent_id` is set).
6. Add to `event_history` (a `Map<string, BaseEvent>` keyed by event id).
7. Increment `event_pending_bus_count`.
8. Push to `pending_event_queue` and `startRunloop()`.

**Runloop + processing:**

1. `runloop()` drains `pending_event_queue`.
2. Adds event id to `in_flight_event_ids`.
3. Calls `EventBus.processEvent()` (async).
4. `EventBus.processEvent()` selects the event semaphore and runs `BaseEvent.processEvent()` (the event-level handler runner).
5. `EventBus.processEvent()`:
   - `event.markStarted()`
   - `notifyFindListeners(event)`
   - creates handler results (`event_results`)
   - runs handlers (respecting handler semaphore)
   - decrements `event_pending_bus_count` and calls `event.markCompleted(false)` (completes only if all buses and children are done)

### 2) Event concurrency modes (`event_concurrency`)

- **`global-serial`**: events are serialized across _all_ buses using `LockManager.global_event_semaphore`.
- **`bus-serial`**: events are serialized per bus; different buses can overlap.
- **`parallel`**: no event semaphore; events can run concurrently on the same bus.

**Mixed buses:** each bus enforces its own event mode. Forwarding to another bus does not inherit the source bus’s mode.

### 3) Handler concurrency modes (`event_handler_concurrency`)

`event_handler_concurrency` controls how handlers run **for a single event**:

- **`serial`**: handlers serialize per event.
- **`parallel`**: handlers run concurrently for the event.
- **`null`**: resolves to the bus default.

**Interaction with event concurrency:**
Even if events are parallel, handlers can still be serialized:
`event_concurrency: "parallel"` + `event_handler_concurrency: "serial"` means events start concurrently but handler execution within each event is serialized.

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
3. `processEventImmediately()` removes the event from pending queues on buses that own it.
4. `processEventImmediately()` processes the event immediately on all buses where it is queued.
5. While immediate processing is active, each affected bus's runloop is paused to prevent unrelated events from running.
6. Once immediate processing completes, `processEventImmediately()` **re-acquires** the parent handler's semaphore
   (unless the parent timed out while the child was processing).
7. Paused runloops resume.

**Important:** queue-jump bypasses event semaphores but **respects** handler semaphores via yield-and-reacquire.
This means queue-jumped handlers still serialize **per event** when `event_handler_concurrency` is `serial`.

### 6) Precedence recap

Highest → lowest:

1. Event instance fields (`event_concurrency`, `event_handler_concurrency`)
2. Bus defaults

`null` always resolves to the bus default.

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

The `LockManager` pause mechanism (`requestRunloopPause`/`waitUntilRunloopResumed`) pauses the runloop while we run the awaited
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

- the system finds all buses that have this event queued (using `EventBus._all_instances` + `event_path` labels)
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

---

## `retry()` Decorator

`retry()` adds retry logic and optional semaphore-based concurrency limiting to any async function.

### Why retry is a handler-level concept

Retry and timeout belong on the **handler**, not on `emit()` or `done()`:

- **Handlers fail, events don't.** An event has no error state — it's a message. Individual handlers
  produce errors, timeouts, and exceptions that may need retrying. The handler knows _why_ it failed
  and whether retrying makes sense.

- **Replayability.** When you replay an event log, each emit should produce exactly one event. If retry
  lives on the handler, the log records one emit → one handler invocation → one result. The retry
  attempts are invisible implementation details. If retry lives on `emit()`, the log contains multiple
  separate events for the same logical operation, making replays non-deterministic.

- **Separation of concerns.** Event-level concurrency (`event_concurrency`) and handler-level concurrency
  (`event_handler_concurrency`) are bus-level scheduling concerns. Retry/timeout/semaphore limiting are
  handler-level resilience concerns. They compose orthogonally — don't mix them.

### Recommended pattern: `@retry()` on class methods

```ts
import { retry, EventBus, BaseEvent } from 'bubus'

class ScreenshotService {
  constructor(private bus: InstanceType<typeof EventBus>) {
    bus.on(ScreenshotRequestEvent, this.on_ScreenshotRequest.bind(this))
  }

  @retry({
    max_attempts: 4,
    retry_on_errors: [/timeout/i],
    timeout: 5,
    semaphore_scope: 'global',
    semaphore_name: 'Screenshots',
    semaphore_limit: 2,
  })
  async on_ScreenshotRequest(event: InstanceType<typeof ScreenshotRequestEvent>): Promise<Buffer> {
    // At most 2 concurrent screenshot operations globally.
    // Each attempt times out after 5s. Up to 4 total attempts.
    // Only retries on timeout-related errors.
    return await takeScreenshot(event.data.url)
  }
}

// Emit side stays clean — no retry/timeout concerns
const event = bus.emit(ScreenshotRequestEvent({ url: 'https://example.com' }))
await event.done()
```

This is the primary supported pattern. The `@retry()` decorator handles:

- **Retry logic**: max attempts, backoff, error filtering
- **Per-attempt timeout**: each attempt gets its own deadline
- **Concurrency limiting**: semaphore-based, with global/class/instance scoping

The emit site just dispatches events and awaits completion — it doesn't know or care about retries.

### Also works: inline HOF for simple handlers

```ts
// For one-off handlers that don't need a class
bus.on(
  MyEvent,
  retry({ max_attempts: 3, timeout: 10 })(async (event) => {
    await riskyOperation(event.data)
  })
)
```

### Options

| Option                 | Type                                      | Default     | Description                                                                                                                                                                                                                                                                   |
| ---------------------- | ----------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_attempts`         | `number`                                  | `1`         | Total attempts including the initial call. `1` = no retry, `3` = up to 2 retries.                                                                                                                                                                                             |
| `retry_after`          | `number`                                  | `0`         | Seconds to wait between retries.                                                                                                                                                                                                                                              |
| `retry_backoff_factor` | `number`                                  | `1.0`       | Multiplier applied to `retry_after` after each attempt. `2.0` = exponential backoff.                                                                                                                                                                                          |
| `retry_on_errors`      | `(ErrorClass \| string \| RegExp)[]`      | `undefined` | Only retry when the error matches a matcher. Accepts class constructors (`instanceof`), strings (matched against `error.name`), or RegExp (tested against `String(error)`). Can be mixed: `[TypeError, 'NetworkError', /timeout/i]`. `undefined` = retry on any error.        |
| `timeout`              | `number \| null`                          | `undefined` | Per-attempt timeout in seconds. Throws `RetryTimeoutError` if exceeded.                                                                                                                                                                                                       |
| `semaphore_limit`      | `number \| null`                          | `undefined` | Max concurrent executions sharing this semaphore.                                                                                                                                                                                                                             |
| `semaphore_name`       | `string \| ((...args) => string) \| null` | fn name     | Semaphore identifier. Functions with the same name share the same slot pool. If a function is provided, it receives the same arguments as the wrapped function.                                                                                                               |
| `semaphore_lax`        | `boolean`                                 | `true`      | If `true`, proceed without concurrency limit when semaphore acquisition times out.                                                                                                                                                                                            |
| `semaphore_scope`      | `'global' \| 'class' \| 'instance'`       | `'global'`  | `'global'`: one semaphore for all calls. `'class'`: one per class (keyed by `constructor.name`). `'instance'`: one per object instance (keyed by WeakMap identity). `'class'`/`'instance'` require `this` to be an object; they fall back to `'global'` for standalone calls. |
| `semaphore_timeout`    | `number \| null`                          | `undefined` | Max seconds to wait for semaphore. Default: `timeout * max(1, limit - 1)`.                                                                                                                                                                                                    |

### Error types

- **`RetryTimeoutError`** — thrown when a single attempt exceeds `timeout`. Has `.timeout_seconds` and `.attempt` fields. Retryable by default (treated like any other error in the retry loop).
- **`SemaphoreTimeoutError`** — thrown (when `semaphore_lax=false`) if the semaphore cannot be acquired within the timeout. Has `.semaphore_name`, `.semaphore_limit`, `.timeout_seconds` fields.

### Semaphore concurrency control

The semaphore is acquired **once** before the first attempt and held across all retries. This prevents other
callers from stealing the slot between retry attempts.

**Timeout interaction with event handlers:** if a handler uses `@retry({ timeout })` and the retry times out,
the thrown `RetryTimeoutError` is treated like a handler timeout inside the event bus. It is wrapped as an
`EventHandlerTimeoutError`, and pending descendants are cancelled just like a normal handler timeout.
If a **handler-wide** timeout fires while retries are still in progress, the bus marks the handler as timed out
and cancels descendants immediately; the in-flight attempt(s) may still finish in the background, but their
results are ignored (JS cannot preempt an async function).

```ts
class ApiService {
  @retry({
    max_attempts: 2,
    semaphore_limit: 3,
    semaphore_name: 'api_calls',
  })
  async callExternalApi(): Promise<Response> {
    // At most 3 concurrent calls across all instances of ApiService
    return await fetch('https://api.example.com')
  }
}
```

Functions that share a `semaphore_name` share the same slot pool — this is how you limit concurrency across
different functions that access the same resource.

### Re-entrancy and deadlock prevention

The decorator uses `AsyncLocalStorage` (on Node.js) to track which semaphores are held in the current async
call stack. When a nested call encounters a semaphore it already holds, it **skips acquisition** and runs
directly within the parent's slot. This prevents deadlocks in recursive or nested scenarios:

```ts
const inner = retry({ semaphore_limit: 1, semaphore_name: 'shared' })(async () => 'ok')

const outer = retry({ semaphore_limit: 1, semaphore_name: 'shared' })(async () => {
  // Without re-entrancy tracking, this would deadlock:
  // outer holds the semaphore, inner tries to acquire the same one.
  // With re-entrancy, inner detects 'shared' is already held and skips acquisition.
  return await inner()
})

await outer() // works, no deadlock
```

This also works for recursive calls (a function calling itself) and deeply nested chains (A → B → C all sharing
a semaphore).

In browsers (no `AsyncLocalStorage`), re-entrancy tracking is unavailable and the decorator gracefully degrades
to a no-op (no deadlock detection). Avoid recursive/nested calls through the same semaphore in browser
environments, or use different `semaphore_name` values.

### Interaction with bus concurrency options

`retry()` and the bus's concurrency modes are **orthogonal** and compose together:

- **`event_concurrency`** controls how many events the bus processes at once (via the runloop + event semaphore).
- **`event_handler_concurrency`** controls how many handlers run concurrently for a single event (via the handler semaphore).
- **`retry()` semaphores** control how many concurrent invocations of a specific handler are allowed (via a global semaphore registry).

These are separate concerns:

- Bus concurrency = scheduling (how the bus orders event/handler execution)
- Retry semaphores = resilience (how individual handlers manage concurrency and failure recovery)

When you use `@retry()` on a bus handler, both layers apply. The execution order is:

1. Bus acquires the **handler concurrency semaphore** (e.g. `serial`)
2. `retry()` acquires its own **retry semaphore** (if `semaphore_limit` is set)
3. The handler function runs (with retries if it throws)
4. `retry()` releases its semaphore
5. Bus releases the handler concurrency semaphore

The bus's `handler_timeout` and `retry()`'s `timeout` are independent:

- `handler_timeout` (set via `bus.on()` options, `event.event_handler_timeout`, or bus defaults) applies to the **entire** wrapped handler call, including all retry attempts.
- `retry({ timeout })` applies to **each individual attempt**.

If you need per-attempt timeouts, use `retry({ timeout })`. If you need an overall deadline for the handler
(including all retries), rely on the bus's `handler_timeout`.

### Discouraged: wrapping `emit()` → `done()` in `retry()`

This pattern is technically supported but **not recommended**:

```ts
// DON'T DO THIS — retry belongs on the handler, not the emit site.
const event = await retry({ max_attempts: 4 })(async () => {
  const ev = bus.emit(ScreenshotRequestEvent({ full_page: false }))
  await ev.done()
  if (ev.event_errors.length) throw ev.event_errors[0]
  return ev
})()
```

Why this is worse:

1. **Architecture**: the emit site doesn't know which handler failed or why. The handler is the right
   place for retry logic because it has the context to decide whether retrying makes sense.

2. **Replayability**: each retry dispatches a **new event**, producing multiple events in the log for
   one logical operation. On replay, if the handler succeeds on the first attempt, you get a different
   event topology than the original run. With handler-level retry, the log always shows one emit → one
   handler result, regardless of how many retry attempts were needed internally.

3. **Determinism**: the same emit may fan out to multiple handlers. Retrying the whole dispatch because
   one handler failed also re-runs handlers that succeeded — wasteful and potentially side-effectful.

Use the `@retry()` decorator on the handler method instead.

### Differences from the Python `@retry` decorator

| Aspect               | Python                                                           | TypeScript                                                                        |
| -------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| **Naming**           | `retries=3` (retry count after first attempt)                    | `max_attempts=1` (total attempts including first)                                 |
| **Naming**           | `wait=3` (seconds between retries)                               | `retry_after=0` (seconds between retries)                                         |
| **Naming**           | `retry_on`                                                       | `retry_on_errors`                                                                 |
| **Default retries**  | 3 retries (4 total attempts)                                     | 1 attempt (no retries)                                                            |
| **Default delay**    | 3 seconds                                                        | 0 seconds                                                                         |
| **Default timeout**  | 5 seconds per attempt                                            | No timeout                                                                        |
| **Semaphore scopes** | `'global'`, `'class'`, `'self'`, `'multiprocess'`                | `'global'`, `'class'`, `'instance'` (no multiprocess — single-process JS runtime) |
| **System overload**  | Tracks active operations, checks CPU/memory via `psutil`         | Not implemented                                                                   |
| **Re-entrancy**      | Not implemented (relies on Python's GIL + asyncio single-thread) | `AsyncLocalStorage`-based tracking to prevent deadlocks                           |
| **Syntax**           | `@retry(...)` decorator on `async def`                           | `@retry({...})` on class methods (TC39 Stage 3), or `retry({...})(fn)` HOF        |
| **Sync functions**   | Not supported (async-only)                                       | Supported (wrapper always returns a Promise)                                      |

The TS version intentionally starts with conservative defaults (1 attempt, no delay, no timeout) so that
`retry()` with no options is a no-op wrapper. The Python version defaults to 3 retries with 3s delay and 5s
timeout, which is more aggressive.

## Runtimes

`bubus-ts` supports:

- Node.js (default development and test runtime)
- Bun
- Deno
- Browsers (ESM)

### Runtime support notes

- The package output is ESM (`dist/esm`) and works across Node/Bun/Deno.
- `AsyncLocalStorage` is used when available (Node/Bun) and gracefully disabled when unavailable (for example in browsers).
- Browser usage is supported for core event bus features; Node-specific tooling scripts (`pnpm test`, Node test runner flags) are not used in browser environments.

### Performance comparison (local run, per-event)

Measured locally on an `Apple M4 Pro` with:

- `pnpm run perf:node` (`node v22.21.1`)
- `pnpm run perf:bun` (`bun v1.3.9`)
- `pnpm run perf:deno` (`deno v2.6.8`)
- `pnpm run perf:browser` (`chrome v145.0.7632.6`)

| Runtime            | 1 bus x 50k events x 1 handler | 500 busses x 100 events x 1 handler | 1 bus x 1 event x 50k parallel handlers | 1 bus x 50k events x 50k one-off handlers | Worst case (N busses x N events x N handlers) |
| ------------------ | ------------------------------ | ----------------------------------- | -------------------------------------- | ----------------------------------------- | --------------------------------------------- |
| Node               | `0.014ms/event`, `1.1kb/event` | `0.059ms/event`, `0.0kb/event`      | `1023.501ms/event`, `103120.0kb/event` | `0.029ms/event`, `0.0kb/event`            | `6.176ms/event`, `0.2kb/event`                |
| Bun                | `0.014ms/event`, `2.9kb/event` | `0.067ms/event`, `0.1kb/event`      | `99.819ms/event`, `142816.0kb/event`   | `0.030ms/event`, `0.6kb/event`            | `6.396ms/event`, `0.2kb/event`                |
| Deno               | `0.019ms/event`, `1.9kb/event` | `0.075ms/event`, `0.0kb/event`      | `1164.815ms/event`, `44896.0kb/event`  | `0.068ms/event`, `0.1kb/event`            | `6.726ms/event`, `0.1kb/event`                |
| Browser (Chromium) | `0.032ms/event`, `n/a`         | `0.203ms/event`, `n/a`              | `919.600ms/event`, `n/a`               | `0.023ms/event`, `n/a`                    | `6.117ms/event`, `n/a`                        |

Notes:

- `kb/event` is the peak RSS delta per event during each scenario.
- Browser runtime does not expose process RSS from page JS, so memory-per-event is `n/a`.
- For `Worst case (N busses x N events x N handlers)`, per-event values are normalized by `500 iterations * 3 logical events`.
- All four runtime suites currently pass (`node`, `bun`, `deno`, and browser/Chromium via Playwright).
