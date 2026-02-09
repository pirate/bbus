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
- `event_handler_concurrency?: "global-serial" | "bus-serial" | "parallel" | "auto"` (default: `"bus-serial"`)
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
  event_handler_concurrency: 'parallel',
})

// Per-handler override (lower precedence)
bus.on(FastEvent, handler, { event_handler_concurrency: 'parallel' })
```

Precedence order (highest → lowest):

1. Event instance overrides (`event_concurrency`, `event_handler_concurrency`)
2. Handler options (`event_handler_concurrency`)
3. Bus defaults (`event_concurrency`, `event_handler_concurrency`)

`"auto"` resolves to the bus default.

## Handler Options

Handlers can be configured at registration time:

```ts
bus.on(SomeEvent, handler, {
  event_handler_concurrency: 'parallel',
  handler_timeout: 10, // per-handler timeout in seconds
})
```

- `event_handler_concurrency` allows per-handler concurrency overrides.
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
  - Queue-jump (`await event.done()` inside handlers) does cross-bus discovery by walking `event_path` and iterating `EventBus._all_instances`, so cost grows with buses and forwarding depth.
- `waitUntilIdle()` is best used at batch boundaries, not per event:
  - Idle checks call `isIdle()`, which scans `event_history` and handler results.
  - There is a fast-path that skips idle scans when no idle waiters exist, which keeps normal dispatch/complete flows fast even with large history.
- Concurrency settings are a direct throughput limiter:
  - `global-serial` and `bus-serial` intentionally serialize work.
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

We use four semaphores:

- `LockManager.global_event_semaphore`
- `LockManager.global_handler_semaphore`
- `bus.locks.bus_event_semaphore`
- `bus.locks.bus_handler_semaphore`

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

### 3) Handler concurrency modes (`event_handler_concurrency`)

`event_handler_concurrency` controls how handlers run **for a single event**:

- **`global-serial`**: only one handler at a time across all buses using `LockManager.global_handler_semaphore`.
- **`bus-serial`**: handlers serialize per bus.
- **`parallel`**: handlers run concurrently for the event.
- **`auto`**: resolves to the bus default.

**Interaction with event concurrency:**
Even if events are parallel, handlers can still be serialized:
`event_concurrency: "parallel"` + `event_handler_concurrency: "bus-serial"` means events start concurrently but handler execution on a bus is serialized.

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

1. Event instance fields (`event_concurrency`, `event_handler_concurrency`)
2. Handler options (`event_handler_concurrency`)
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

---

## `retry()` Decorator

`retry()` is a standalone higher-order function / decorator that adds retry logic and optional semaphore-based
concurrency limiting to any async function. It works independently of the event bus — you can use it on plain
functions, class methods, or event bus handlers.

### Basic usage

```ts
import { retry } from 'bubus'

// Higher-order function wrapper (works on any function)
const fetchWithRetry = retry({ max_attempts: 3, retry_after: 1 })(async (url: string) => {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
})

// On an event bus handler
bus.on(MyEvent, retry({ max_attempts: 3, timeout: 10 })(async (event) => {
  await riskyOperation(event.data)
}))

// On a class method (manual wrapping pattern)
class ApiClient {
  fetchData = retry({ max_attempts: 3, retry_after: 0.5 })(async function (this: ApiClient) {
    return await this.doRequest()
  })
}
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_attempts` | `number` | `1` | Total attempts including the initial call. `1` = no retry, `3` = up to 2 retries. |
| `retry_after` | `number` | `0` | Seconds to wait between retries. |
| `retry_backoff_factor` | `number` | `1.0` | Multiplier applied to `retry_after` after each attempt. `2.0` = exponential backoff. |
| `retry_on_errors` | `(ErrorClass \| string \| RegExp)[]` | `undefined` | Only retry when the error matches a matcher. Accepts class constructors (`instanceof`), strings (matched against `error.name`), or RegExp (tested against `String(error)`). Can be mixed: `[TypeError, 'NetworkError', /timeout/i]`. `undefined` = retry on any error. |
| `timeout` | `number \| null` | `undefined` | Per-attempt timeout in seconds. Throws `RetryTimeoutError` if exceeded. |
| `semaphore_limit` | `number \| null` | `undefined` | Max concurrent executions sharing this semaphore. |
| `semaphore_name` | `string \| null` | fn name | Semaphore identifier. Functions with the same name share the same slot pool. |
| `semaphore_lax` | `boolean` | `true` | If `true`, proceed without concurrency limit when semaphore acquisition times out. |
| `semaphore_scope` | `'global' \| 'class' \| 'instance'` | `'global'` | `'global'`: one semaphore for all calls. `'class'`: one per class (keyed by `constructor.name`). `'instance'`: one per object instance (keyed by WeakMap identity). `'class'`/`'instance'` require `this` to be an object; they fall back to `'global'` for standalone calls. |
| `semaphore_timeout` | `number \| null` | `undefined` | Max seconds to wait for semaphore. Default: `timeout * max(1, limit - 1)`. |

### Error types

- **`RetryTimeoutError`** — thrown when a single attempt exceeds `timeout`. Has `.timeout_seconds` and `.attempt` fields. Retryable by default (treated like any other error in the retry loop).
- **`SemaphoreTimeoutError`** — thrown (when `semaphore_lax=false`) if the semaphore cannot be acquired within the timeout. Has `.semaphore_name`, `.semaphore_limit`, `.timeout_seconds` fields.

### Semaphore concurrency control

The semaphore is acquired **once** before the first attempt and held across all retries. This prevents other
callers from stealing the slot between retry attempts.

```ts
// At most 3 concurrent calls to this function across the entire process
const limited = retry({
  max_attempts: 2,
  semaphore_limit: 3,
  semaphore_name: 'api_calls',
})(async () => {
  await callExternalApi()
})
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

### Interaction with `event_concurrency` and `event_handler_concurrency`

`retry()` and the bus's concurrency modes are **orthogonal** and compose together:

- **`event_concurrency`** controls how many events the bus processes at once (via the runloop + event semaphore).
- **`event_handler_concurrency`** controls how many handlers run concurrently for a single event (via the handler semaphore).
- **`retry()` semaphores** control how many concurrent invocations of a specific function are allowed (via a global semaphore registry).

When you wrap an event handler with `retry()`, both layers apply:

```ts
// Bus enforces bus-serial handler ordering (default).
// retry() additionally limits this specific handler to 2 concurrent invocations
// and retries up to 3 times on failure.
bus.on(
  MyEvent,
  retry({ max_attempts: 3, semaphore_limit: 2, semaphore_name: 'my_handler' })(
    async (event) => { await doWork(event) }
  )
)
```

The execution order is:
1. Bus acquires the **handler concurrency semaphore** (e.g. `bus-serial`)
2. `retry()` acquires its own **retry semaphore** (if `semaphore_limit` is set)
3. The handler function runs (with retries if it throws)
4. `retry()` releases its semaphore
5. Bus releases the handler concurrency semaphore

The bus's `handler_timeout` and `retry()`'s `timeout` are independent:
- `handler_timeout` (set via `bus.on()` options or bus defaults) applies to the **entire** wrapped handler call, including all retry attempts.
- `retry({ timeout })` applies to **each individual attempt**.

If you need per-attempt timeouts, use `retry({ timeout })`. If you need an overall deadline for the handler
(including all retries), rely on the bus's `handler_timeout`.

### Differences from the Python `@retry` decorator

| Aspect | Python | TypeScript |
|--------|--------|------------|
| **Naming** | `retries=3` (retry count after first attempt) | `max_attempts=1` (total attempts including first) |
| **Naming** | `wait=3` (seconds between retries) | `retry_after=0` (seconds between retries) |
| **Naming** | `retry_on` | `retry_on_errors` |
| **Default retries** | 3 retries (4 total attempts) | 1 attempt (no retries) |
| **Default delay** | 3 seconds | 0 seconds |
| **Default timeout** | 5 seconds per attempt | No timeout |
| **Semaphore scopes** | `'global'`, `'class'`, `'self'`, `'multiprocess'` | `'global'`, `'class'`, `'instance'` (no multiprocess — single-process JS runtime) |
| **System overload** | Tracks active operations, checks CPU/memory via `psutil` | Not implemented |
| **Re-entrancy** | Not implemented (relies on Python's GIL + asyncio single-thread) | `AsyncLocalStorage`-based tracking to prevent deadlocks |
| **Syntax** | `@retry(...)` decorator on `async def` | `retry({...})(fn)` HOF or `@retry({...})` on class methods (TC39 Stage 3) |
| **Sync functions** | Not supported (async-only) | Supported (wrapper always returns a Promise) |

The TS version intentionally starts with conservative defaults (1 attempt, no delay, no timeout) so that
`retry()` with no options is a no-op wrapper. The Python version defaults to 3 retries with 3s delay and 5s
timeout, which is more aggressive.
