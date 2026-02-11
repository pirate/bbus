# `bubus`: 📢 Production-ready multi-language event bus

<img width="200" alt="image" src="https://github.com/user-attachments/assets/b3525c24-51ba-496c-b327-ccdfe46a7362" align="right" />

[![DeepWiki: Python](https://img.shields.io/badge/DeepWiki-bbus%2FPython-yellow.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/pirate/bbus) ![PyPI - Version](https://img.shields.io/pypi/v/bubus) ![GitHub License](https://img.shields.io/github/license/pirate/bbus) ![GitHub last commit](https://img.shields.io/github/last-commit/pirate/bbus)

[![DeepWiki: TS](https://img.shields.io/badge/DeepWiki-bbus%2FTypescript-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/pirate/bbus/3-typescript-implementation) ![NPM Version](https://img.shields.io/npm/v/bubus)

Bubus is an in-memory event bus library for async Python and TS (node/bun/deno/browser).

It's designed for quickly building resilient, predictable, complex event-driven apps.

It "just works" with an intuitive, but powerful event JSON format + dispatch API that's consistent across both languages and scales consistently from one event up to millions:

```python
bus.on(SomeEvent, some_function)
bus.emit(SomeEvent({some_data: 132}))
```

It's async native, has proper automatic nested event tracking, and powerful concurrency control options. The API is inspired by `EventEmitter` or [`emittery`](https://github.com/sindresorhus/emittery) in JS, but it takes it a step further:

- nice Zod / Pydantic schemas for events that can be exchanged between both languages
- automatic UUIDv7s and monotonic nanosecond timestamps for ordering events globally
- built in locking options to force strict global FIFO procesing or fully parallel processing

---

♾️ It's inspired by the simplicity of async and events in `JS` but with baked-in features that allow to eliminate most of the tedious repetitive complexity in event-driven codebases:

- correct timeout enforcement across multiple levels of events, if a parent times out it correctly aborts all child event processing
- ability to strongly type hint and enforce the return type of event handlers at compile-time
- ability to queue events on the bus, or inline await them for immediate execution like a normal function call
- handles ~5,000 events/sec/core in both languages, with ~2kb/event RAM consumed per event during active processing

<br/>

## 🔢 Quickstart

```bash
npm install bubus
```

```ts
import { BaseEvent, EventBus } from 'bubus'
import { z } from 'zod'

const CreateUserEvent = BaseEvent.extend('CreateUserEvent', {
  email: z.string(),
  event_result_schema: z.object({ user_id: z.string() }),
})

const bus = new EventBus('MyAuthEventBus')

bus.on(CreateUserEvent, async (event) => {
  const user = await yourCreateUserLogic(event.email)
  return { user_id: user.id }
})

const event = bus.emit(CreateUserEvent({ email: 'someuser@example.com' }))
await event.done()
console.log(event.first_result) // { user_id: 'some-user-uuid' }
```

<br/>

---

<br/>

## ✨ Features

The features offered in TS are broadly similar to the ones offered in the python library.

- Typed events with Zod schemas (cross-compatible with Pydantic events from python library)
- FIFO event queueing with configurable concurrency
- Nested event support with automatic parent/child tracking
- Cross-bus forwarding with loop prevention
- Handler result tracking + validation + timeout enforcement
- History retention controls (`max_history_size`) for memory bounds
- Optional `@retry` decorator for easy management of per-handler retries, timeouts, and semaphore-limited execution

See the [Python README](../README.md) for more details.

<br/>

---

<br/>

## 📚 API Documentation

### `EventBus`

Create a bus:

```ts
const bus = new EventBus('MyBus', {
  max_history_size: 100, // keep small, copy events to external store manually if you want to persist/query long-term logs
  event_concurrency: 'bus-serial', // 'global-serial' | 'bus-serial' (default) | 'parallel'
  event_handler_concurrency: 'serial', // 'serial' (default) | 'parallel'
  event_handler_completion: 'all', // 'all' (default) | 'first' (stop handlers after the first non-undefined result from any handler)
  event_timeout: 60, // default hard timeout for event handlers before they are marked result.status = 'error' w/ result.error = HandlerTimeoutError(...)
  event_handler_slow_timeout: 30, // default timeout before a console.warn("Slow event handler bus.on(SomeEvent, someHandler()) has taken more than 30s"
  event_slow_timeout: 300, // default timeout before a console.warn("Slow event processing: bus.on(SomeEvent, ...4 handlers) have taken more than 300s"
})
```

Core methods:

- `bus.emit(event)` aka `bus.dispatch(event)`
- `bus.on(event_type, handler, options?)`
- `bus.off(event_type, handler)`
- `bus.find(event_type, options?)`
- `bus.waitUntilIdle()`

Notes:

- String matching of event types using `bus.on('SomeEvent', ...)` and `bus.on('*', ...)` wildcard matching is supported
- Prefer passing event class to (`bus.on(MyEvent, handler)`) over string-based maching for stricter type inference

### `BaseEvent`

Define typed events:

```ts
const MyEvent = BaseEvent.extend('MyEvent', {
  some_key: z.string(),
  some_other_key: z.number(),
  // ...
  // any other payload fields you want to include can go here

  // fields that start with event_* are reserved for metadata used by the library
  event_result_schema: z.string().optional(),
  event_timeout: 60,
  // ...
})

const pending_event: MyEvent = MyEvent({ some_key: 'abc', some_other_key: 234 })
const queued_event: MyEvent = bus.emit(pending_event)
const completed_event: MyEvent = queued_event.done()
```

Special fields that change how the event is processed:

- `event_result_schema` defines the type to enforce for handler return values
- `event_concurrency`, `event_handler_concurrency`, `event_handler_completion`
- `event_timeout`, `event_handler_timeout`, `event_handler_slow_timeout`

Common methods:

- `await event.done()`
- `await event.first()`
- `event.toJSON()` (serialization format is compatible with python library)
- `event.fromJSON()`

#### `done()`

- Runs the event with completion mode `'all'` and waits for all handlers/buses to finish.
- Returns the same event instance in completed state so you can inspect `event_results`, `event_errors`, etc.
- Want to dispatch and await an event like a function call? simply `await event.done()` and it will process immediately, skipping queued events.
- Want to wait for normal processing in the order it was originally queued? use `await event.waitForCompletion()`

#### `first()`

- Runs the event with completion mode `'first'`.
- Returns the temporally first non-`undefined` handler result (not registration order).
- If all handlers return `undefined` (or only error), it resolves to `undefined`.
- Remaining handlers are cancelled after the winning result is found.

### `EventResult`

Each handler run produces an `EventResult` stored in `event.event_results` with:

- `status`: `pending | started | completed | error`
- `result: EventType.event_result_schema` or `error: Error | undefined`
- handler metadata (`handler_id`, `handler_name`, bus metadata)
- `event_children` list of any sub-events that were emitted during handling

The event aggregates these via `event.event_results` and exposes the values from them via getters like `event.first_result`, `event.event_errors`, and others.

<br/>

---

<br/>

## 🧵 Advanced Concurrency Control

### Concurrency Config Options

#### Bus-level config options (`new EventBus(name, {...options...})`)

- `max_history_size?: number | null` (default: `100`)
  - Max completed events kept in history. `null` = unlimited. `bus.find(...)` uses this log to query recently completed events
- `event_concurrency?: 'global-serial' | 'bus-serial' | 'parallel' | null` (default: `'bus-serial'`)
  - Event-level scheduling policy (`global-serial`: FIFO across all buses, `bus-serial`: FIFO per bus, `parallel`: concurrent events per bus).
- `event_handler_concurrency?: 'serial' | 'parallel' | null` (default: `'serial'`)
  - Handler-level scheduling policy for each event (`serial`: one handler at a time per event, `parallel`: all handlers for the event can run concurrently).
- `event_handler_completion?: 'all' | 'first'` (default: `'all'`)
  - Completion strategy (`all`: wait for all handlers, `first`: stop after first non-`undefined` result).
- `event_timeout?: number | null` (default: `60`)
  - Default handler timeout budget in seconds.
- `event_handler_slow_timeout?: number | null` (default: `30`)
  - Slow-handler warning threshold in seconds.
- `event_slow_timeout?: number | null` (default: `300`)
  - Slow-event warning threshold in seconds.

#### Event-level config options

Override the bus defaults on a per-event basis by using these special fields in the event:

```ts
const event = MyEvent({
  event_concurrency: 'parallel',
  event_handler_concurrency: 'parallel',
  event_handler_completion: 'first',
  event_timeout: 10,
  event_handler_timeout: 3,
})
```

Notes:

- `null` means "inherit/fall back to bus default" for event-level concurrency and timeout fields.
- Forwarded events are processed under the target bus's config; source bus config is not inherited.
- `event_handler_completion` is independent from handler scheduling mode (`serial` vs `parallel`).

#### Handler-level config options

Set at registration:

```ts
bus.on(MyEvent, handler, { handler_timeout: 2 }) // max time in seconds this handler is allowed to run before it's aborted
```

#### Precedence and interaction

Event and handler concurrency precedence:

1. Event instance override (`event.event_concurrency`, `event.event_handler_concurrency`)
2. Bus defaults (`EventBus` options)
3. Built-in defaults (`bus-serial`, `serial`)

Timeout resolution for each handler run:

1. Resolve handler timeout source:
   - `bus.on(..., { handler_timeout })`
   - else `event.event_handler_timeout`
   - else bus `event_timeout`
2. Apply event cap:
   - effective timeout is `min(resolved_handler_timeout, event.event_timeout)` when both are non-null
   - if either is `null`, the non-null value wins; both null means no timeout

Additional timeout nuance:

- `BaseEvent.event_timeout` starts as `null` unless set; dispatch applies bus default timeout when still unset.
- Bus/event timeouts are outer budgets for handler execution; use `@retry({ timeout })` for per-attempt timeouts.

Use `@retry` for per-handler execution timeout/retry/backoff/semaphore control. Keep bus/event timeouts as outer execution budgets.

### Runtime lifecycle (bus -> event -> handler)

Dispatch flow:

1. `dispatch()` normalizes to original event and captures async context when available.
2. Bus applies defaults and appends itself to `event_path`.
3. Event enters `event_history`, `pending_event_queue`, and runloop starts.
4. Runloop dequeues and calls `processEvent()`.
5. Event-level semaphore (`event_concurrency`) is applied.
6. Handler results are created and executed under handler-level semaphore (`event_handler_concurrency`).
7. Event completion and child completion propagate through `event_pending_bus_count` and result states.
8. History trimming evicts completed events first; if still over limit, oldest pending events can be dropped (with warning), then cleanup runs.

Locking model:

- Global event semaphore: `global-serial`
- Bus event semaphore: `bus-serial`
- Per-event handler semaphore: `serial` handler mode

### Queue-jumping (`await event.done()` inside handlers)

Want to dispatch and await an event like a function call? simply `await event.done()`.
When called inside a handler, the awaited event is processed immediately (queue-jump behavior) before normal queued work continues.

### `@retry` Decorator

`retry()` adds retry logic and optional semaphore-based concurrency limiting to async functions/handlers.

#### Why retry is handler-level

Retry and timeout belong on handlers, not emit sites:

- Handlers fail; events are messages.
- Handler-level retries preserve replay semantics (one event dispatch, internal retry attempts).
- Bus concurrency and retry concerns are orthogonal and compose cleanly.

#### Recommended pattern: `@retry()` on class methods

```ts
import { retry, EventBus } from 'bubus'

class ScreenshotService {
  constructor(private bus: InstanceType<typeof EventBus>) {
    bus.on(ScreenshotRequestEvent, this.onScreenshot.bind(this))
  }

  @retry({
    max_attempts: 4,
    retry_on_errors: [/timeout/i],
    timeout: 5,
    semaphore_scope: 'global',
    semaphore_name: 'Screenshots',
    semaphore_limit: 2,
  })
  async onScreenshot(event: InstanceType<typeof ScreenshotRequestEvent>): Promise<Buffer> {
    return await takeScreenshot(event.data.url)
  }
}

const ev = bus.emit(ScreenshotRequestEvent({ url: 'https://example.com' }))
await ev.done()
```

#### Also works: inline HOF

```ts
bus.on(
  MyEvent,
  retry({ max_attempts: 3, timeout: 10 })(async (event) => {
    await riskyOperation(event.data)
  })
)
```

#### Options

| Option                 | Type                                      | Default     | Description                                     |
| ---------------------- | ----------------------------------------- | ----------- | ----------------------------------------------- |
| `max_attempts`         | `number`                                  | `1`         | Total attempts including first call.            |
| `retry_after`          | `number`                                  | `0`         | Seconds between retries.                        |
| `retry_backoff_factor` | `number`                                  | `1.0`       | Multiplier for retry delay.                     |
| `retry_on_errors`      | `(ErrorClass \| string \| RegExp)[]`      | `undefined` | Retry filter. `undefined` retries on any error. |
| `timeout`              | `number \| null`                          | `undefined` | Per-attempt timeout in seconds.                 |
| `semaphore_limit`      | `number \| null`                          | `undefined` | Max concurrent executions sharing semaphore.    |
| `semaphore_name`       | `string \| ((...args) => string) \| null` | fn name     | Semaphore key.                                  |
| `semaphore_lax`        | `boolean`                                 | `true`      | Continue if semaphore acquisition times out.    |
| `semaphore_scope`      | `'global' \| 'class' \| 'instance'`       | `'global'`  | Scope for semaphore identity.                   |
| `semaphore_timeout`    | `number \| null`                          | `undefined` | Max seconds waiting for semaphore.              |

#### Error types

- `RetryTimeoutError`: per-attempt timeout exceeded.
- `SemaphoreTimeoutError`: semaphore acquisition timeout (`semaphore_lax=false`).

#### Re-entrancy

On Node.js/Bun, `AsyncLocalStorage` tracks held semaphores and avoids deadlocks for nested calls using the same semaphore.
In browsers, this tracking is unavailable, avoid recursive/nested same-semaphore patterns there.

#### Interaction with bus concurrency

Execution order when used on bus handlers:

1. Bus acquires handler semaphore (`event_handler_concurrency`)
2. `retry()` acquires retry semaphore (if configured)
3. Handler executes (with retries)
4. `retry()` releases retry semaphore
5. Bus releases handler semaphore

Use bus/event timeouts for outer deadlines and `retry({ timeout })` for per-handler-attempt deadlines.

#### Discouraged: retrying emit sites

Avoid wrapping `emit()/done()` in `retry()` unless you intentionally want multiple event dispatches (a new event for every retry).  
Keep retries on handlers so that your logs represent the original high-level intent, with a single event per call even if handling it took multiple tries.  
Emitting a new event for each retry is only recommended if you are using the logs for debugging more than for replayability / time-travel.

<br/>

---

<br/>

## 🏃 Runtimes

`bubus-ts` supports all major JS runtimes.

- Node.js (default development and test runtime)
- Browsers (ESM)
- Bun
- Deno

### Browser support notes

- The package output is ESM (`./dist/esm`) which is supported by all browsers [released after 2018](https://caniuse.com/?search=ESM)
- `AsyncLocalStorage` is preserved at dispatch and used during handling when availabe (Node/Bun), otel/tracing context will work normally in those environments

### Performance comparison (local run, per-event)

Measured locally on an `Apple M4 Pro` with:

- `pnpm run perf:node` (`node v22.21.1`)
- `pnpm run perf:bun` (`bun v1.3.9`)
- `pnpm run perf:deno` (`deno v2.6.8`)
- `pnpm run perf:browser` (`chrome v145.0.7632.6`)

| Runtime            | 1 bus x 50k events x 1 handler | 500 busses x 100 events x 1 handler | 1 bus x 1 event x 50k parallel handlers | 1 bus x 50k events x 50k one-off handlers | Worst case (N busses x N events x N handlers) |
| ------------------ | ------------------------------ | ----------------------------------- | --------------------------------------- | ----------------------------------------- | --------------------------------------------- |
| Node               | `0.015ms/event`, `0.6kb/event` | `0.058ms/event`, `0.1kb/event`      | `0.021ms/handler`, `189792.0kb/event`   | `0.028ms/event`, `0.6kb/event`            | `0.442ms/event`, `0.9kb/event`                |
| Bun                | `0.011ms/event`, `2.5kb/event` | `0.054ms/event`, `1.0kb/event`      | `0.006ms/handler`, `223296.0kb/event`   | `0.019ms/event`, `2.8kb/event`            | `0.441ms/event`, `3.1kb/event`                |
| Deno               | `0.018ms/event`, `1.2kb/event` | `0.063ms/event`, `0.4kb/event`      | `0.024ms/handler`, `156752.0kb/event`   | `0.064ms/event`, `2.6kb/event`            | `0.461ms/event`, `7.9kb/event`                |
| Browser (Chromium) | `0.030ms/event`                | `0.197ms/event`                     | `0.022ms/handler`                       | `0.022ms/event`                           | `1.566ms/event`                               |

Notes:

- `kb/event` is peak RSS delta per event during active processing (most representative of OS-visible RAM in Activity Monitor / Task Manager, with `EventBus.max_history_size=1`)
- In `1 bus x 1 event x 50k parallel handlers` stats are shown per-handler for clarity, `0.02ms/handler * 50k handlers ~= 1000ms` for the entire event
- Browser runtime does not expose memory usage easily, in practice memory performance in-browser is comparable to Node (they both use V8)

<br/>

---

<br/>

## 👾 Development

```bash
git clone https://github.com/pirate/bbus bubus && cd bubus

cd ./bubus-ts
pnpm install
pnpm lint
pnpm test
```
