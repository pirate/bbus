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

The main bus class that registers handlers, schedules events, and tracks results.

Constructor:

```ts
new EventBus(name?: string, options?: {
  id?: string
  max_history_size?: number | null
  event_concurrency?: 'global-serial' | 'bus-serial' | 'parallel' | null
  event_timeout?: number | null
  event_slow_timeout?: number | null
  event_handler_concurrency?: 'serial' | 'parallel' | null
  event_handler_completion?: 'all' | 'first'
  event_handler_slow_timeout?: number | null
  event_handler_detect_file_paths?: boolean
})
```

#### Constructor options

| Option                            | Type                                                    | Default        | Purpose                                                                                                                                                                              |
| --------------------------------- | ------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`                              | `string`                                                | `uuidv7()`     | Override bus UUID (mostly for serialization/tests).                                                                                                                                  |
| `max_history_size`                | `number \| null`                                        | `100`          | Max events kept in `event_history`; `null` = unbounded. Current behavior is equivalent to `max_history_drop=true`: if `True`, drop oldest history entries (even uncompleted events). |
| `event_concurrency`               | `'global-serial' \| 'bus-serial' \| 'parallel' \| null` | `'bus-serial'` | Event-level scheduling policy.                                                                                                                                                       |
| `event_handler_concurrency`       | `'serial' \| 'parallel' \| null`                        | `'serial'`     | Per-event handler scheduling policy.                                                                                                                                                 |
| `event_handler_completion`        | `'all' \| 'first'`                                      | `'all'`        | Event completion mode if event does not override it.                                                                                                                                 |
| `event_timeout`                   | `number \| null`                                        | `60`           | Default per-handler timeout budget in seconds (unless overridden).                                                                                                                   |
| `event_handler_slow_timeout`      | `number \| null`                                        | `30`           | Slow handler warning threshold (seconds).                                                                                                                                            |
| `event_slow_timeout`              | `number \| null`                                        | `300`          | Slow event warning threshold (seconds).                                                                                                                                              |
| `event_handler_detect_file_paths` | `boolean`                                               | `true`         | Capture source file:line for handlers (slower, better logs).                                                                                                                         |

#### Runtime state properties

- `id: string`
- `name: string`
- `label: string` (`${name}#${id.slice(-4)}`)
- `handlers: Map<string, EventHandler>`
- `handlers_by_key: Map<string, string[]>`
- `event_history: Map<string, BaseEvent>`
- `pending_event_queue: BaseEvent[]`
- `in_flight_event_ids: Set<string>`
- `locks: LockManager`

#### `on()`

```ts
on<T extends BaseEvent>(
  event_key: string | '*' | EventClass<T>,
  handler: EventHandlerFunction<T>,
  options?: Partial<EventHandler>
): EventHandler
```

Use during startup/composition to register handlers.

Advanced `options` fields, these can be used to override defaults per-handler if needed:

- `handler_timeout?: number | null` hard delay before handler execution is aborted with a `HandlerTimeoutError`
- `handler_slow_timeout?: number | null` delay before emitting a slow handler warning log line
- `handler_name?: string` optional name to use instead of `anonymous` if handler is an unnamed arrow function
- `handler_file_path?: string` optional path/to/source/file.js:lineno where the handler is defined, used for logging only
- `id?: string` unique UUID for the handler (normally a hash of bus_id + event_key + handler_name + handler_registered_at)

Notes:

- Prefer class/factory keys (`bus.on(MyEvent, handler)`) for typed payload/result inference.
- String and `'*'` matching are supported (`bus.on('MyEvent', ...)`, `bus.on('*', ...)`).
- Returns an `EventHandler` object you can later pass to `off()` to de-register the handler if needed.

#### `off()`

```ts
off<T extends BaseEvent>(
  event_key: EventKey<T> | '*',
  handler?: EventHandlerFunction<T> | string | EventHandler
): void
```

Use when tearing down subscriptions (tests, plugin unload, hot-reload).

- Omit `handler` to remove all handlers for `event_key`.
- Pass handler function reference to remove one by function identity.
- Pass handler id (`string`) or `EventHandler` object to remove by id.
- use `bus.off('*')` to remove _all_ registered handlers from the bus

#### `dispatch()` / `emit()`

```ts
dispatch<T extends BaseEvent>(event: T): T
emit<T extends BaseEvent>(event: T): T
```

`emit()` is just an alias of `dispatch()`.

Behavior notes:

- Per-event configuration options like `event_timeout`, `event_handler_timeout`, etc. are copied from bus defaults at dispatch time if unset
- If same event ends up forwarded through multiple buses, it is loop-protected using `event_path`.
- Dispatch is synchronous and returns immediately with the same event object (`event.event_status` is initially `'pending'`).

Normal lifecycle:

1. Create event instance (`const event = MyEvent({...})`).
2. Dispatch (`const queued = bus.emit(event)`).
3. Await with `await queued.done()` (immediate/queue-jump semantics) or `await queued.waitForCompletion()` (bus queue order).
4. Inspect `queued.event_results`, `queued.first_result`, `queued.event_errors`, etc. if you need to access handler return values

#### `find()`

```ts
find<T extends BaseEvent>(event_key: EventKey<T> | '*', options?: FindOptions): Promise<T | null>
find<T extends BaseEvent>(
  event_key: EventKey<T> | '*',
  where: (event: T) => boolean,
  options?: FindOptions
): Promise<T | null>
```

Where:

```ts
type FindOptions = {
  past?: boolean | number // true to look through all past events, or number in seconds to filter time range
  future?: boolean | number // true to wait for event to appear indefinitely, or number in seconds to wait for event to appear
  child_of?: BaseEvent | null // filter to only match events that are a child_of: some_parent_event
} & {
  // event_status: 'pending' | 'started' | 'completed'
  // event_id: 'some-exact-event-uuid-here',
  // event_started_at: string (exact iso datetime string)
  // ... any event.event_* field can be passed to filter filter events using simple equality checks
  [K in keyof BaseEvent as K extends `event_${string}` ? K : never]?: BaseEvent[K]
}
```

`bus.find()` returns the first matching event (in dispatch timestamp order).
To find multiple matching events, iterate through `bus.event_history.filter((event) => ...some condition...)` manually.

`where` behavior:
Any filter predicate function in the form of `(event) => true | false`, returning true to consider the event a match.

```ts
const matching_event = bus.find(SomeEvent, (event) => event.some_field == 123)
// or to match all event types:
const matching_event = bus.find('*', (event) => event.some_field == 123)
```

`past` behavior:

- `true`: search all history.
- `false`: skip searching past event history.
- `number`: search events dispatched within last `N` seconds.

`future` behavior:

- `true`: wait forever for future match.
- `false`: do not wait.
- `number`: wait up to `N` seconds.

Lifecycle use:

- Use for idempotency / de-dupe before dispatch (`past: ...`).
- Use for synchronization/waiting (`future: ...`).
- Combine both to "check recent then wait".
- Add `child_of` to constrain by parent/ancestor event chain.
- Add any `event_*` field (e.g. `event_status`, `event_id`, `event_timeout`) to filter by strict equality.
- Use wildcard matching with predicates when you want to search all event types: `bus.find('*', (event) => ...)`.

Debouncing expensive events with `find()`:

```ts
const some_expensive_event = (await bus.find(ExpensiveEvent, { past: 15, future: 5 })) ?? bus.dispatch(ExpensiveEvent({}))
await some_expensive_event.done()
```

Important semantics:

- Past lookup matches any dispatched events, not just completed events.
- Past/future matches resolve as soon as event is dispatched. If you need the completed event, await `event.done()` or pass `{event_status: 'completed'}` to filter only for completed events.
- If both `past` and `future` are omitted, defaults are `past: true, future: false`.
- If both `past` and `future` are `false`, it returns `null` immediately.
- Detailed behavior matrix is covered in `bubus-ts/tests/find.test.ts`.

#### `waitUntilIdle()`

`await bus.waitUntilIdle()` is the normal "drain bus work" call to wait until bus is done processing everything queued.

```ts
bus.emit(OneEvent(...))
bus.emit(TwoEvent(...))
bus.emit(ThreeEvent(...))
await bus.waitUntilIdle()   // this resolves once all three events have finished processing
```

#### Parent/child/event lookup helpers

```ts
eventIsChildOf(child_event: BaseEvent, paret_event: BaseEvent): boolean
eventIsParentOf(parent_event: BaseEvent, child_event: BaseEvent): boolean
findEventById(event_id: string): BaseEvent | null
```

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventBusJSON
EventBus.fromJSON(data: unknown): EventBus
```

- `toString()` returns `BusName#abcd` style labels used in logs/errors.
- `toJSON()` exports full bus state snapshot (config, handlers, indexes, event_history, pending queue, in-flight ids, find-waiter snapshots).
- `fromJSON()` restores a new bus instance from that payload (handler functions are restored as no-op stubs).

#### `logTree()`

```ts
logTree(): string
```

- `logTree()` returns a full event log hierarchy tree diagram for debugging.

#### `destroy()`

```ts
destroy(): void
```

- `destroy()` clears handlers/history/locks and removes this bus from global weak registry.
- `destroy()`/GC behavior is exercised in `bubus-ts/tests/eventbus_basics.test.ts` and `bubus-ts/tests/performance.test.ts`.

### `BaseEvent`

Base class + factory builder for typed event models.

Define your own strongly typed events with `BaseEvent.extend('EventName', {...zod fields...})`:

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

const pending_event = MyEvent({ some_key: 'abc', some_other_key: 234 })
const queued_event = bus.emit(pending_event)
const completed_event = await queued_event.done()
```

API behavior and lifecycle examples:

- `bubus-ts/examples/simple.ts`
- `bubus-ts/examples/immediate_event_processing.ts`
- `bubus-ts/examples/forwarding_between_busses.ts`
- `bubus-ts/tests/eventbus_basics.test.ts`
- `bubus-ts/tests/find.test.ts`
- `bubus-ts/tests/first.test.ts`
- `bubus-ts/tests/event_bus_proxy.test.ts`
- `bubus-ts/tests/timeout.test.ts`
- `bubus-ts/tests/event_results.test.ts`

#### Event configuration fields

Special configuration fields you can set on each event to control processing:

- `event_result_schema?: z.ZodTypeAny`
- `event_result_type?: string`
- `event_version?: string` (default: `'0.0.1'`; useful for your own schema/data migrations)
- `event_timeout?: number | null`
- `event_handler_timeout?: number | null`
- `event_handler_slow_timeout?: number | null`
- `event_concurrency?: 'global-serial' | 'bus-serial' | 'parallel' | null`
- `event_handler_concurrency?: 'serial' | 'parallel' | null`
- `event_handler_completion?: 'all' | 'first'`

#### Runtime state fields

- `event_id`, `event_type`, `event_version`, `event_path`, `event_parent_id`
- `event_status: 'pending' | 'started' | 'completed'`
- `event_results: Map<string, EventResult>`
- `event_pending_bus_count`
- `event_created_at/ts`, `event_started_at/ts`, `event_completed_at/ts`

#### Read-only attributes

- `event_parent` -> `BaseEvent | undefined`
- `event_children` -> `BaseEvent[]`
- `event_descendants` -> `BaseEvent[]`
- `event_errors` -> `Error[]`
- `all_results` -> `EventResultType<this>[]`
- `first_result` -> `EventResultType<this> | undefined`
- `last_result` -> `EventResultType<this> | undefined`

#### `done()`

```ts
done(): Promise<this>
```

- `immediate()` is an alias for `done()`.
- If called from inside a running handler, it queue-jumps child processing immediately.
- If called outside handler context, it waits for normal completion (or processes immediately if already next).
- Rejects if event is not attached to a bus (`event has no bus attached`).
- Queue-jump behavior is demonstrated in `bubus-ts/examples/immediate_event_processing.ts` and `bubus-ts/tests/event_bus_proxy.test.ts`.

#### `waitForCompletion()`

```ts
waitForCompletion(): Promise<this>
```

- `finished()` is an alias for `waitForCompletion()`
- Waits for completion in normal runloop order.
- Use inside handlers when you explicitly do not want queue-jump behavior.

#### `first()`

```ts
first(): Promise<EventResultType<this> | undefined>
```

- Forces `event_handler_completion = 'first'` for this run.
- Returns temporally first non-`undefined` successful handler result.
- Cancels pending/running losing handlers on the same bus.
- Returns `undefined` when no handler produces a successful non-`undefined` value.
- Cancellation and winner-selection behavior is covered in `bubus-ts/tests/first.test.ts`.

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): BaseEventData
BaseEvent.fromJSON(data: unknown): BaseEvent
EventFactory.fromJSON?.(data: unknown): TypedEvent
```

- JSON format is cross-language compatible with Python implementation.
- `event_result_schema` is serialized as JSON Schema when possible and rehydrated on `fromJSON`.
- Round-trip coverage is in `bubus-ts/tests/typed_results.test.ts` and `bubus-ts/tests/eventbus_basics.test.ts`.

#### Advanced/internal public methods

Mostly used by bus internals or custom runtimes:

- `markStarted()`
- `markCancelled(cause)`
- `markCompleted(force?, notify_parents?)`
- `createPendingHandlerResults(bus)`
- `processEvent(pending_entries?)`
- `cancelPendingDescendants(reason)`

### `EventResult`

Each handler execution creates one `EventResult` stored in `event.event_results`.

#### Main fields

- `id: string` (uuidv7 string)
- `status: 'pending' | 'started' | 'completed' | 'error'`
- `event: BaseEvent`
- `handler: EventHandler`
- `result: EventResultType<this> | undefined`
- `error: Error | undefined`
- `started_at: string` (ISO Format datetime string)
- `completed_at: string` (ISO Format datetime string)
- `event_children: BaseEvent[]`

#### Read-only getters

- `event_id` -> `string` uuiv7 of the event the result is for
- `bus` -> `EventBus` instance it's associated with
- `handler_id` -> `string` uuidv5 of the `EventHandler`
- `handler_name` -> `string | 'anonymous'` function name of the handler method
- `handler_file_path` -> `string | undefined` path/to/file.js:lineno where the handler method is defined
- `eventbus_name` -> `string` name, same as `this.bus.name`
- `eventbus_id` -> `string` uuidv7, same as `this.bus.id`
- `eventbus_label` -> `string` label, same as `this.bus.label`
- `value` -> `EventResultType<this> | undefined` alias of `this.result`
- `raw_value` -> `any` raw result value before schema validation, available when handler return value validation fails
- `handler_timeout` -> `number` seconds before handler execution is aborted (precedence: handler config -> event config -> bus level defaults)
- `handler_slow_timeout` -> `number` seconds before logging a slow execution warning (same prececence as `handler_timeout`)

#### Advanced/Internal methods

```ts
markStarted(): Promise<never>
markCompleted(result): void
markError(error): void

runHandler(): Promise<void>
signalAbort(error: Error): void
linkEmittedChildEvent(child_event): void
```

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventResultJSON
EventResult.fromJSON(event, data): EventResult
```

### `EventHandler`

Represents one registered handler entry on a bus. You usually get these from `bus.on(...)`, then pass them to `bus.off(...)` to remove.

#### Main fields

- `id` unique handler UUIDv5 (deterministic hash from bus/event/handler metadata unless overridden)
- `handler` function reference that executes for matching events
- `handler_name` function name (or `'anonymous'`)
- `handler_file_path` optional detected source path (`~/path/file.ts:line`)
- `handler_timeout` optional timeout override in seconds (`null` disables timeout limit)
- `handler_slow_timeout` optional slow-warning threshold in seconds (`null` disables slow warning)
- `handler_registered_at` ISO timestamp
- `handler_registered_ts` monotonic timestamp
- `event_key` subscribed key (`'SomeEvent'` or `'*'`)
- `eventbus_name` bus name where this handler was registered
- `eventbus_id` bus UUID where this handler was registered

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventHandlerJSON
EventHandler.fromJSON(data: unknown, handler?: EventHandlerFunction): EventHandler
```

- `toString()` returns `handlerName() (path:line)` when path/name are available, otherwise `function#abcd()`.
- `toJSON()` emits only serializable handler metadata (never function bodies).
- `fromJSON()` reconstructs the handler entry and accepts an optional real function to re-bind execution behavior.

<br/>

---

<br/>

## 🧵 Advanced Concurrency Control

### Concurrency Config Options

#### Bus-level config options (`new EventBus(name, {...options...})`)

- `max_history_size?: number | null` (default: `100`)
  - Max events kept in history. `null` = unlimited. `bus.find(...)` uses this log to query recently dispatched events
  - Current TS behavior is equivalent to `max_history_drop=true`: if `True`, drop oldest history entries (even uncompleted events).
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

## Bridges

Bridges are optional extra connectors provided that allow you to send/receive events from an external service, and you do not need to use a bridge to use bubus since it's normally purely in-memory. These are just simple helpers to forward bubus events JSON to storage engines / other processes / other machines; they prevent loops automatically, but beyond that it's only basic forwarding with no handler pickling or anything fancy.

Bridges all expose a very simple bus-like API with only `.emit()` and `.on()`.

**Example usage: link a bus to a redis pub/sub channel**
```ts
const bridge = new RedisEventBridge('redis://redis@localhost:6379')

bus.on('*', bridge.emit)  // listen for all events on bus and send them to redis channel
bridge.on('*', bus.emit)  // listen for new events in redis channel and dispatch them to our bus
```

- `new SocketEventBridge('/tmp/bubus_events.sock')`
- `new HTTPEventBridge({ send_to: 'https://127.0.0.1:8001/bubus_events', listen_on: 'http://0.0.0.0:8002/bubus_events' })`
- `new JSONLEventBridge('/tmp/bubus_events.jsonl')`
- `new SQLiteEventBridge('/tmp/bubus_events.sqlite3')`
- `new PostgresEventBridge('postgresql://user:pass@localhost:5432/dbname/bubus_events')`
- `new RedisEventBridge('redis://user:pass@localhost:6379/1/bubus_events')`
- `new NATSEventBridge('nats://localhost:4222', 'bubus_events')`

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
| Node               | `0.015ms/event`, `0.6kb/event` | `0.058ms/event`, `0.1kb/event`      | `0.021ms/handler`, `3.8kb/handler`      | `0.028ms/event`, `0.6kb/event`            | `0.442ms/event`, `0.9kb/event`                |
| Bun                | `0.011ms/event`, `2.5kb/event` | `0.054ms/event`, `1.0kb/event`      | `0.006ms/handler`, `4.5kb/handler`      | `0.019ms/event`, `2.8kb/event`            | `0.441ms/event`, `3.1kb/event`                |
| Deno               | `0.018ms/event`, `1.2kb/event` | `0.063ms/event`, `0.4kb/event`      | `0.024ms/handler`, `3.1kb/handler`      | `0.064ms/event`, `2.6kb/event`            | `0.461ms/event`, `7.9kb/event`                |
| Browser (Chromium) | `0.030ms/event`                | `0.197ms/event`                     | `0.022ms/handler`                       | `0.022ms/event`                           | `1.566ms/event`                               |

Notes:

- `kb/event` is peak RSS delta per event during active processing (most representative of OS-visible RAM in Activity Monitor / Task Manager, with `EventBus.max_history_size=1`)
- In `1 bus x 1 event x 50k parallel handlers` stats are shown per-handler for clarity, `0.02ms/handler * 50k handlers ~= 1000ms` for the entire event
- Browser runtime does not expose memory usage directly, in practice memory performance in-browser is comparable to Node (they both use V8)

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
