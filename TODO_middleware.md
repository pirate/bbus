# TODO: TypeScript middleware support (parity with Python)

Goal: add full EventBus middleware support in `bubus-ts`, matching Python lifecycle semantics where practical, with these requirements:
- `middlewares` accepts instances or constructors and auto-initializes constructors.
- Hooks run sequentially (per middleware order) for a given hook call.
- Hooks are async (`Promise<void>`) and are `await`ed sequentially where invoked on async code paths.
- For sync code paths (e.g., `markCancelled`), use microtask scheduling to fire hooks without changing sync signatures. Use `queueMicrotask` with a `Promise.resolve().then(...)` fallback for browser/node/deno/bun.
- Per-bus lifecycle hook emission is acceptable (different buses can have different middleware stacks).
- Middleware exceptions are out-of-contract for now (undefined behavior if a middleware throws).
- Keep changes minimal and focused on runtime behavior; docs updates are out of scope for this pass.

Notes on Python behavior to match:
- Event status only uses `pending | started | completed` (errors are represented on EventResult, not Event).
- Middleware hooks are invoked for:
  - `on_event_change`: pending, started, completed
  - `on_event_result_change`: pending, started, completed
  - `on_handler_change`: on handler add/remove

## Planned API surface

Add a TS interface and constructor type:

```ts
// bubus-ts/src/middlewares.ts (new)
import type { BaseEvent } from './base_event.js'
import type { EventBus } from './event_bus.js'
import type { EventHandler } from './event_handler.js'
import type { EventResult } from './event_result.js'
import type { EventStatus } from './types.js'

export interface EventBusMiddleware {
  on_event_change?(eventbus: EventBus, event: BaseEvent, status: EventStatus): Promise<void>
  on_event_result_change?(
    eventbus: EventBus,
    event: BaseEvent,
    event_result: EventResult,
    status: EventStatus
  ): Promise<void>
  on_handler_change?(eventbus: EventBus, handler: EventHandler, registered: boolean): Promise<void>
}

export type EventBusMiddlewareCtor = new () => EventBusMiddleware
export type { EventStatus }
```

Example async middleware (hooks are `async` and may await work):

```ts
class AnalyticsMiddleware implements EventBusMiddleware {
  async on_event_change(eventbus: EventBus, event: BaseEvent, status: EventStatus): Promise<void> {
    await analyticsClient.trackEvent(status, event.event_type, eventbus.label)
  }

  async on_event_result_change(
    eventbus: EventBus,
    event: BaseEvent,
    event_result: EventResult,
    status: EventStatus
  ): Promise<void> {
    if (status === 'completed') {
      await analyticsClient.trackHandlerResult(event.event_type, event_result.handler_name)
    }
  }
}
```

Update `EventBus` options to accept either type:

```ts
// bubus-ts/src/event_bus.ts
export type EventBusOptions = {
  // ...existing
  middlewares?: Array<EventBusMiddleware | EventBusMiddlewareCtor>
}
```

Export the new types:

```ts
// bubus-ts/src/index.ts
export type { EventBusMiddleware, EventBusMiddlewareCtor, EventStatus } from './middlewares.js'
```

## Hooking plan (minimal LoC changes)

Add a small internal helper in `EventBus` to execute hooks sequentially:

```ts
// bubus-ts/src/event_bus.ts
private readonly middlewares: EventBusMiddleware[]

private async runMiddlewareHook(
  hook: 'on_event_change' | 'on_event_result_change' | 'on_handler_change',
  args: [EventBus, BaseEvent, EventStatus] | [EventBus, BaseEvent, EventResult, EventStatus] | [EventBus, EventHandler, boolean]
): Promise<void> {
  if (this.middlewares.length === 0) return
  for (const middleware of this.middlewares) {
    const fn = (middleware as any)[hook]
    if (!fn) continue
    await fn.apply(middleware, args as any)
  }
}
```

Constructor auto-init behavior:

```ts
// bubus-ts/src/event_bus.ts (constructor)
const raw = options.middlewares ?? []
this.middlewares = raw.map((item) => {
  if (typeof item === 'function') {
    return new (item as EventBusMiddlewareCtor)()
  }
  return item
})
```

Microtask helper (runtime-safe):

```ts
const scheduleMicrotask = (fn: () => void | Promise<void>): void => {
  if (typeof queueMicrotask === 'function') {
    queueMicrotask(() => void fn())
  } else {
    Promise.resolve().then(() => void fn())
  }
}
```

### Notification pipeline (reduce layering violations)

Route all middleware execution through `EventBus`, while lower layers only notify upward:

- `EventResult` notifies its parent `Event` of status changes (no middleware knowledge).
- `Event` notifies a target bus of changes (no middleware knowledge).
- `EventBus` owns the middleware list and always executes hooks sequentially.
- Use the same method names at each level: `._on_event_change` and `._on_event_result_change`.

This keeps middleware logic centralized in `EventBus`, while `EventResult` and `BaseEvent` only trigger parent notifications.

### Event lifecycle hooks

1. Pending: enqueue time (non-blocking, like Python).
2. Started: inside `BaseEvent.markStarted()` (fires for all paths, including no-handlers).
3. Completed: inside `BaseEvent.markCompleted(...)` (fires for normal completion and cancel paths).

```ts
// bubus-ts/src/event_bus.ts
const original_event = event._event_original ?? event
// after enqueue, before runloop kick (same bus context)
scheduleMicrotask(async () => {
  await original_event._on_event_change(this, 'pending')
})

// in processEvent(...)
event.markStarted(this) // triggers event._on_event_change(this, 'started') internally
...
event.markCompleted(false, true, this) // triggers event._on_event_change(this, 'completed') internally
```

```ts
// bubus-ts/src/base_event.ts (new helper)
async _on_event_change(bus: EventBus | null | undefined, status: EventStatus): Promise<void> {
  const original = this._event_original ?? this
  if (!bus) return
  await bus._on_event_change(original, status)
}
```

```ts
// bubus-ts/src/event_bus.ts (new internal notifier)
async _on_event_change(event: BaseEvent, status: EventStatus): Promise<void> {
  await this.runMiddlewareHook('on_event_change', [this, event, status])
}
```

### Handler result lifecycle hooks

- Pending: after `createPendingHandlerResults` creates/returns pending results.
  - `createPendingHandlerResults` stays sync (as today) and does not invoke hooks; `EventBus` emits pending hooks immediately after creation, matching Python's behavior.
- Started: inside `EventResult.markStarted()` (fires for all paths).
- Completed: inside `EventResult.markCompleted` or `EventResult.markError` (fires for normal and cancel paths).

```ts
// bubus-ts/src/event_bus.ts
const pending_entries = event.createPendingHandlerResults(this)
for (const entry of pending_entries) {
  // pending hook re-fires for pre-existing pending entries is acceptable
  if (entry.result.status === 'pending') {
    await event._on_event_result_change(entry.result, 'pending')
  }
}

// bubus-ts/src/event_result.ts
const abort_signal = this.markStarted() // triggers event._on_event_result_change('started') internally
...
this.markCompleted(...) // triggers event._on_event_result_change('completed') internally
// markError(...) will also trigger event._on_event_result_change('completed') internally
```

```ts
// bubus-ts/src/base_event.ts (new helper)
async _on_event_result_change(result: EventResult, status: EventStatus): Promise<void> {
  const original = this._event_original ?? this
  const scoped_event = result.event?._event_original ?? result.event
  const bus = scoped_event?.bus ?? original.bus
  if (!bus) return
  await bus._on_event_result_change(original, result, status)
}
```

```ts
// bubus-ts/src/event_bus.ts (new internal notifier)
async _on_event_result_change(event: BaseEvent, result: EventResult, status: EventStatus): Promise<void> {
  await this.runMiddlewareHook('on_event_result_change', [this, event, result, status])
}
```

### Handler registration hooks

- `on(...)`: after registration.
- `off(...)`: after removal.
- Fire-and-forget, but still sequential per hook call.

```ts
// bubus-ts/src/event_bus.ts
scheduleMicrotask(async () => {
  await this.runMiddlewareHook('on_handler_change', [this, handler_entry, true])
})

scheduleMicrotask(async () => {
  await this.runMiddlewareHook('on_handler_change', [this, entry, false])
})
```

## Built-in middleware parity (follow-on)

After core hooks land, implement these in TS with identical names/behavior:
- `AutoErrorEventMiddleware`
- `AutoReturnEventMiddleware`
- `AutoHandlerChangeEventMiddleware`
- `LoggerEventBusMiddleware`
- `WALEventBusMiddleware`
- `SQLiteHistoryMirrorMiddleware`

For the UI, `SQLiteHistoryMirrorMiddleware` must match the table schemas read by `ui/db.py`.

## Test plan (minimal)

1. `middlewares` auto-init:
- Pass constructor and instance; ensure both are invoked.

2. Hook order:
- Register two middlewares, verify hook calls are sequential in order.

3. Middleware errors:
- Throw inside middleware hook; behavior is intentionally undefined (do not assert suppression).

4. Lifecycle coverage:
- Verify pending/started/completed hooks for event and event_result.

## Flow audit (edge cases to cover)

- No handlers registered:
  - `EventBus.processEvent` still calls `event.markStarted(...)` and `event.markCompleted(false, ...)` so `started` + `completed` are emitted.
- First-mode cancellation:
  - `BaseEvent.cancelEventHandlersForFirstMode` calls `EventResult.markError(...)`; since `markError` triggers `_on_event_result_change('completed')`, hooks fire.
- Parent/child completion propagation:
  - `BaseEvent.notifyEventParentsOfCompletion` calls `parent.markCompleted(false, false, parent.bus)`; `markCompleted` emits `completed` change to the parent bus.
- Cancellation paths:
  - `BaseEvent.cancelPendingDescendants` and `BaseEvent.markCancelled` call `markCompleted`; `markCompleted` emits `completed`.
  - `BaseEvent.markCancelled` is sync; it must never emit `completed -> started` ordering. Emitting only `completed` is acceptable, and `started -> completed` is also acceptable.

## Files to touch

- `bubus-ts/src/middlewares.ts` (new)
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/base_event.ts`
- `bubus-ts/src/event_result.ts`
- `bubus-ts/src/index.ts`
- `bubus-ts/tests/*` (new tests)
