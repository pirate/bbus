# TODO: TypeScript middleware support (parity with Python)

Goal: add full EventBus middleware support in `bubus-ts`, matching Python semantics, with these requirements:
- `middlewares` accepts instances or constructors and auto-initializes constructors.
- Hooks run sequentially (per middleware order) for a given hook call.
- Middleware errors do not fail events or handlers; they are logged and suppressed.
- Hooks are async (`Promise<void>`) and are `await`ed sequentially where invoked.
- Keep user-facing API and behavior aligned with Python, with minimal LoC.

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
export type EventStatus = 'pending' | 'started' | 'completed'

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

Add a small internal helper in `EventBus` to execute hooks sequentially and log failures:

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
    try {
      await fn.apply(middleware, args as any)
    } catch (error) {
      const name = middleware?.constructor?.name ?? 'AnonymousMiddleware'
      console.error(`[bubus] middleware ${name} ${hook} failed`, error)
    }
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

### Notification pipeline (reduce layering violations)

Route all middleware execution through `EventBus`, while lower layers only notify their parent:

- `EventResult` notifies its `Event` of status changes (no middleware knowledge).
- `Event` notifies its bus of changes (no middleware knowledge).
- `EventBus` owns the middleware list and always executes hooks sequentially with error suppression.
- Use the same method names at each level: `._on_event_change` and `._on_event_result_change`.

This keeps middleware logic centralized in `EventBus`, while `EventResult` and `BaseEvent` only trigger parent notifications.

### Event lifecycle hooks

1. Pending: enqueue time (non-blocking, like Python).
2. Started: inside `BaseEvent.markStarted()` (fires for all paths, including no-handlers).
3. Completed: inside `BaseEvent.markCompleted(...)` (fires for normal completion and cancel paths).

```ts
// bubus-ts/src/event_bus.ts
const original_event = event._event_original ?? event
// after enqueue, before runloop kick
queueMicrotask(async () => {
  await original_event._on_event_change('pending')
})

// in processEvent(...)
event.markStarted() // triggers event._on_event_change('started') internally
...
event.markCompleted(false) // triggers event._on_event_change('completed') internally
```

```ts
// bubus-ts/src/base_event.ts (new helper)
async _on_event_change(status: EventStatus): Promise<void> {
  const original = this._event_original ?? this
  const bus = original.bus
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
  const bus = EventBus._all_instances.findBusById(result.eventbus_id) ?? original.bus
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
queueMicrotask(async () => {
  await this.runMiddlewareHook('on_handler_change', [this, handler_entry, true])
})

queueMicrotask(async () => {
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

3. Error suppression:
- Throw inside middleware hook; event and handler still complete.

4. Lifecycle coverage:
- Verify pending/started/completed hooks for event and event_result.

## Flow audit (edge cases to cover)

- No handlers registered:
  - `EventBus.processEvent` still calls `event.markStarted()` and `event.markCompleted(false)` so `started` + `completed` are emitted.
- First-mode cancellation:
  - `BaseEvent.cancelEventHandlersForFirstMode` calls `EventResult.markError(...)`; since `markError` triggers `_on_event_result_change('completed')`, hooks fire.
- Parent/child completion propagation:
  - `BaseEvent.notifyEventParentsOfCompletion` calls `parent.markCompleted(false, false)`; `markCompleted` emits `completed` change to the parent bus.
- Cancellation paths:
  - `BaseEvent.cancelPendingDescendants` and `BaseEvent.markCancelled` call `markCompleted`; `markCompleted` emits `completed`.
  - `BaseEvent.markCancelled` calls `createPendingHandlerResults` in a sync context; if pending hooks are required there, schedule a microtask to call `event._on_event_result_change(..., 'pending')` for any newly created results.

## Files to touch

- `bubus-ts/src/middlewares.ts` (new)
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/event_result.ts`
- `bubus-ts/src/index.ts`
- `bubus-ts/tests/*` (new tests)
