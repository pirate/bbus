import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

/*
Potential failure modes

A) Event concurrency modes
- global-serial not enforcing strict FIFO across multiple buses (events interleave).
- bus-serial allows cross-bus interleaving but still must be FIFO within a bus; breaks under forwarding.
- parallel accidentally serializes (e.g., semaphore still used) or breaks queue-jump semantics.
- auto not resolving correctly to bus defaults.

B) Handler concurrency modes
- global-serial not enforcing strict handler order across buses.
- bus-serial leaks parallelism between handlers on the same bus.
- parallel accidentally serializes or fails to enforce per-handler ordering.
- auto not resolving correctly to handler options or bus defaults.

C) Precedence resolution
- Event overrides not taking precedence over handler options.
- Handler options not taking precedence over bus defaults.
- Conflicting settings (event says parallel, handler says serial) choose wrong winner.

D) Queue-jump / awaited events
- event.done() inside handler doesn’t jump the queue across buses.
- Queue-jump bypasses semaphores incorrectly in contexts where it shouldn’t.
- Queue-jump fails when event already in-flight.

E) FIFO correctness
- FIFO order broken under bus-serial with interleaved emissions.
- FIFO order broken under global-serial across buses.
- FIFO order broken with forwarded events.

F) Forwarding & bus context
- Forwarded event’s event.bus mutates current handler context (wrong bus).
- Child events emitted after forwarding are mis-parented.
- event.event_path diverges between buses.
- Handler attribution lost when forwarded across buses (tree/log issues).

G) Parent/child tracking
- Child events not correctly linked to the parent handler when emitted via event.bus.
- event_children missing under concurrency due to async timing.
- event_pending_bus_count not decremented properly, leaving events stuck.

H) Find semantics under concurrency
- find(past) returns event not yet completed.
- find(future) doesn’t resolve when event finishes in another bus.
- find with child_of returns mismatched events under concurrency.

I) Timeouts + cancellation propagation
- Timeout doesn’t cancel pending child handlers.
- Cancelled results not marked or mis-attributed to the wrong handler.
- Timeout doesn’t propagate across forwarded buses (event still waits forever).

J) Handler result validation
- event_result_schema not enforced under parallel handler completion.
- Invalid result doesn’t mark handler error or event failure.
- Timeout + schema error ordering wrong (e.g., schema error overwrites timeout).

K) Idle / completion
- waitUntilIdle() returns early with in-flight events.
- event.done() resolves before children complete.
- event.done() never resolves due to deadlock in runloop.

L) Reentrancy / nested awaits
- Nested awaited child events starve sibling handlers.
- Awaited child events skip semaphore incorrectly (deadlocks or ordering regressions).

M) Edge-cases
- Multiple handlers for same event type with different options collide.
- Handler throws synchronously before await (still counted, no leaks).
- Handler returns a rejected promise (properly surfaced).
- Event emitted with event_concurrency/event_handler_concurrency invalid value (schema rejects).
- Event emitted with no bus set (done should reject).
*/

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const withResolvers = <T>() => {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolve_fn, reject_fn) => {
    resolve = resolve_fn
    reject = reject_fn
  })
  return { promise, resolve, reject }
}

test('global-serial: only one event processes at a time across buses', async () => {
  const SerialEvent = BaseEvent.extend('SerialEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('GlobalSerialA', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('GlobalSerialB', { event_concurrency: 'global-serial' })

  let in_flight = 0
  let max_in_flight = 0
  const starts: string[] = []

  const handler = async (event: InstanceType<typeof SerialEvent>) => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    starts.push(`${event.source}:${event.order}`)
    await sleep(10)
    in_flight -= 1
  }

  bus_a.on(SerialEvent, handler)
  bus_b.on(SerialEvent, handler)

  for (let i = 0; i < 3; i += 1) {
    bus_a.dispatch(SerialEvent({ order: i, source: 'a' }))
    bus_b.dispatch(SerialEvent({ order: i, source: 'b' }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight, 1)

  const starts_a = starts.filter((value) => value.startsWith('a:')).map((value) => Number(value.split(':')[1]))
  const starts_b = starts.filter((value) => value.startsWith('b:')).map((value) => Number(value.split(':')[1]))

  assert.deepEqual(starts_a, [0, 1, 2])
  assert.deepEqual(starts_b, [0, 1, 2])
})

test('global-serial: awaited child jumps ahead of queued events across buses', async () => {
  const ParentEvent = BaseEvent.extend('ParentEvent', {})
  const ChildEvent = BaseEvent.extend('ChildEvent', {})
  const QueuedEvent = BaseEvent.extend('QueuedEvent', {})

  const bus_a = new EventBus('GlobalSerialParent', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('GlobalSerialChild', { event_concurrency: 'global-serial' })

  const order: string[] = []

  bus_b.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(5)
    order.push('child_end')
  })

  bus_b.on(QueuedEvent, async () => {
    order.push('queued_start')
    await sleep(1)
    order.push('queued_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus_b.emit(QueuedEvent({}))
    // Emit through the scoped proxy so parent tracking is set up,
    // then also dispatch to bus_b for cross-bus processing.
    const child = event.bus?.emit(ChildEvent({}))!
    bus_b.dispatch(child)
    order.push('child_dispatched')
    await child.done()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.dispatch(ParentEvent({}))
  await parent.done()
  await bus_b.waitUntilIdle()

  const child_start_idx = order.indexOf('child_start')
  const child_end_idx = order.indexOf('child_end')
  const queued_start_idx = order.indexOf('queued_start')

  assert.ok(child_start_idx !== -1)
  assert.ok(child_end_idx !== -1)
  assert.ok(queued_start_idx !== -1)
  assert.ok(child_start_idx < queued_start_idx)
  assert.ok(child_end_idx < queued_start_idx)
})

test('global-serial: handler semaphore serializes handlers across buses', async () => {
  const HandlerEvent = BaseEvent.extend('HandlerEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('GlobalHandlerA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'global-serial',
  })
  const bus_b = new EventBus('GlobalHandlerB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'global-serial',
  })

  let in_flight = 0
  let max_in_flight = 0

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await sleep(5)
    in_flight -= 1
  }

  bus_a.on(HandlerEvent, handler)
  bus_b.on(HandlerEvent, handler)

  for (let i = 0; i < 4; i += 1) {
    bus_a.dispatch(HandlerEvent({ order: i, source: 'a' }))
    bus_b.dispatch(HandlerEvent({ order: i, source: 'b' }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('bus-serial: events serialize per bus but overlap across buses', async () => {
  const SerialEvent = BaseEvent.extend('SerialPerBusEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('BusSerialA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialB', { event_concurrency: 'bus-serial' })

  let in_flight_global = 0
  let max_in_flight_global = 0
  let in_flight_a = 0
  let in_flight_b = 0
  let max_in_flight_a = 0
  let max_in_flight_b = 0

  let resolve_b_started: (() => void) | null = null
  const b_started = new Promise<void>((resolve) => {
    resolve_b_started = resolve
  })

  bus_a.on(SerialEvent, async () => {
    in_flight_global += 1
    in_flight_a += 1
    max_in_flight_global = Math.max(max_in_flight_global, in_flight_global)
    max_in_flight_a = Math.max(max_in_flight_a, in_flight_a)
    await b_started
    await sleep(10)
    in_flight_global -= 1
    in_flight_a -= 1
  })

  bus_b.on(SerialEvent, async () => {
    in_flight_global += 1
    in_flight_b += 1
    max_in_flight_global = Math.max(max_in_flight_global, in_flight_global)
    max_in_flight_b = Math.max(max_in_flight_b, in_flight_b)
    if (resolve_b_started) {
      resolve_b_started()
      resolve_b_started = null
    }
    await sleep(10)
    in_flight_global -= 1
    in_flight_b -= 1
  })

  bus_a.dispatch(SerialEvent({ order: 0, source: 'a' }))
  bus_b.dispatch(SerialEvent({ order: 0, source: 'b' }))

  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  assert.equal(max_in_flight_a, 1)
  assert.equal(max_in_flight_b, 1)
  assert.ok(max_in_flight_global >= 2)
})

test('bus-serial: FIFO order preserved per bus with interleaving', async () => {
  const SerialEvent = BaseEvent.extend('SerialInterleavedEvent', {
    order: z.number(),
    source: z.string(),
  })

  const bus_a = new EventBus('BusSerialOrderA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialOrderB', { event_concurrency: 'bus-serial' })

  const starts_a: number[] = []
  const starts_b: number[] = []

  bus_a.on(SerialEvent, async (event) => {
    starts_a.push(event.order)
    await sleep(2)
  })

  bus_b.on(SerialEvent, async (event) => {
    starts_b.push(event.order)
    await sleep(2)
  })

  for (let i = 0; i < 4; i += 1) {
    bus_a.dispatch(SerialEvent({ order: i, source: 'a' }))
    bus_b.dispatch(SerialEvent({ order: i, source: 'b' }))
  }

  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  assert.deepEqual(starts_a, [0, 1, 2, 3])
  assert.deepEqual(starts_b, [0, 1, 2, 3])
})

test('bus-serial: awaiting child on one bus does not block other bus queue', async () => {
  const ParentEvent = BaseEvent.extend('BusSerialParent', {})
  const ChildEvent = BaseEvent.extend('BusSerialChild', {})
  const OtherEvent = BaseEvent.extend('BusSerialOther', {})

  const bus_a = new EventBus('BusSerialParentBus', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('BusSerialOtherBus', { event_concurrency: 'bus-serial' })

  const order: string[] = []

  bus_a.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(10)
    order.push('child_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    const child = event.bus?.emit(ChildEvent({}))!
    await child.done()
    order.push('parent_end')
  })

  bus_b.on(OtherEvent, async () => {
    order.push('other_start')
    await sleep(2)
    order.push('other_end')
  })

  const parent = bus_a.dispatch(ParentEvent({}))
  await sleep(0)
  bus_b.dispatch(OtherEvent({}))

  await parent.done()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  const other_start_idx = order.indexOf('other_start')
  const parent_end_idx = order.indexOf('parent_end')
  assert.ok(other_start_idx !== -1)
  assert.ok(parent_end_idx !== -1)
  assert.ok(other_start_idx < parent_end_idx)
})

test('parallel: events overlap on same bus when event_concurrency is parallel', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEvent', { order: z.number() })
  const bus = new EventBus('ParallelEventBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()
  setTimeout(() => resolve(), 20)

  bus.on(ParallelEvent, async (_event) => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    await sleep(10)
    in_flight -= 1
  })

  bus.dispatch(ParallelEvent({ order: 0 }))
  bus.dispatch(ParallelEvent({ order: 1 }))

  await bus.waitUntilIdle()
  assert.ok(max_in_flight >= 2)
})

test('parallel: handlers overlap for same event when event_handler_concurrency is parallel', async () => {
  const ParallelHandlerEvent = BaseEvent.extend('ParallelHandlerEvent', {})
  const bus = new EventBus('ParallelHandlerBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler_a = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  const handler_b = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(ParallelHandlerEvent, handler_a)
  bus.on(ParallelHandlerEvent, handler_b)

  const event = bus.dispatch(ParallelHandlerEvent({}))
  await sleep(0)
  resolve()
  await event.done()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('parallel: global-serial handler semaphore still serializes across buses', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventGlobalHandler', {
    source: z.string(),
  })

  const bus_a = new EventBus('ParallelHandlerGlobalA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'global-serial',
  })
  const bus_b = new EventBus('ParallelHandlerGlobalB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'global-serial',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(ParallelEvent, handler)
  bus_b.on(ParallelEvent, handler)

  bus_a.dispatch(ParallelEvent({ source: 'a' }))
  bus_b.dispatch(ParallelEvent({ source: 'b' }))

  await sleep(0)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  assert.equal(max_in_flight, 1)
})

test('precedence: event event_handler_concurrency overrides handler options', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEvent', {
    event_handler_concurrency: z.literal('bus-serial'),
  })
  const bus = new EventBus('OverrideBus', { event_handler_concurrency: 'parallel' })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(OverrideEvent, handler, { event_handler_concurrency: 'parallel' })
  bus.on(OverrideEvent, handler, { event_handler_concurrency: 'parallel' })

  const event = bus.dispatch(OverrideEvent({ event_handler_concurrency: 'bus-serial' }))
  await sleep(0)
  resolve()
  await event.done()
  await bus.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('precedence: handler options override bus defaults when event has no override', async () => {
  const OptionEvent = BaseEvent.extend('OptionEvent', {})
  const bus = new EventBus('OptionBus', { event_handler_concurrency: 'bus-serial' })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler_a = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  const handler_b = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(OptionEvent, handler_a, { event_handler_concurrency: 'parallel' })
  bus.on(OptionEvent, handler_b, { event_handler_concurrency: 'parallel' })

  const event = bus.dispatch(OptionEvent({}))
  await sleep(0)
  resolve()
  await event.done()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('precedence: event event_handler_concurrency overrides handler options to parallel', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEventParallelHandlers', {
    event_handler_concurrency: z.literal('parallel'),
  })
  const bus = new EventBus('OverrideParallelHandlersBus', { event_handler_concurrency: 'bus-serial' })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(OverrideEvent, handler, { event_handler_concurrency: 'bus-serial' })
  bus.on(OverrideEvent, handler, { event_handler_concurrency: 'bus-serial' })

  const event = bus.dispatch(OverrideEvent({ event_handler_concurrency: 'parallel' }))
  await sleep(0)
  resolve()
  await event.done()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('precedence: event event_concurrency overrides bus defaults to parallel', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEventParallelEvents', {
    event_concurrency: z.literal('parallel'),
    order: z.number(),
  })
  const bus = new EventBus('OverrideParallelEventsBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  bus.on(OverrideEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus.dispatch(OverrideEvent({ order: 0, event_concurrency: 'parallel' }))
  bus.dispatch(OverrideEvent({ order: 1, event_concurrency: 'parallel' }))

  await sleep(0)
  resolve()
  await bus.waitUntilIdle()

  assert.ok(max_in_flight >= 2)
})

test('precedence: event event_concurrency overrides bus defaults to bus-serial', async () => {
  const OverrideEvent = BaseEvent.extend('OverrideEventBusSerial', {
    event_concurrency: z.literal('bus-serial'),
    order: z.number(),
  })
  const bus = new EventBus('OverrideBusSerialEventsBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  bus.on(OverrideEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus.dispatch(OverrideEvent({ order: 0, event_concurrency: 'bus-serial' }))
  bus.dispatch(OverrideEvent({ order: 1, event_concurrency: 'bus-serial' }))

  await sleep(0)
  assert.equal(max_in_flight, 1)
  resolve()
  await bus.waitUntilIdle()
})

test('global-serial + handler parallel: handlers overlap but events do not across buses', async () => {
  const SerialParallelEvent = BaseEvent.extend('GlobalSerialParallelHandlers', {})

  const bus_a = new EventBus('GlobalSerialParallelA', {
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'parallel',
  })
  const bus_b = new EventBus('GlobalSerialParallelB', {
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(SerialParallelEvent, handler)
  bus_a.on(SerialParallelEvent, handler)
  bus_b.on(SerialParallelEvent, handler)
  bus_b.on(SerialParallelEvent, handler)

  bus_a.dispatch(SerialParallelEvent({}))
  bus_b.dispatch(SerialParallelEvent({}))

  await sleep(0)
  assert.equal(max_in_flight, 2)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('event parallel + handler bus-serial: handlers serialize within a bus across events', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventsSerialHandlers', { order: z.number() })
  const bus = new EventBus('ParallelEventsSerialHandlersBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'bus-serial',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  bus.on(ParallelEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  })

  bus.dispatch(ParallelEvent({ order: 0 }))
  bus.dispatch(ParallelEvent({ order: 1 }))

  await sleep(0)
  assert.equal(max_in_flight, 1)
  resolve()
  await bus.waitUntilIdle()
})

test('event parallel + handler bus-serial: handlers overlap across buses', async () => {
  const ParallelEvent = BaseEvent.extend('ParallelEventsBusHandlers', { source: z.string() })

  const bus_a = new EventBus('ParallelBusHandlersA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('ParallelBusHandlersB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'bus-serial',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(ParallelEvent, handler)
  bus_b.on(ParallelEvent, handler)

  bus_a.dispatch(ParallelEvent({ source: 'a' }))
  bus_b.dispatch(ParallelEvent({ source: 'b' }))

  await sleep(0)
  assert.ok(max_in_flight >= 2)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('handler options can enforce global-serial even when bus defaults to parallel', async () => {
  const HandlerEvent = BaseEvent.extend('HandlerOptionsGlobalSerial', { source: z.string() })

  const bus_a = new EventBus('HandlerOptionsGlobalA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  const bus_b = new EventBus('HandlerOptionsGlobalB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus_a.on(HandlerEvent, handler, { event_handler_concurrency: 'global-serial' })
  bus_b.on(HandlerEvent, handler, { event_handler_concurrency: 'global-serial' })

  bus_a.dispatch(HandlerEvent({ source: 'a' }))
  bus_b.dispatch(HandlerEvent({ source: 'b' }))

  await sleep(0)
  assert.equal(max_in_flight, 1)
  resolve()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])
})

test('auto: event_concurrency auto resolves to bus defaults', async () => {
  const AutoEvent = BaseEvent.extend('AutoEvent', {
    event_concurrency: z.literal('auto'),
  })
  const bus = new EventBus('AutoBus', { event_concurrency: 'bus-serial' })

  let in_flight = 0
  let max_in_flight = 0

  bus.on(AutoEvent, async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await sleep(5)
    in_flight -= 1
  })

  bus.dispatch(AutoEvent({ event_concurrency: 'auto' }))
  bus.dispatch(AutoEvent({ event_concurrency: 'auto' }))

  await bus.waitUntilIdle()
  assert.equal(max_in_flight, 1)
})

test('auto: event_handler_concurrency auto resolves to bus defaults', async () => {
  const AutoHandlerEvent = BaseEvent.extend('AutoHandlerEvent', {
    event_handler_concurrency: z.literal('auto'),
  })
  const bus = new EventBus('AutoHandlerBus', { event_handler_concurrency: 'bus-serial' })

  let in_flight = 0
  let max_in_flight = 0
  const { promise, resolve } = withResolvers<void>()

  const handler = async () => {
    in_flight += 1
    max_in_flight = Math.max(max_in_flight, in_flight)
    await promise
    in_flight -= 1
  }

  bus.on(AutoHandlerEvent, handler)
  bus.on(AutoHandlerEvent, handler)

  const event = bus.dispatch(AutoHandlerEvent({ event_handler_concurrency: 'auto' }))
  await sleep(0)
  resolve()
  await event.done()
  await bus.waitUntilIdle()

  assert.equal(max_in_flight, 1)
})

test('queue-jump: awaited child preempts queued sibling on same bus', async () => {
  const ParentEvent = BaseEvent.extend('QueueJumpParent', {})
  const ChildEvent = BaseEvent.extend('QueueJumpChild', {})
  const SiblingEvent = BaseEvent.extend('QueueJumpSibling', {})

  const bus = new EventBus('QueueJumpBus', { event_concurrency: 'bus-serial' })
  const order: string[] = []

  bus.on(ChildEvent, async () => {
    order.push('child_start')
    await sleep(5)
    order.push('child_end')
  })

  bus.on(SiblingEvent, async () => {
    order.push('sibling_start')
    await sleep(1)
    order.push('sibling_end')
  })

  bus.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus.emit(SiblingEvent({}))
    const child = event.bus?.emit(ChildEvent({}))!
    order.push('child_dispatched')
    await child.done()
    order.push('child_awaited')
    order.push('parent_end')
  })

  const parent = bus.dispatch(ParentEvent({}))
  await parent.done()
  await bus.waitUntilIdle()

  const child_start_idx = order.indexOf('child_start')
  const child_end_idx = order.indexOf('child_end')
  const sibling_start_idx = order.indexOf('sibling_start')

  assert.ok(child_start_idx !== -1)
  assert.ok(child_end_idx !== -1)
  assert.ok(sibling_start_idx !== -1)
  assert.ok(child_start_idx < sibling_start_idx)
  assert.ok(child_end_idx < sibling_start_idx)
})

test('queue-jump: same event handlers on separate buses stay isolated without forwarding', async () => {
  const ParentEvent = BaseEvent.extend('QueueJumpIsolatedParent', {})
  const SharedEvent = BaseEvent.extend('QueueJumpIsolatedShared', {})
  const SiblingEvent = BaseEvent.extend('QueueJumpIsolatedSibling', {})

  const bus_a = new EventBus('QueueJumpIsolatedA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('QueueJumpIsolatedB', { event_concurrency: 'bus-serial' })

  const order: string[] = []
  let bus_a_shared_runs = 0
  let bus_b_shared_runs = 0

  bus_a.on(SharedEvent, async () => {
    bus_a_shared_runs += 1
    order.push('bus_a_shared_start')
    await sleep(2)
    order.push('bus_a_shared_end')
  })

  bus_b.on(SharedEvent, async () => {
    bus_b_shared_runs += 1
    order.push('bus_b_shared_start')
    await sleep(2)
    order.push('bus_b_shared_end')
  })

  bus_a.on(SiblingEvent, async () => {
    order.push('bus_a_sibling_start')
    await sleep(1)
    order.push('bus_a_sibling_end')
  })

  bus_a.on(ParentEvent, async (event) => {
    order.push('parent_start')
    bus_a.emit(SiblingEvent({}))
    const shared = event.bus?.emit(SharedEvent({}))!
    order.push('shared_dispatched')
    await shared.done()
    order.push('shared_awaited')
    order.push('parent_end')
  })

  const parent = bus_a.dispatch(ParentEvent({}))
  await parent.done()
  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  assert.equal(bus_a_shared_runs, 1)
  assert.equal(bus_b_shared_runs, 0)
  assert.equal(order.includes('bus_b_shared_start'), false)

  const bus_a_shared_end_idx = order.indexOf('bus_a_shared_end')
  const bus_a_sibling_start_idx = order.indexOf('bus_a_sibling_start')
  assert.ok(bus_a_shared_end_idx !== -1)
  assert.ok(bus_a_sibling_start_idx !== -1)
  assert.ok(bus_a_shared_end_idx < bus_a_sibling_start_idx)
})

test('queue-jump: awaiting in-flight event does not double-run handlers', async () => {
  const InFlightEvent = BaseEvent.extend('InFlightEvent', {})
  const bus = new EventBus('InFlightBus', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  let handler_runs = 0
  let resolve_started: (() => void) | null = null
  const started = new Promise<void>((resolve) => {
    resolve_started = resolve
  })
  const { promise: release_child, resolve: resolve_child } = withResolvers<void>()

  bus.on(InFlightEvent, async () => {
    handler_runs += 1
    if (resolve_started) {
      resolve_started()
      resolve_started = null
    }
    await release_child
  })

  const child = bus.dispatch(InFlightEvent({}))
  await started

  let done_resolved = false
  const done_promise = child.done().then(() => {
    done_resolved = true
  })

  await sleep(0)
  assert.equal(done_resolved, false)

  resolve_child()
  await done_promise
  await bus.waitUntilIdle()

  assert.equal(handler_runs, 1)
})

test('edge-case: event with no handlers completes immediately', async () => {
  const NoHandlerEvent = BaseEvent.extend('NoHandlerEvent', {})
  const bus = new EventBus('NoHandlerBus')

  const event = bus.dispatch(NoHandlerEvent({}))
  await event.done()
  await bus.waitUntilIdle()

  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_pending_bus_count, 0)
})

test('fifo: forwarded events preserve order on target bus (bus-serial)', async () => {
  const OrderedEvent = BaseEvent.extend('ForwardOrderEvent', { order: z.number() })

  const bus_a = new EventBus('ForwardOrderA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('ForwardOrderB', { event_concurrency: 'bus-serial' })

  const order_a: number[] = []
  const order_b: number[] = []

  bus_a.on(OrderedEvent, async (event) => {
    order_a.push(event.order)
    bus_b.dispatch(event)
    await sleep(2)
  })

  bus_b.on(OrderedEvent, async (event) => {
    const bus_b_results = Array.from(event.event_results.values()).filter((result) => result.eventbus_name === 'ForwardOrderB')
    const in_flight = bus_b_results.filter((result) => result.status === 'pending' || result.status === 'started')
    assert.ok(in_flight.length <= 1)
    order_b.push(event.order)
    await sleep(1)
  })

  for (let i = 0; i < 5; i += 1) {
    bus_a.dispatch(OrderedEvent({ order: i }))
  }

  await Promise.all([bus_a.waitUntilIdle(), bus_b.waitUntilIdle()])

  const history_orders = Array.from(bus_b.event_history.values()).map((event) => (event as { order?: number }).order)
  const results_sizes = Array.from(bus_b.event_history.values()).map((event) => event.event_results.size)
  const bus_b_result_counts = Array.from(bus_b.event_history.values()).map(
    (event) => Array.from(event.event_results.values()).filter((result) => result.eventbus_name === 'ForwardOrderB').length
  )
  const processed_flags = Array.from(bus_b.event_history.values()).map((event) =>
    Array.from(event.event_results.values())
      .filter((result) => result.eventbus_name === 'ForwardOrderB')
      .every((result) => result.status === 'completed' || result.status === 'error')
  )
  const pending_counts = Array.from(bus_b.event_history.values()).map(
    (event) => Array.from(event.event_results.values()).filter((result) => result.status === 'pending').length
  )
  assert.deepEqual(order_a, [0, 1, 2, 3, 4])
  assert.deepEqual(order_b, [0, 1, 2, 3, 4])
  assert.deepEqual(history_orders, [0, 1, 2, 3, 4])
  assert.deepEqual(results_sizes, [2, 2, 2, 2, 2])
  assert.deepEqual(bus_b_result_counts, [1, 1, 1, 1, 1])
  assert.deepEqual(processed_flags, [true, true, true, true, true])
  assert.deepEqual(pending_counts, [0, 0, 0, 0, 0])
})

test('fifo: forwarded events preserve order across chained buses (bus-serial)', async () => {
  const OrderedEvent = BaseEvent.extend('ForwardChainEvent', { order: z.number() })

  const bus_a = new EventBus('ForwardChainA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('ForwardChainB', { event_concurrency: 'bus-serial' })
  const bus_c = new EventBus('ForwardChainC', { event_concurrency: 'bus-serial' })

  const order_c: number[] = []

  bus_b.on(OrderedEvent, async () => {
    await sleep(2)
  })

  bus_c.on(OrderedEvent, async (event) => {
    order_c.push(event.order)
    await sleep(1)
  })

  bus_a.on('*', bus_b.dispatch)
  bus_b.on('*', bus_c.dispatch)

  for (let i = 0; i < 6; i += 1) {
    bus_a.dispatch(OrderedEvent({ order: i }))
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  assert.deepEqual(order_c, [0, 1, 2, 3, 4, 5])
})

test('find: past returns most recent completed event (bus-scoped)', async () => {
  const DebounceEvent = BaseEvent.extend('FindPastEvent', { value: z.number() })
  const bus = new EventBus('FindPastBus')

  bus.on(DebounceEvent, async () => {})

  bus.dispatch(DebounceEvent({ value: 1 }))
  bus.dispatch(DebounceEvent({ value: 2 }))

  await bus.waitUntilIdle()

  const found = await bus.find(DebounceEvent, { past: true, future: false })
  assert.ok(found)
  assert.equal(found.value, 2)
  assert.equal(found.event_status, 'completed')
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindPastBus')
  assert.equal(typeof found.bus.dispatch, 'function')
})

test('find: future returns in-flight event and done waits', async () => {
  const DebounceEvent = BaseEvent.extend('FindFutureEvent', { value: z.number() })
  const bus = new EventBus('FindFutureBus')
  const { promise, resolve } = withResolvers<void>()

  bus.on(DebounceEvent, async () => {
    await promise
  })

  bus.dispatch(DebounceEvent({ value: 1 }))

  const found = await bus.find(DebounceEvent, { past: false, future: true })
  assert.ok(found)
  assert.equal(found.value, 1)
  assert.ok(found.event_status !== 'completed')
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindFutureBus')

  resolve()
  const completed = await found.done()
  assert.equal(completed.event_status, 'completed')
})

test('find: future waits for next event when none in-flight', async () => {
  const DebounceEvent = BaseEvent.extend('FindWaitEvent', { value: z.number() })
  const bus = new EventBus('FindWaitBus')

  bus.on(DebounceEvent, async () => {})

  setTimeout(() => {
    bus.dispatch(DebounceEvent({ value: 99 }))
  }, 10)

  const found = await bus.find(DebounceEvent, { past: false, future: 0.2 })
  assert.ok(found)
  assert.equal(found.value, 99)
  assert.ok(found.bus)
  assert.equal(found.bus.name, 'FindWaitBus')
  await found.done()
})

test('find: most recent wins across completed and in-flight', async () => {
  const DebounceEvent = BaseEvent.extend('FindMostRecentEvent', { value: z.number() })
  const bus = new EventBus('FindMostRecentBus')
  const { promise, resolve } = withResolvers<void>()

  bus.on(DebounceEvent, async (event) => {
    if (event.value === 2) {
      await promise
    }
  })

  bus.dispatch(DebounceEvent({ value: 1 }))
  await bus.waitUntilIdle()

  bus.dispatch(DebounceEvent({ value: 2 }))

  const found = await bus.find(DebounceEvent, { past: true, future: true })
  assert.ok(found)
  assert.equal(found.value, 2)
  assert.ok(found.event_status !== 'completed')

  resolve()
  await found.done()
})
