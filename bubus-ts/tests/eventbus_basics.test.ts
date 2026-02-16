import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'
import { LockManager } from '../src/lock_manager.js'
import { z } from 'zod'

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

// ─── Constructor defaults ────────────────────────────────────────────────────

test('EventBus initializes with correct defaults', async () => {
  const bus = new EventBus('DefaultsBus')

  assert.equal(bus.name, 'DefaultsBus')
  assert.equal(bus.max_history_size, 100)
  assert.equal(bus.max_history_drop, false)
  assert.equal(bus.event_concurrency_default, 'bus-serial')
  assert.equal(bus.event_handler_concurrency_default, 'serial')
  assert.equal(bus.event_handler_completion_default, 'all')
  assert.equal(bus.event_timeout_default, 60)
  assert.equal(bus.event_history.size, 0)
  assert.ok(EventBus._all_instances.has(bus))
  await bus.waitUntilIdle()
})

test('EventBus applies custom options', () => {
  const bus = new EventBus('CustomBus', {
    max_history_size: 500,
    max_history_drop: false,
    event_concurrency: 'parallel',
    event_handler_concurrency: 'serial',
    event_handler_completion: 'first',
    event_timeout: 30,
  })

  assert.equal(bus.max_history_size, 500)
  assert.equal(bus.max_history_drop, false)
  assert.equal(bus.event_concurrency_default, 'parallel')
  assert.equal(bus.event_handler_concurrency_default, 'serial')
  assert.equal(bus.event_handler_completion_default, 'first')
  assert.equal(bus.event_timeout_default, 30)
})

test('EventBus with null max_history_size means unlimited', () => {
  const bus = new EventBus('UnlimitedBus', { max_history_size: null })
  assert.equal(bus.max_history_size, null)
})

test('EventBus with null event_timeout disables timeouts', () => {
  const bus = new EventBus('NoTimeoutBus', { event_timeout: null })
  assert.equal(bus.event_timeout_default, null)
})

test('EventBus auto-generates name when not provided', () => {
  const bus = new EventBus()
  assert.equal(bus.name, 'EventBus')
})

test('EventBus toString and toJSON/fromJSON roundtrip full state', async () => {
  const bus = new EventBus('SerializableBus', {
    id: '018f8e40-1234-7000-8000-000000001234',
    max_history_size: 500,
    max_history_drop: false,
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'first',
    event_timeout: null,
    event_handler_slow_timeout: 12,
    event_slow_timeout: 34,
    event_handler_detect_file_paths: false,
  })
  const SerializableEvent = BaseEvent.extend('SerializableEvent', {})

  bus.on(SerializableEvent, async () => {
    await delay(20)
    return 'ok'
  })

  // keep one event pending so queue/in-flight related state is serializable
  const release_pause = bus.locks.requestRunloopPause()
  const pending_event = bus.dispatch(SerializableEvent({ event_timeout: 11 }))
  await Promise.resolve()

  assert.equal(bus.toString(), 'SerializableBus#1234')

  const json = bus.toJSON()
  assert.equal(json.id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(json.name, 'SerializableBus')
  assert.equal(json.max_history_size, 500)
  assert.equal(json.max_history_drop, false)
  assert.equal(json.event_concurrency, 'parallel')
  assert.equal(json.event_handler_concurrency, 'parallel')
  assert.equal(json.event_handler_completion, 'first')
  assert.equal(json.event_timeout, null)
  assert.equal(json.event_handler_slow_timeout, 12)
  assert.equal(json.event_slow_timeout, 34)
  assert.equal(json.event_handler_detect_file_paths, false)
  assert.equal(Object.keys(json.handlers).length, 1)
  assert.equal(Object.keys(json.handlers_by_key).length, 1)
  assert.equal(Array.isArray(json.handlers_by_key.SerializableEvent), true)
  assert.equal(Object.keys(json.event_history).length, 1)
  assert.equal((json.event_history[pending_event.event_id] as Record<string, unknown>).event_id, pending_event.event_id)
  assert.equal(json.pending_event_queue.length, 1)
  assert.equal(json.pending_event_queue[0], pending_event.event_id)

  const restored = EventBus.fromJSON(json)
  assert.equal(restored.id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(restored.name, 'SerializableBus')
  assert.equal(restored.max_history_size, 500)
  assert.equal(restored.max_history_drop, false)
  assert.equal(restored.event_concurrency_default, 'parallel')
  assert.equal(restored.event_handler_concurrency_default, 'parallel')
  assert.equal(restored.event_handler_completion_default, 'first')
  assert.equal(restored.event_timeout_default, null)
  assert.equal(restored.event_handler_slow_timeout, 12)
  assert.equal(restored.event_slow_timeout, 34)
  assert.equal(restored.event_handler_detect_file_paths, false)
  assert.equal(restored.handlers.size, 1)
  assert.equal(restored.handlers_by_key.get('SerializableEvent')?.length, 1)
  assert.equal(restored.event_history.size, 1)
  assert.equal(restored.pending_event_queue.length, 1)
  assert.equal(restored.pending_event_queue[0].event_id, pending_event.event_id)
  assert.equal(restored.runloop_running, false)

  release_pause()
  await pending_event.done()
})

test('EventBus.fromJSON recreates missing handler entries from event_result metadata', async () => {
  const bus = new EventBus('MissingHandlerHydrationBus', {
    event_handler_detect_file_paths: false,
  })
  const SerializableEvent = BaseEvent.extend('MissingHandlerHydrationEvent', {})

  bus.on(SerializableEvent, () => 'ok')
  const event = bus.dispatch(SerializableEvent({}))
  await event.done()

  const handler_id = Array.from(event.event_results.values())[0].handler_id
  const json = bus.toJSON()
  json.handlers = {}
  json.handlers_by_key = {}

  const restored = EventBus.fromJSON(json)
  const restored_event = restored.event_history.get(event.event_id)
  assert.ok(restored_event)
  assert.ok(restored.handlers.has(handler_id))
  const restored_result = restored_event!.event_results.get(handler_id)
  assert.ok(restored_result)
  assert.equal(restored_result!.handler, restored.handlers.get(handler_id))
})

test('EventBus exposes locks API surface', () => {
  const bus = new EventBus('GateSurfaceBus')
  const locks = bus.locks as unknown as Record<string, unknown>

  assert.equal(typeof locks.requestRunloopPause, 'function')
  assert.equal(typeof locks.waitUntilRunloopResumed, 'function')
  assert.equal(typeof locks.isPaused, 'function')
  assert.equal(typeof locks.waitForIdle, 'function')
  assert.equal(typeof locks.notifyIdleListeners, 'function')
  assert.equal(typeof locks.getLockForEvent, 'function')
})

test('EventBus locks methods are callable and preserve lock resolution behavior', async () => {
  const bus = new EventBus('GateInvocationBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const GateEvent = BaseEvent.extend('GateInvocationEvent', {})

  const release_pause = bus.locks.requestRunloopPause()
  assert.equal(bus.locks.isPaused(), true)

  let resumed = false
  const resumed_promise = bus.locks.waitUntilRunloopResumed().then(() => {
    resumed = true
  })
  await Promise.resolve()
  assert.equal(resumed, false)

  release_pause()
  await resumed_promise
  assert.equal(bus.locks.isPaused(), false)

  const event_with_global = GateEvent({
    event_concurrency: 'global-serial',
    event_handler_concurrency: 'serial',
  })
  assert.equal(bus.locks.getLockForEvent(event_with_global), LockManager.global_event_lock)
  const handler_lock = event_with_global.getHandlerLock(bus.event_handler_concurrency_default)
  assert.ok(handler_lock)

  const event_with_parallel = GateEvent({
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  assert.equal(bus.locks.getLockForEvent(event_with_parallel), null)
  assert.equal(event_with_parallel.getHandlerLock(bus.event_handler_concurrency_default), null)

  const another_serial_event = GateEvent({ event_handler_concurrency: 'serial' })
  const another_lock = another_serial_event.getHandlerLock(bus.event_handler_concurrency_default)
  assert.notEqual(handler_lock, another_lock)

  bus.dispatch(GateEvent({}))
  bus.locks.notifyIdleListeners()
  await bus.locks.waitForIdle()
})

test('BaseEvent lifecycle methods are callable and preserve lifecycle behavior', async () => {
  const LifecycleEvent = BaseEvent.extend('LifecycleMethodInvocationEvent', {})

  const standalone = LifecycleEvent({})
  standalone.markStarted()
  assert.equal(standalone.event_status, 'started')
  standalone.markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  await standalone.waitForCompletion()

  const bus = new EventBus('LifecycleMethodInvocationBus')
  const dispatched = bus.dispatch(LifecycleEvent({}))
  await dispatched.waitForCompletion()
  assert.equal(dispatched.event_status, 'completed')
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('RuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('RuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.dispatch(RuntimeEvent({}))
  await event.done()

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_ts, 'number')
  assert.equal(typeof json.event_started_ts, 'number')
  assert.equal(typeof json.event_completed_ts, 'number')
  assert.equal(json.event_pending_bus_count, 0)
  assert.ok(Array.isArray(json.event_results))
  const json_results = json.event_results as Array<Record<string, unknown>>
  assert.equal(json_results.length, 1)
  assert.equal(json_results[0].status, 'completed')
  assert.equal(json_results[0].result, 'ok')
  assert.equal(json_results[0].handler_id, Array.from(event.event_results.values())[0].handler_id)

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_ts, event.event_created_ts)
  assert.equal(restored.event_pending_bus_count, 0)
  assert.equal(restored.event_results.size, 1)
  const restored_result = Array.from(restored.event_results.values())[0]
  assert.equal(restored_result.status, 'completed')
  assert.equal(restored_result.result, 'ok')
})

test('event_version supports defaults, extend-time defaults, runtime override, and JSON roundtrip', () => {
  const DefaultEvent = BaseEvent.extend('DefaultVersionEvent', {})
  const ExtendVersionEvent = BaseEvent.extend('ExtendVersionEvent', { event_version: '1.2.3' })

  class StaticVersionEvent extends BaseEvent {
    static event_type = 'StaticVersionEvent'
    static event_version = '4.5.6'
  }

  const default_event = DefaultEvent({})
  assert.equal(default_event.event_version, '0.0.1')

  const extended_default = ExtendVersionEvent({})
  assert.equal(extended_default.event_version, '1.2.3')

  const static_default = new StaticVersionEvent({})
  assert.equal(static_default.event_version, '4.5.6')

  const runtime_override = ExtendVersionEvent({ event_version: '9.9.9' })
  assert.equal(runtime_override.event_version, '9.9.9')

  const restored = BaseEvent.fromJSON(runtime_override.toJSON())
  assert.equal(restored.event_version, '9.9.9')
})

test('fromJSON accepts event_parent_id: null and preserves it in toJSON output', () => {
  const missing_field_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001233',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'MissingParentIdEvent',
    event_timeout: null,
  })
  assert.equal(missing_field_event.event_parent_id, null)

  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001234',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullParentIdEvent',
    event_parent_id: null,
    event_timeout: null,
  })

  assert.equal(event.event_parent_id, null)
  assert.equal((event.toJSON() as Record<string, unknown>).event_parent_id, null)
})

test('event_emitted_by_handler_id defaults to null and accepts null in fromJSON', () => {
  const fresh_event = BaseEvent.extend('NullEmittedByDefaultEvent')({})
  assert.equal(fresh_event.event_emitted_by_handler_id, null)

  const missing_field_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001239',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'MissingEmittedByIdEvent',
    event_timeout: null,
  })
  assert.equal(missing_field_event.event_emitted_by_handler_id, null)

  const json_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'NullEmittedByIdEvent',
    event_emitted_by_handler_id: null,
    event_timeout: null,
  })

  assert.equal(json_event.event_emitted_by_handler_id, null)
  assert.equal((json_event.toJSON() as Record<string, unknown>).event_emitted_by_handler_id, null)
})

test('fromJSON deserializes event_result_type and toJSON reserializes schema', () => {
  const raw_schema = { type: 'integer' }
  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001235',
    event_created_at: new Date('2025-01-01T00:00:01.000Z').toISOString(),
    event_type: 'RawSchemaEvent',
    event_timeout: null,
    event_result_type: raw_schema,
  })
  const json = event.toJSON() as Record<string, unknown>
  assert.equal(typeof (event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  assert.equal(typeof json.event_result_type, 'object')
  assert.ok(['integer', 'number'].includes(String((json.event_result_type as { type?: unknown }).type)))
})

test('fromJSON reconstructs integer and null schemas for runtime validation', async () => {
  const bus = new EventBus('SchemaPrimitiveRuntimeBus')

  const int_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001236',
    event_created_at: new Date('2025-01-01T00:00:02.000Z').toISOString(),
    event_type: 'RawIntegerEvent',
    event_timeout: null,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEvent', () => 123)
  await bus.dispatch(int_event).done()
  const int_result = Array.from(int_event.event_results.values())[0]
  assert.equal(int_result.status, 'completed')

  const int_bad_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001237',
    event_created_at: new Date('2025-01-01T00:00:03.000Z').toISOString(),
    event_type: 'RawIntegerEventBad',
    event_timeout: null,
    event_result_type: { type: 'integer' },
  })
  bus.on('RawIntegerEventBad', () => 1.5)
  await bus.dispatch(int_bad_event).done()
  const int_bad_result = Array.from(int_bad_event.event_results.values())[0]
  assert.equal(int_bad_result.status, 'error')

  const null_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001238',
    event_created_at: new Date('2025-01-01T00:00:04.000Z').toISOString(),
    event_type: 'RawNullEvent',
    event_timeout: null,
    event_result_type: { type: 'null' },
  })
  bus.on('RawNullEvent', () => null)
  await bus.dispatch(null_event).done()
  const null_result = Array.from(null_event.event_results.values())[0]
  assert.equal(null_result.status, 'completed')

  await bus.waitUntilIdle()
})

test('fromJSON reconstructs nested object/array result schemas', async () => {
  const bus = new EventBus('SchemaNestedRuntimeBus')
  const raw_nested_schema = {
    type: 'object',
    properties: {
      items: { type: 'array', items: { type: 'integer' } },
      meta: { type: 'object', additionalProperties: { type: 'boolean' } },
    },
    required: ['items', 'meta'],
  }

  const valid_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001239',
    event_created_at: new Date('2025-01-01T00:00:05.000Z').toISOString(),
    event_type: 'RawNestedSchemaEvent',
    event_timeout: null,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEvent', () => ({ items: [1, 2, 3], meta: { ok: true } }))
  await bus.dispatch(valid_event).done()
  const valid_result = Array.from(valid_event.event_results.values())[0]
  assert.equal(valid_result.status, 'completed')

  const invalid_event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-000000001240',
    event_created_at: new Date('2025-01-01T00:00:06.000Z').toISOString(),
    event_type: 'RawNestedSchemaEventBad',
    event_timeout: null,
    event_result_type: raw_nested_schema,
  })
  bus.on('RawNestedSchemaEventBad', () => ({ items: ['bad'], meta: { ok: 'yes' } }))
  await bus.dispatch(invalid_event).done()
  const invalid_result = Array.from(invalid_event.event_results.values())[0]
  assert.equal(invalid_result.status, 'error')

  await bus.waitUntilIdle()
})

// ─── Event dispatch and status lifecycle ─────────────────────────────────────

test('dispatch returns pending event with correct initial state', async () => {
  const bus = new EventBus('LifecycleBus', { max_history_size: 100 })
  const TestEvent = BaseEvent.extend('TestEvent', { data: z.string() })

  const event = bus.dispatch(TestEvent({ data: 'hello' }))

  // Immediate state after dispatch (before any microtask runs)
  assert.equal(event.event_type, 'TestEvent')
  assert.ok(event.event_id)
  assert.ok(event.event_created_at)
  assert.equal((event as any).data, 'hello')

  // event_path should include the bus label
  const original = event._event_original ?? event
  assert.ok(original.event_path.includes(bus.label))

  await bus.waitUntilIdle()
})

test('event transitions through pending -> started -> completed', async () => {
  const bus = new EventBus('StatusBus', { max_history_size: 100 })
  const TestEvent = BaseEvent.extend('TestEvent', {})
  let status_during_handler: string | undefined

  bus.on(TestEvent, (event: BaseEvent) => {
    status_during_handler = event.event_status
    return 'done'
  })

  const event = bus.dispatch(TestEvent({}))
  const original = event._event_original ?? event

  await event.done()

  assert.equal(status_during_handler, 'started')
  assert.equal(original.event_status, 'completed')
  assert.ok(original.event_started_at, 'event_started_at should be set')
  assert.ok(original.event_completed_at, 'event_completed_at should be set')
})

test('event with no handlers completes immediately', async () => {
  const bus = new EventBus('NoHandlerBus', { max_history_size: 100 })
  const OrphanEvent = BaseEvent.extend('OrphanEvent', {})

  const event = bus.dispatch(OrphanEvent({}))
  await event.done()

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')
  assert.equal(original.event_results.size, 0)
})

// ─── Event history tracking ──────────────────────────────────────────────────

test('dispatched events appear in event_history', async () => {
  const bus = new EventBus('HistoryBus', { max_history_size: 100 })
  const EventA = BaseEvent.extend('EventA', {})
  const EventB = BaseEvent.extend('EventB', {})

  bus.dispatch(EventA({}))
  bus.dispatch(EventB({}))
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 2)
  const history = Array.from(bus.event_history.values())
  assert.equal(history[0].event_type, 'EventA')
  assert.equal(history[1].event_type, 'EventB')

  // All events are accessible by id
  for (const event of bus.event_history.values()) {
    assert.ok(bus.event_history.has(event.event_id))
  }
})

// ─── History trimming (max_history_size) ─────────────────────────────────────

test('history is trimmed to max_history_size, completed events removed first', async () => {
  const bus = new EventBus('TrimBus', { max_history_size: 5, max_history_drop: true })
  const TrimEvent = BaseEvent.extend('TrimEvent', { seq: z.number() })

  bus.on(TrimEvent, () => 'ok')

  // Dispatch 10 events; they'll process and complete in FIFO order
  for (let i = 0; i < 10; i++) {
    bus.dispatch(TrimEvent({ seq: i }))
  }
  await bus.waitUntilIdle()

  // History should be trimmed to at most max_history_size
  assert.ok(bus.event_history.size <= 5, `expected <= 5, got ${bus.event_history.size}`)

  // The remaining events should be the MOST RECENT ones (oldest completed removed first)
  const seqs = Array.from(bus.event_history.values()).map((e) => (e as any).seq as number)
  for (let i = 1; i < seqs.length; i++) {
    assert.ok(seqs[i] > seqs[i - 1], 'remaining history should be in order')
  }
})

test('unlimited history (max_history_size: null) keeps all events', async () => {
  const bus = new EventBus('UnlimitedHistBus', { max_history_size: null })
  const PingEvent = BaseEvent.extend('PingEvent', {})

  bus.on(PingEvent, () => 'pong')

  for (let i = 0; i < 150; i++) {
    bus.dispatch(PingEvent({}))
  }
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 150)

  // All completed
  for (const event of bus.event_history.values()) {
    assert.equal(event.event_status, 'completed')
  }
})

test('max_history_drop=false rejects new dispatch when history is full', async () => {
  const bus = new EventBus('NoDropHistBus', { max_history_size: 2, max_history_drop: false })
  const NoDropEvent = BaseEvent.extend('NoDropEvent', { seq: z.number() })

  bus.on(NoDropEvent, () => 'ok')

  await bus.dispatch(NoDropEvent({ seq: 1 })).done()
  await bus.dispatch(NoDropEvent({ seq: 2 })).done()

  assert.equal(bus.event_history.size, 2)
  assert.throws(() => bus.dispatch(NoDropEvent({ seq: 3 })), /history limit reached \(2\/2\); set bus\.max_history_drop=true/)
  assert.equal(bus.event_history.size, 2)
  assert.equal(bus.pending_event_queue.length, 0)
})

test('max_history_size=0 with max_history_drop=false still allows unbounded queueing and drops completed events', async () => {
  const bus = new EventBus('ZeroHistNoDropBus', { max_history_size: 0, max_history_drop: false })
  const BurstEvent = BaseEvent.extend('BurstEvent', {})

  let release!: () => void
  const unblock = new Promise<void>((resolve) => {
    release = resolve
  })

  bus.on(BurstEvent, async () => {
    await unblock
  })

  const events: BaseEvent[] = []
  for (let i = 0; i < 25; i++) {
    events.push(bus.dispatch(BurstEvent({})))
  }

  await delay(10)
  assert.ok(bus.pending_event_queue.length > 1)
  assert.ok(bus.event_history.size >= 1)

  release()
  await Promise.all(events.map((event) => event.done()))
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 0)
  assert.equal(bus.pending_event_queue.length, 0)
})

test('max_history_size=0 keeps in-flight events and drops them on completion', async () => {
  const bus = new EventBus('ZeroHistBus', { max_history_size: 0 })
  const SlowEvent = BaseEvent.extend('SlowEvent', {})

  let release!: () => void
  const unblock = new Promise<void>((resolve) => {
    release = resolve
  })

  bus.on(SlowEvent, async () => {
    await unblock
  })

  const first = bus.dispatch(SlowEvent({}))
  const second = bus.dispatch(SlowEvent({}))

  await delay(10)
  assert.ok(bus.event_history.has(first.event_id))
  assert.ok(bus.event_history.has(second.event_id))

  release()
  await Promise.all([first.done(), second.done()])
  await bus.waitUntilIdle()

  assert.equal(bus.event_history.size, 0)
})

// ─── Event type derivation ───────────────────────────────────────────────────

test('event_type is derived from extend() name argument', () => {
  const MyCustomEvent = BaseEvent.extend('MyCustomEvent', { val: z.number() })
  const event = MyCustomEvent({ val: 42 })
  assert.equal(event.event_type, 'MyCustomEvent')
})

test('event_type can be overridden at instantiation', () => {
  const FlexEvent = BaseEvent.extend('FlexEvent', {})
  const event = FlexEvent({ event_type: 'OverriddenType' })
  assert.equal(event.event_type, 'OverriddenType')
})

test('handler registration by string matches extend() name', async () => {
  const bus = new EventBus('StringMatchBus', { max_history_size: 100 })
  const NamedEvent = BaseEvent.extend('NamedEvent', {})
  const received: string[] = []

  bus.on('NamedEvent', () => {
    received.push('string_handler')
  })

  bus.dispatch(NamedEvent({}))
  await bus.waitUntilIdle()

  assert.equal(received.length, 1)
  assert.equal(received[0], 'string_handler')
})

test('wildcard handler receives all events', async () => {
  const bus = new EventBus('WildcardBus', { max_history_size: 100 })
  const EventA = BaseEvent.extend('EventA', {})
  const EventB = BaseEvent.extend('EventB', {})
  const types: string[] = []

  bus.on('*', (event: BaseEvent) => {
    types.push(event.event_type)
  })

  bus.dispatch(EventA({}))
  bus.dispatch(EventB({}))
  await bus.waitUntilIdle()

  assert.deepEqual(types, ['EventA', 'EventB'])
})

// ─── Error handling and isolation ────────────────────────────────────────────

test('handler error is captured without crashing the bus', async () => {
  const bus = new EventBus('ErrorBus', { max_history_size: 100 })
  const ErrorEvent = BaseEvent.extend('ErrorEvent', {})

  bus.on(ErrorEvent, () => {
    throw new Error('handler blew up')
  })

  const event = bus.dispatch(ErrorEvent({}))
  await event.done()

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')
  assert.ok(original.event_errors.length > 0, 'event should record the error')

  // The handler result should have error status
  const results = Array.from(original.event_results.values())
  assert.equal(results.length, 1)
  assert.equal(results[0].status, 'error')
  assert.ok(results[0].error instanceof Error)
  assert.equal((results[0].error as Error).message, 'handler blew up')
})

test('one handler error does not prevent other handlers from running', async () => {
  const bus = new EventBus('IsolationBus', {
    max_history_size: 100,
    event_handler_concurrency: 'parallel',
  })
  const MultiEvent = BaseEvent.extend('MultiEvent', {})

  const results_seen: string[] = []

  bus.on(MultiEvent, () => {
    results_seen.push('handler_1_ok')
    return 'result_1'
  })
  bus.on(MultiEvent, () => {
    throw new Error('handler_2_fails')
  })
  bus.on(MultiEvent, () => {
    results_seen.push('handler_3_ok')
    return 'result_3'
  })

  const event = bus.dispatch(MultiEvent({}))
  await event.done()

  const original = event._event_original ?? event
  assert.equal(original.event_status, 'completed')

  // Both non-erroring handlers should have run
  assert.ok(results_seen.includes('handler_1_ok'))
  assert.ok(results_seen.includes('handler_3_ok'))

  // Check individual results
  const all_results = Array.from(original.event_results.values())
  const completed_results = all_results.filter((r) => r.status === 'completed')
  const error_results = all_results.filter((r) => r.status === 'error')
  assert.equal(completed_results.length, 2)
  assert.equal(error_results.length, 1)
})

// ─── Concurrent dispatch ─────────────────────────────────────────────────────

test('many events dispatched concurrently all complete', async () => {
  const bus = new EventBus('ConcurrentBus', { max_history_size: null })
  const BatchEvent = BaseEvent.extend('BatchEvent', { idx: z.number() })
  let processed = 0

  bus.on(BatchEvent, () => {
    processed += 1
    return 'ok'
  })

  const events: BaseEvent[] = []
  for (let i = 0; i < 100; i++) {
    events.push(bus.dispatch(BatchEvent({ idx: i })))
  }

  // Wait for all to complete
  await Promise.all(events.map((e) => e.done()))
  await bus.waitUntilIdle()

  assert.equal(processed, 100)
  assert.equal(bus.event_history.size, 100)

  for (const event of bus.event_history.values()) {
    assert.equal(event.event_status, 'completed')
  }
})

// ─── event_timeout default application ───────────────────────────────────────

test('dispatch applies bus event_timeout_default when event has null timeout', async () => {
  const bus = new EventBus('TimeoutDefaultBus', {
    max_history_size: 100,
    event_timeout: 42,
  })
  const TEvent = BaseEvent.extend('TEvent', {})

  const event = bus.dispatch(TEvent({}))
  const original = event._event_original ?? event

  // The bus should have applied its default timeout
  assert.equal(original.event_timeout, 42)

  await bus.waitUntilIdle()
})

test('event with explicit timeout is not overridden by bus default', async () => {
  const bus = new EventBus('TimeoutOverrideBus', {
    max_history_size: 100,
    event_timeout: 42,
  })
  const TEvent = BaseEvent.extend('TEvent', {})

  const event = bus.dispatch(TEvent({ event_timeout: 10 }))
  const original = event._event_original ?? event

  assert.equal(original.event_timeout, 10)

  await bus.waitUntilIdle()
})

// ─── EventBus._all_instances tracking ─────────────────────────────────────────────

test('EventBus._all_instances tracks all created buses', () => {
  const initial_count = EventBus._all_instances.size
  const bus_a = new EventBus('TrackA')
  const bus_b = new EventBus('TrackB')

  assert.ok(EventBus._all_instances.has(bus_a))
  assert.ok(EventBus._all_instances.has(bus_b))
  assert.equal(EventBus._all_instances.size, initial_count + 2)
})

// ─── Circular forwarding prevention ──────────────────────────────────────────

test('circular forwarding does not cause infinite loop', async () => {
  const bus_a = new EventBus('CircA', { max_history_size: 100 })
  const bus_b = new EventBus('CircB', { max_history_size: 100 })
  const bus_c = new EventBus('CircC', { max_history_size: 100 })

  // A -> B -> C -> A (circular)
  bus_a.on('*', bus_b.dispatch)
  bus_b.on('*', bus_c.dispatch)
  bus_c.on('*', bus_a.dispatch)

  const CircEvent = BaseEvent.extend('CircEvent', {})
  const handler_calls: string[] = []

  // Register real handlers on each bus
  bus_a.on(CircEvent, () => {
    handler_calls.push('A')
    return 'a'
  })
  bus_b.on(CircEvent, () => {
    handler_calls.push('B')
    return 'b'
  })
  bus_c.on(CircEvent, () => {
    handler_calls.push('C')
    return 'c'
  })

  const event = bus_a.dispatch(CircEvent({}))
  await event.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  // Each bus should process the event exactly once (loop prevention via event_path)
  assert.equal(handler_calls.filter((h) => h === 'A').length, 1)
  assert.equal(handler_calls.filter((h) => h === 'B').length, 1)
  assert.equal(handler_calls.filter((h) => h === 'C').length, 1)

  // event_path should contain all three buses
  const original = event._event_original ?? event
  assert.ok(original.event_path.includes(bus_a.label))
  assert.ok(original.event_path.includes(bus_b.label))
  assert.ok(original.event_path.includes(bus_c.label))
})

// ─── EventBus GC / memory leak ───────────────────────────────────────────────

test('unreferenced EventBus can be garbage collected (not retained by _all_instances)', async () => {
  // This test requires --expose-gc to force garbage collection
  const gc = globalThis.gc as (() => void) | undefined
  if (typeof gc !== 'function') {
    // Can't test GC without --expose-gc; skip gracefully
    return
  }

  let weak_ref: WeakRef<EventBus>

    // Create a bus inside an IIFE so the only reference is the WeakRef
  ;(() => {
    const bus = new EventBus('GCTestBus')
    weak_ref = new WeakRef(bus)
  })()

  // Force garbage collection
  gc()
  await delay(50)
  gc()

  // If EventBus._all_instances holds a strong reference (Set<EventBus>),
  // the bus will NOT be collected — proving the memory leak.
  // After the fix (WeakRef-based storage), the bus should be collected.
  assert.equal(
    weak_ref!.deref(),
    undefined,
    'bus should be garbage collected when no external references remain — ' +
      'EventBus._all_instances is holding a strong reference (memory leak)'
  )
})

test('unreferenced buses with event history are garbage collected without destroy()', async () => {
  const gc = globalThis.gc as (() => void) | undefined
  if (typeof gc !== 'function') {
    return
  }

  const GcEvent = BaseEvent.extend('GcNoDestroyEvent', {})
  const weak_refs: Array<WeakRef<EventBus>> = []

  gc()
  await delay(20)
  gc()
  const heap_before = process.memoryUsage().heapUsed

  const create_and_run_bus = async (index: number): Promise<WeakRef<EventBus>> => {
    const bus = new EventBus(`GC-NoDestroy-${index}`, { max_history_size: 200 })
    bus.on(GcEvent, () => {})
    for (let i = 0; i < 200; i += 1) {
      const event = bus.dispatch(GcEvent({}))
      await event.done()
    }
    await bus.waitUntilIdle()
    return new WeakRef(bus)
  }

  for (let i = 0; i < 120; i += 1) {
    weak_refs.push(await create_and_run_bus(i))
  }

  for (let i = 0; i < 30; i += 1) {
    gc()
    await delay(20)
  }

  const alive_count = weak_refs.reduce((count, ref) => count + (ref.deref() ? 1 : 0), 0)
  const heap_after = process.memoryUsage().heapUsed

  assert.equal(alive_count, 0, 'all unreferenced buses should be garbage collected without explicit destroy()')
  assert.equal(EventBus._all_instances.size, 0, '_all_instances should not retain unreferenced buses')
  assert.ok(
    heap_after <= heap_before + 20 * 1024 * 1024,
    `heap should return near baseline after GC, before=${(heap_before / 1024 / 1024).toFixed(1)}MB after=${(heap_after / 1024 / 1024).toFixed(1)}MB`
  )
})

// ─── off() handler deregistration ────────────────────────────────────────────

test('off() removes a handler so it no longer fires', async () => {
  const bus = new EventBus('OffBus', { max_history_size: 100 })
  const OffEvent = BaseEvent.extend('OffEvent', {})
  let call_count = 0

  const handler = () => {
    call_count += 1
  }

  bus.on(OffEvent, handler)
  bus.dispatch(OffEvent({}))
  await bus.waitUntilIdle()
  assert.equal(call_count, 1)

  bus.off(OffEvent, handler)
  bus.dispatch(OffEvent({}))
  await bus.waitUntilIdle()
  assert.equal(call_count, 1, 'handler should not fire after off()')
})

test('off() removes a handler by handler_id string', async () => {
  const bus = new EventBus('OffByIdBus', { max_history_size: 100 })
  const OffIdEvent = BaseEvent.extend('OffIdEvent', {})
  let call_count = 0

  bus.on(OffIdEvent, function my_handler() {
    call_count += 1
  })

  // Dispatch once so we can find the handler_id from the event results
  const event1 = bus.dispatch(OffIdEvent({}))
  await bus.waitUntilIdle()
  assert.equal(call_count, 1)

  // Get the handler_id from the event's results
  const results = Array.from(event1.event_results.values())
  assert.equal(results.length, 1, 'should have exactly one handler result')
  const handler_id = results[0].handler_id
  assert.ok(handler_id, 'handler_id should exist')

  // Remove by handler_id string
  bus.off(OffIdEvent, handler_id)

  // Dispatch again — handler should NOT fire
  bus.dispatch(OffIdEvent({}))
  await bus.waitUntilIdle()
  assert.equal(call_count, 1, 'handler should not fire after off() by handler_id')
})

test('off() with no handler removes all handlers for that event', async () => {
  const bus = new EventBus('OffAllBus', { max_history_size: 100 })
  const OffAllEvent = BaseEvent.extend('OffAllEvent', {})
  const OtherEvent = BaseEvent.extend('OffAllOther', {})
  let call_count_a = 0
  let call_count_b = 0
  let other_count = 0

  bus.on(OffAllEvent, () => {
    call_count_a += 1
  })
  bus.on(OffAllEvent, () => {
    call_count_b += 1
  })
  bus.on(OtherEvent, () => {
    other_count += 1
  })

  bus.dispatch(OffAllEvent({}))
  await bus.waitUntilIdle()
  assert.equal(call_count_a, 1)
  assert.equal(call_count_b, 1)

  // Remove ALL handlers for OffAllEvent
  bus.off(OffAllEvent)

  bus.dispatch(OffAllEvent({}))
  bus.dispatch(OtherEvent({}))
  await bus.waitUntilIdle()

  // Neither OffAllEvent handler should fire
  assert.equal(call_count_a, 1, 'handler A should not fire after off(event)')
  assert.equal(call_count_b, 1, 'handler B should not fire after off(event)')
  // OtherEvent handler should still work
  assert.equal(other_count, 1, 'unrelated handler should still fire')
})
