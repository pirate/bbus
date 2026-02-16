import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

test('BaseEvent lifecycle transitions are explicit and awaitable', async () => {
  const LifecycleEvent = BaseEvent.extend('BaseEventLifecycleTestEvent', {})

  const standalone = LifecycleEvent({})
  assert.equal(standalone.event_status, 'pending')
  assert.equal(standalone.event_started_at, null)
  assert.equal(standalone.event_completed_at, null)

  standalone._markStarted()
  assert.equal(standalone.event_status, 'started')
  assert.equal(typeof standalone.event_started_at, 'string')

  standalone._markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  assert.equal(typeof standalone.event_completed_at, 'string')
  await standalone._waitForCompletion()
})

test('BaseEvent.eventTimestampNow emits raw monotonic performance.now() values', () => {
  const first = BaseEvent.eventTimestampNow()
  const second = BaseEvent.eventTimestampNow()

  assert.equal(typeof first.ts, 'number')
  assert.equal(typeof second.ts, 'number')
  assert.ok(Number.isFinite(first.ts))
  assert.ok(Number.isFinite(second.ts))
  assert.ok(first.ts >= 0)
  assert.ok(second.ts >= first.ts)
  assert.equal(Number.isInteger(Date.parse(first.isostring)), true)
  assert.equal(Number.isInteger(Date.parse(second.isostring)), true)
})

test('BaseEvent rejects reserved runtime fields in payload and event shape', () => {
  const ReservedFieldEvent = BaseEvent.extend('BaseEventReservedFieldEvent', {})

  assert.throws(() => {
    void ReservedFieldEvent({ bus: 'payload_bus_field' } as unknown as never)
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFieldShapeEvent', { bus: z.string() })
  }, /field "bus" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ first: 'payload_first_field' } as unknown as never)
  }, /field "first" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFirstShapeEvent', { first: z.string() })
  }, /field "first" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toString: 'payload_to_string_field' } as unknown as never)
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToStringShapeEvent', { toString: z.string() })
  }, /field "toString" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ toJSON: 'payload_to_json_field' } as unknown as never)
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedToJSONShapeEvent', { toJSON: z.string() })
  }, /field "toJSON" is reserved/i)

  assert.throws(() => {
    void ReservedFieldEvent({ fromJSON: 'payload_from_json_field' } as unknown as never)
  }, /field "fromJSON" is reserved/i)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventReservedFromJSONShapeEvent', { fromJSON: z.string() })
  }, /field "fromJSON" is reserved/i)
})

test('BaseEvent rejects unknown event_* fields while allowing known event_* overrides', () => {
  const AllowedEvent = BaseEvent.extend('BaseEventAllowedEventConfigEvent', {
    event_timeout: 123,
    event_slow_timeout: 9,
    event_handler_timeout: 45,
    value: z.string(),
  })

  const event = AllowedEvent({ value: 'ok' })
  assert.equal(event.event_timeout, 123)
  assert.equal(event.event_slow_timeout, 9)
  assert.equal(event.event_handler_timeout, 45)

  assert.throws(() => {
    void BaseEvent.extend('BaseEventUnknownEventShapeFieldEvent', { event_some_field_we_dont_recognize: 1 })
  }, /starts with "event_" but is not a recognized BaseEvent field/i)

  assert.throws(() => {
    void AllowedEvent({
      value: 'ok',
      event_some_field_we_dont_recognize: 1,
    } as unknown as never)
  }, /starts with "event_" but is not a recognized BaseEvent field/i)
})

test('BaseEvent rejects model_* fields in payload and event shape', () => {
  const ModelReservedEvent = BaseEvent.extend('BaseEventModelReservedEvent', {})

  assert.throws(() => {
    void BaseEvent.extend('BaseEventModelReservedShapeEvent', { model_something_random: 1 })
  }, /starts with "model_" and is reserved/i)

  assert.throws(() => {
    void ModelReservedEvent({ model_something_random: 1 } as unknown as never)
  }, /starts with "model_" and is reserved/i)
})

test('BaseEvent toJSON/fromJSON roundtrips runtime fields and event_results', async () => {
  const RuntimeEvent = BaseEvent.extend('BaseEventRuntimeSerializationEvent', {
    event_result_type: z.string(),
  })
  const bus = new EventBus('BaseEventRuntimeSerializationBus')

  bus.on(RuntimeEvent, () => 'ok')

  const event = bus.emit(RuntimeEvent({}))
  await event.done()

  const json = event.toJSON() as Record<string, unknown>
  assert.equal(json.event_status, 'completed')
  assert.equal(typeof json.event_created_ts, 'number')
  assert.equal(typeof json.event_started_ts, 'number')
  assert.equal(typeof json.event_completed_ts, 'number')
  assert.equal(json.event_pending_bus_count, 0)

  const json_results = json.event_results as Array<Record<string, unknown>>
  assert.equal(json_results.length, 1)
  assert.equal(json_results[0].status, 'completed')
  assert.equal(json_results[0].result, 'ok')

  const restored = RuntimeEvent.fromJSON?.(json) ?? RuntimeEvent(json as never)
  assert.equal(restored.event_status, 'completed')
  assert.equal(restored.event_created_ts, event.event_created_ts)
  assert.equal(restored.event_results.size, 1)
  assert.equal(Array.from(restored.event_results.values())[0].result, 'ok')

  bus.destroy()
})

test('BaseEvent reset returns a fresh pending event that can be redispatched', async () => {
  const ResetEvent = BaseEvent.extend('BaseEventResetEvent', {
    label: z.string(),
  })

  const bus_a = new EventBus('BaseEventResetBusA')
  const bus_b = new EventBus('BaseEventResetBusB')

  bus_a.on(ResetEvent, (event) => `a:${event.label}`)
  bus_b.on(ResetEvent, (event) => `b:${event.label}`)

  const completed = await bus_a.emit(ResetEvent({ label: 'hello' })).done()
  const fresh = completed.eventReset()

  assert.notEqual(fresh.event_id, completed.event_id)
  assert.equal(fresh.event_status, 'pending')
  assert.equal(fresh.event_results.size, 0)
  assert.equal(fresh.event_started_at, null)
  assert.equal(fresh.event_completed_at, null)

  const forwarded = await bus_b.emit(fresh).done()
  assert.equal(forwarded.event_status, 'completed')
  assert.equal(
    Array.from(forwarded.event_results.values()).some((result) => result.result === 'b:hello'),
    true
  )

  bus_a.destroy()
  bus_b.destroy()
})

test('BaseEvent fromJSON preserves nullable parent/emitted metadata', () => {
  const event = BaseEvent.fromJSON({
    event_id: '018f8e40-1234-7000-8000-00000000123a',
    event_created_at: new Date('2025-01-01T00:00:00.000Z').toISOString(),
    event_type: 'BaseEventFromJsonNullFieldsEvent',
    event_parent_id: null,
    event_emitted_by_handler_id: null,
    event_timeout: null,
  })

  assert.equal(event.event_parent_id, null)
  assert.equal(event.event_emitted_by_handler_id, null)

  const roundtrip = event.toJSON() as Record<string, unknown>
  assert.equal(roundtrip.event_parent_id, null)
  assert.equal(roundtrip.event_emitted_by_handler_id, null)
})

test('BaseEvent status hooks capture bus reference before event gc', async () => {
  const HookEvent = BaseEvent.extend('BaseEventHookCaptureEvent', {})

  class HookCaptureBus extends EventBus {
    scheduled: Array<() => void> = []
    seen_statuses: string[] = []

    scheduleMicrotask(fn: () => void): void {
      this.scheduled.push(fn)
    }

    async onEventChange(_event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
      this.seen_statuses.push(status)
    }
  }

  const bus = new HookCaptureBus('BaseEventHookCaptureBus')
  const event = HookEvent({})
  event.bus = bus

  event._markStarted()
  event._markCompleted()
  event._gc()

  assert.equal(bus.scheduled.length, 2)
  assert.doesNotThrow(() => {
    for (const callback of bus.scheduled) {
      callback()
    }
  })
  await Promise.resolve()
  assert.deepEqual(bus.seen_statuses, ['started', 'completed'])

  bus.destroy()
})
