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

  standalone.markStarted()
  assert.equal(standalone.event_status, 'started')
  assert.equal(typeof standalone.event_started_at, 'string')

  standalone.markCompleted(false)
  assert.equal(standalone.event_status, 'completed')
  assert.equal(typeof standalone.event_completed_at, 'string')
  await standalone.waitForCompletion()
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

  event.markStarted()
  event.markCompleted()
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
