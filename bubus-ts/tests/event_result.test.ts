import assert from 'node:assert/strict'
import { test } from 'node:test'

import { v5 as uuidv5 } from 'uuid'
import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'
import { EventHandler } from '../src/event_handler.js'
import { EventResult } from '../src/event_result.js'

const StringResultEvent = BaseEvent.extend('StringResultEvent', {
  event_result_type: z.string(),
})

const ObjectResultEvent = BaseEvent.extend('ObjectResultEvent', {
  event_result_type: z.object({ value: z.string(), count: z.number() }),
})

const NoResultSchemaEvent = BaseEvent.extend('NoResultSchemaEvent', {})

test('event results capture handler return values', async () => {
  const bus = new EventBus('ResultCaptureBus')

  bus.on(StringResultEvent, () => 'ok')

  const event = bus.emit(StringResultEvent({}))
  await event.done()

  assert.equal(event.event_results.size, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'ok')
})

test('event_result_type validates handler results', async () => {
  const bus = new EventBus('ResultSchemaBus')

  bus.on(ObjectResultEvent, () => ({ value: 'hello', count: 2 }))

  const event = bus.emit(ObjectResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 2 })
})

test('event_result_type allows undefined handler return values', async () => {
  const bus = new EventBus('ResultSchemaUndefinedBus')

  bus.on(ObjectResultEvent, () => {})

  const event = bus.emit(ObjectResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, undefined)
})

test('invalid result marks handler error', async () => {
  const bus = new EventBus('ResultSchemaErrorBus')

  bus.on(ObjectResultEvent, () => JSON.parse('{"value":"bad","count":"nope"}'))

  const event = bus.emit(ObjectResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof Error)
})

test('event with no result schema stores raw values', async () => {
  const bus = new EventBus('NoSchemaBus')

  bus.on(NoResultSchemaEvent, () => ({ raw: true }))

  const event = bus.emit(NoResultSchemaEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { raw: true })
})

test('event result JSON omits result_type and derives from parent event', async () => {
  const bus = new EventBus('ResultTypeDeriveBus')

  bus.on(StringResultEvent, () => 'ok')

  const event = bus.emit(StringResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  const json = result.toJSON() as Record<string, unknown>

  assert.equal('result_type' in json, false)
  assert.equal('handler' in json, false)
  assert.equal(typeof json.handler_id, 'string')
  assert.equal(typeof json.handler_name, 'string')
  assert.equal(typeof json.handler_event_pattern, 'string')
  assert.equal(typeof json.eventbus_name, 'string')
  assert.equal(typeof json.eventbus_id, 'string')
  assert.equal(typeof json.handler_registered_at, 'string')
  assert.equal(typeof json.handler_registered_ts, 'number')
  assert.equal(result.result_type, event.event_result_type)
})

test('EventHandler JSON roundtrips handler metadata', () => {
  const handler = (event: BaseEvent): string => event.event_type
  const entry = new EventHandler({
    handler,
    handler_name: 'pkg.module.handler',
    handler_file_path: '~/project/app.ts:123',
    handler_registered_at: '2025-01-02T03:04:05.678Z',
    handler_registered_ts: 1735787045678901000,
    event_pattern: 'StandaloneEvent',
    eventbus_name: 'StandaloneBus',
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
  })

  const dumped = entry.toJSON()
  const loaded = EventHandler.fromJSON(dumped)

  assert.equal(loaded.id, entry.id)
  assert.equal(loaded.event_pattern, 'StandaloneEvent')
  assert.equal(loaded.eventbus_name, 'StandaloneBus')
  assert.equal(loaded.eventbus_id, '018f8e40-1234-7000-8000-000000001234')
  assert.equal(loaded.handler_name, 'pkg.module.handler')
  assert.equal(loaded.handler_file_path, '~/project/app.ts:123')
})

test('EventHandler.computeHandlerId matches uuidv5 seed algorithm', () => {
  const namespace = uuidv5('bubus-handler', uuidv5.DNS)
  const expected_seed =
    '018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.ts:123|' +
    '2025-01-02T03:04:05.678Z|1735787045678901000|StandaloneEvent'
  const expected_id = uuidv5(expected_seed, namespace)

  const computed_id = EventHandler.computeHandlerId({
    eventbus_id: '018f8e40-1234-7000-8000-000000001234',
    handler_name: 'pkg.module.handler',
    handler_file_path: '~/project/app.ts:123',
    handler_registered_at: '2025-01-02T03:04:05.678Z',
    handler_registered_ts: 1735787045678901000,
    event_pattern: 'StandaloneEvent',
  })

  assert.equal(computed_id, expected_id)
})

test('runHandler does not create a slow monitor timer for already-settled results', async () => {
  const SettledEvent = BaseEvent.extend('RunHandlerSettledEvent', {})
  const bus = new EventBus('RunHandlerSettledBus')
  const handler = bus.on(SettledEvent, () => 'ok')

  const event = SettledEvent({})
  event.bus = bus

  const result = new EventResult({ event, handler })
  result.status = 'completed'

  let timer_created = false
  result.createSlowHandlerWarningTimer = () => {
    timer_created = true
    return null
  }

  await result.runHandler(null)

  assert.equal(timer_created, false)
  bus.destroy()
})

test('runHandler starts slow monitor timer only after handler is marked started', async () => {
  const TimerOrderEvent = BaseEvent.extend('RunHandlerTimerOrderEvent', {})
  const bus = new EventBus('RunHandlerTimerOrderBus')
  const handler = bus.on(TimerOrderEvent, async () => 'ok')

  const event = TimerOrderEvent({})
  event.bus = bus

  const result = new EventResult({ event, handler })
  let status_seen_during_timer_creation: string | null = null
  result.createSlowHandlerWarningTimer = () => {
    status_seen_during_timer_creation = result.status
    return null
  }

  await result.runHandler(null)

  assert.equal(status_seen_during_timer_creation, 'started')
  bus.destroy()
})

test('handler result stays pending while waiting for withHandlerLock entry', async () => {
  const LockWaitEvent = BaseEvent.extend('RunHandlerLockWaitEvent', {})
  const bus = new EventBus('RunHandlerLockWaitBus', { event_handler_concurrency: 'serial' })

  bus.on(LockWaitEvent, async () => 'ok')

  let release_lock!: () => void
  const release_lock_promise = new Promise<void>((resolve) => {
    release_lock = resolve
  })
  const original_with_handler_lock = bus.locks.withHandlerLock.bind(bus.locks)
  bus.locks.withHandlerLock = async (event, default_handler_concurrency, fn) => {
    await release_lock_promise
    return await original_with_handler_lock(event, default_handler_concurrency, fn)
  }

  const event = bus.emit(LockWaitEvent({}))
  const start = Date.now()
  while (event.event_results.size === 0) {
    if (Date.now() - start > 1_000) {
      throw new Error('Timed out waiting for pending handler result')
    }
    await new Promise((resolve) => setTimeout(resolve, 0))
  }

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'pending')

  release_lock()
  await event.done()
  assert.equal(result.status, 'completed')
  bus.destroy()
})

test('slow handler warning is based on handler runtime after lock wait', async () => {
  const SlowAfterLockWaitEvent = BaseEvent.extend('RunHandlerSlowAfterLockWaitEvent', {})
  const bus = new EventBus('RunHandlerSlowAfterLockWaitBus', {
    event_handler_concurrency: 'serial',
    event_handler_slow_timeout: 0.01,
  })
  const warnings: string[] = []
  const original_warn = console.warn
  console.warn = (message?: unknown, ...args: unknown[]) => {
    warnings.push(String(message))
    if (args.length > 0) {
      warnings.push(args.map(String).join(' '))
    }
  }
  try {
    bus.on(SlowAfterLockWaitEvent, async () => {
      await new Promise((resolve) => setTimeout(resolve, 30))
      return 'ok'
    })

    let release_lock!: () => void
    const release_lock_promise = new Promise<void>((resolve) => {
      release_lock = resolve
    })
    const original_with_handler_lock = bus.locks.withHandlerLock.bind(bus.locks)
    bus.locks.withHandlerLock = async (event, default_handler_concurrency, fn) => {
      await release_lock_promise
      return await original_with_handler_lock(event, default_handler_concurrency, fn)
    }

    const event = bus.emit(SlowAfterLockWaitEvent({}))
    const start = Date.now()
    while (event.event_results.size === 0) {
      if (Date.now() - start > 1_000) {
        throw new Error('Timed out waiting for pending handler result')
      }
      await new Promise((resolve) => setTimeout(resolve, 0))
    }
    assert.equal(Array.from(event.event_results.values())[0].status, 'pending')

    await new Promise((resolve) => setTimeout(resolve, 30))
    release_lock()
    await event.done()

    assert.equal(
      warnings.some((message) => message.toLowerCase().includes('slow event handler')),
      true
    )
  } finally {
    console.warn = original_warn
    bus.destroy()
  }
})
