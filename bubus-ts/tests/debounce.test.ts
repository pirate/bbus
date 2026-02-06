import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})

const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', { target_id: z.string() })

const SyncEvent = BaseEvent.extend('SyncEvent', {})

test('simple debounce uses recent history or dispatches new', async () => {
  const bus = new EventBus('DebounceBus')

  const parent_event = bus.dispatch(ParentEvent({}))
  await parent_event.done()

  const child_event = parent_event.bus?.emit(ScreenshotEvent({ target_id: 'tab-1' }))
  assert.ok(child_event)
  await child_event.done()

  const reused_event =
    (await bus.find(ScreenshotEvent, {
      past: 10,
      future: false,
      child_of: parent_event,
    })) ?? (await bus.dispatch(ScreenshotEvent({ target_id: 'fallback' })).done())

  assert.equal(reused_event.event_id, child_event.event_id)
  assert.equal(reused_event.event_parent_id, parent_event.event_id)
})

test('advanced debounce prefers history, then waits for future, then dispatches', async () => {
  const bus = new EventBus('AdvancedDebounceBus')

  const pending_event = bus.find(SyncEvent, { past: false, future: 0.5 })

  setTimeout(() => {
    bus.dispatch(SyncEvent({}))
  }, 50)

  const resolved_event =
    (await bus.find(SyncEvent, { past: true, future: false })) ?? (await pending_event) ?? (await bus.dispatch(SyncEvent({})).done())

  assert.ok(resolved_event)
  assert.equal(resolved_event.event_type, 'SyncEvent')
})

test('debounce returns existing fresh event', async () => {
  const bus = new EventBus('DebounceFreshBus')

  const original = await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done()

  const is_fresh = (event: typeof original): boolean => {
    const completed_at = event.event_completed_at ? Date.parse(event.event_completed_at) : 0
    return Date.now() - completed_at < 5000
  }

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab1' && is_fresh(event), { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done())

  assert.equal(result.event_id, original.event_id)
})

test('debounce dispatches new when no match', async () => {
  const bus = new EventBus('DebounceNoMatchBus')

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab1', { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done())

  assert.ok(result)
  assert.equal(result.target_id, 'tab1')
  assert.equal(result.event_status, 'completed')
})

test('debounce dispatches new when existing is stale', async () => {
  const bus = new EventBus('DebounceStaleBus')

  await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done()

  const result =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab1' && false, { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done())

  assert.ok(result)
  const screenshots = Array.from(bus.event_history.values()).filter((event) => event.event_type === 'ScreenshotEvent')
  assert.equal(screenshots.length, 2)
})

test('debounce or-chain handles sequential lookups without blocking', async () => {
  const bus = new EventBus('DebounceSequentialBus')

  const result1 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab1', { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done())

  const result2 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab1', { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab1' })).done())

  const result3 =
    (await bus.find(ScreenshotEvent, (event) => event.target_id === 'tab2', { past: true, future: false })) ??
    (await bus.dispatch(ScreenshotEvent({ target_id: 'tab2' })).done())

  assert.equal(result1.event_id, result2.event_id)
  assert.notEqual(result1.event_id, result3.event_id)
  assert.equal(result3.target_id, 'tab2')
})
