import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

test('reset creates a fresh pending event for cross-bus dispatch', async () => {
  const ResetEvent = BaseEvent.extend('ResetCoverageEvent', {
    label: z.string(),
  })

  const bus_a = new EventBus('ResetCoverageBusA')
  const bus_b = new EventBus('ResetCoverageBusB')

  bus_a.on(ResetEvent, (event) => `a:${event.label}`)
  bus_b.on(ResetEvent, (event) => `b:${event.label}`)

  const completed = await bus_a.dispatch(ResetEvent({ label: 'hello' })).done()
  const fresh = completed.reset()

  assert.notEqual(fresh.event_id, completed.event_id)
  assert.equal(fresh.event_status, 'pending')
  assert.equal(fresh.event_results.size, 0)
  assert.equal(fresh.event_started_at, undefined)
  assert.equal(fresh.event_completed_at, undefined)

  const forwarded = await bus_b.dispatch(fresh).done()
  assert.equal(forwarded.event_status, 'completed')
  assert.equal(
    Array.from(forwarded.event_results.values()).some((result) => result.result === 'b:hello'),
    true
  )
  assert.equal(
    forwarded.event_path.some((entry) => entry.startsWith('ResetCoverageBusA#')),
    true
  )
  assert.equal(
    forwarded.event_path.some((entry) => entry.startsWith('ResetCoverageBusB#')),
    true
  )

  bus_a.destroy()
  bus_b.destroy()
})

test('scoped handler event reports bus and _event_original via in-operator', async () => {
  const ProxyEvent = BaseEvent.extend('ProxyHasCoverageEvent', {})
  const bus = new EventBus('ProxyHasCoverageBus')
  let has_bus = false
  let has_original = false

  bus.on(ProxyEvent, (event) => {
    has_bus = 'bus' in event
    has_original = '_event_original' in event
  })

  await bus.dispatch(ProxyEvent({})).done()

  assert.equal(has_bus, true)
  assert.equal(has_original, true)
  bus.destroy()
})

test('on() rejects BaseEvent matcher without a concrete event type', () => {
  const bus = new EventBus('InvalidMatcherCoverageBus')
  assert.throws(() => bus.on(BaseEvent as unknown as any, () => undefined), /must be a string event type/)
  bus.destroy()
})

test('max_history_size=0 prunes previously completed events on later dispatch', async () => {
  const HistEvent = BaseEvent.extend('ZeroHistoryCoverageEvent', {
    label: z.string(),
  })
  const bus = new EventBus('ZeroHistoryCoverageBus', { max_history_size: 1 })
  bus.on(HistEvent, () => undefined)

  const first = await bus.dispatch(HistEvent({ label: 'first' })).done()
  assert.equal(bus.event_history.has(first.event_id), true)

  bus.max_history_size = 0
  const second = await bus.dispatch(HistEvent({ label: 'second' })).done()
  assert.equal(bus.event_history.has(first.event_id), false)
  assert.equal(bus.event_history.has(second.event_id), false)
  assert.equal(bus.event_history.size, 0)

  bus.destroy()
})
