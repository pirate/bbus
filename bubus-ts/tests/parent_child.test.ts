import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})
const ChildEvent = BaseEvent.extend('ChildEvent', {})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {})
const UnrelatedEvent = BaseEvent.extend('UnrelatedEvent', {})

test('eventIsChildOf and eventIsParentOf work for direct children', async () => {
  const bus = new EventBus('ParentChildBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({}))
  })

  const parent_event = bus.dispatch(ParentEvent({}))
  await bus.waitUntilIdle()

  const child_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(child_event)

  assert.equal(child_event.event_parent_id, parent_event.event_id)
  assert.equal(bus.eventIsChildOf(child_event, parent_event), true)
  assert.equal(bus.eventIsParentOf(parent_event, child_event), true)
})

test('eventIsChildOf works for grandchildren', async () => {
  const bus = new EventBus('GrandchildBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({}))
  })

  bus.on(ChildEvent, (event) => {
    event.bus?.emit(GrandchildEvent({}))
  })

  const parent_event = bus.dispatch(ParentEvent({}))
  await bus.waitUntilIdle()

  const child_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  const grandchild_event = Array.from(bus.event_history.values()).find((event) => event.event_type === 'GrandchildEvent')

  assert.ok(child_event)
  assert.ok(grandchild_event)

  assert.equal(bus.eventIsChildOf(child_event, parent_event), true)
  assert.equal(bus.eventIsChildOf(grandchild_event, parent_event), true)
  assert.equal(bus.eventIsParentOf(parent_event, grandchild_event), true)
})

test('eventIsChildOf returns false for unrelated events', async () => {
  const bus = new EventBus('UnrelatedBus')

  const parent_event = bus.dispatch(ParentEvent({}))
  const unrelated_event = bus.dispatch(UnrelatedEvent({}))
  await parent_event.done()
  await unrelated_event.done()

  assert.equal(bus.eventIsChildOf(unrelated_event, parent_event), false)
  assert.equal(bus.eventIsParentOf(parent_event, unrelated_event), false)
})
