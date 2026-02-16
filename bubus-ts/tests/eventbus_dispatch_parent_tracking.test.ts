import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {
  message: z.string().optional(),
})
const ChildEvent = BaseEvent.extend('ChildEvent', {
  data: z.string().optional(),
})
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', {
  value: z.number().optional(),
})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('basic parent tracking: child events get event_parent_id', async () => {
  const bus = new EventBus('TestBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    const child = event.bus?.emit(ChildEvent({ data: `child_of_${event.message ?? 'root'}` }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'parent_handled'
  })

  const parent = bus.emit(ParentEvent({ message: 'test_parent' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 1)
  assert.equal(child_events[0].event_parent_id, parent.event_id)
})

test('multi-level parent tracking preserves lineage', async () => {
  const bus = new EventBus('LineageBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'child_data' }))
    return 'parent'
  })

  bus.on(ChildEvent, (event) => {
    event.bus?.emit(GrandchildEvent({ value: 42 }))
    return 'child'
  })

  bus.on(GrandchildEvent, () => 'grandchild')

  const parent = bus.emit(ParentEvent({ message: 'root' }))
  await bus.waitUntilIdle()
  await parent.done()

  const seen_parent = Array.from(bus.event_history.values()).find((event) => event.event_id === parent.event_id)
  const seen_child = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  const seen_grandchild = Array.from(bus.event_history.values()).find((event) => event.event_type === 'GrandchildEvent')
  assert.ok(seen_parent)
  assert.ok(seen_child)
  assert.ok(seen_grandchild)
  assert.equal(seen_parent.event_parent_id, null)
  assert.equal(seen_child.event_parent_id, parent.event_id)
  assert.equal(seen_grandchild.event_parent_id, seen_child.event_id)
})

test('multiple children from same parent keep same event_parent_id', async () => {
  const bus = new EventBus('MultiChildBus')
  const child_events: BaseEvent[] = []

  bus.on(ParentEvent, (event) => {
    for (let i = 0; i < 3; i += 1) {
      const child = event.bus?.emit(ChildEvent({ data: `child_${i}` }))
      if (!child) {
        throw new Error('expected scoped bus on parent handler event')
      }
      child_events.push(child)
    }
    return 'spawned_children'
  })

  const parent = bus.emit(ParentEvent({ message: 'multi_child_parent' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 3)
  for (const child of child_events) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
})

test('parallel parent handlers preserve parent tracking', async () => {
  const bus = new EventBus('ParallelParentTrackingBus', { event_handler_concurrency: 'parallel' })
  const child_events_h1: BaseEvent[] = []
  const child_events_h2: BaseEvent[] = []

  bus.on(ParentEvent, async (event) => {
    await delay(10)
    const child = event.bus?.emit(ChildEvent({ data: 'from_h1' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events_h1.push(child)
    return 'h1'
  })

  bus.on(ParentEvent, async (event) => {
    await delay(20)
    const child = event.bus?.emit(ChildEvent({ data: 'from_h2' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events_h2.push(child)
    return 'h2'
  })

  const parent = bus.emit(ParentEvent({ message: 'parallel_test' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events_h1.length, 1)
  assert.equal(child_events_h2.length, 1)
  assert.equal(child_events_h1[0].event_parent_id, parent.event_id)
  assert.equal(child_events_h2[0].event_parent_id, parent.event_id)
})

test('explicit event_parent_id is not overridden', async () => {
  const bus = new EventBus('ExplicitParentBus')
  const explicit_parent_id = '018f8e40-1234-7000-8000-000000001234'

  bus.on(ParentEvent, () => {
    const child = ChildEvent({ data: 'explicit', event_parent_id: explicit_parent_id })
    bus.emit(child)
    return 'dispatched'
  })

  const parent = bus.emit(ParentEvent({ message: 'test' }))
  await bus.waitUntilIdle()
  await parent.done()

  const captured_child = Array.from(bus.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(captured_child)
  assert.equal(captured_child.event_parent_id, explicit_parent_id)
  assert.notEqual(captured_child.event_parent_id, parent.event_id)
})

test('cross-eventbus dispatch preserves parent tracking', async () => {
  const bus1 = new EventBus('Bus1')
  const bus2 = new EventBus('Bus2')

  bus1.on(ParentEvent, (event) => {
    const child = ChildEvent({ data: 'cross_bus_child' })
    event.bus?.emit(child)
    bus2.emit(child)
    return 'bus1_handled'
  })

  bus2.on(ChildEvent, () => 'bus2_handled')

  const parent = bus1.emit(ParentEvent({ message: 'cross_bus_test' }))
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle()])
  await parent.done()

  const received_child = Array.from(bus2.event_history.values()).find((event) => event.event_type === 'ChildEvent')
  assert.ok(received_child)
  assert.equal(received_child.event_parent_id, parent.event_id)
})

test('parent tracking works with sync handlers and handler errors', async () => {
  const bus = new EventBus('SyncAndErrorParentTrackingBus')
  const child_events: BaseEvent[] = []

  const sync_handler = (event: BaseEvent): string => {
    const child = event.bus?.emit(ChildEvent({ data: 'from_sync' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'sync_handled'
  }

  const failing_handler = (event: BaseEvent): never => {
    const child = event.bus?.emit(ChildEvent({ data: 'before_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    throw new Error('expected parent-tracking error path')
  }

  const success_handler = (event: BaseEvent): string => {
    const child = event.bus?.emit(ChildEvent({ data: 'after_error' }))
    if (!child) {
      throw new Error('expected scoped bus on parent handler event')
    }
    child_events.push(child)
    return 'success'
  }

  bus.on(ParentEvent, sync_handler)
  bus.on(ParentEvent, failing_handler)
  bus.on(ParentEvent, success_handler)
  bus.on(ChildEvent, () => 'child_handled')

  const parent = bus.emit(ParentEvent({ message: 'mixed_test' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(child_events.length, 3)
  for (const child of child_events) {
    assert.equal(child.event_parent_id, parent.event_id)
  }
})

test('event_children tracks direct and nested descendants', async () => {
  const bus = new EventBus('ChildrenTrackingBus')

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({ data: 'level1' }))
    return 'parent'
  })

  bus.on(ChildEvent, (event) => {
    event.bus?.emit(GrandchildEvent({ value: 42 }))
    return 'child'
  })

  bus.on(GrandchildEvent, () => 'grandchild')

  const parent = bus.emit(ParentEvent({ message: 'nested_test' }))
  await bus.waitUntilIdle()
  await parent.done()

  assert.equal(parent.event_children.length, 1)
  const child = parent.event_children[0]
  assert.equal(child.event_type, 'ChildEvent')
  assert.equal(child.event_children.length, 1)
  const grandchild = child.event_children[0]
  assert.equal(grandchild.event_type, 'GrandchildEvent')
})

test('forwarded events are not counted as parent event_children', async () => {
  const bus1 = new EventBus('ForwardBus1')
  const bus2 = new EventBus('ForwardBus2')

  bus1.on('*', bus2.emit)

  const parent = bus1.emit(ParentEvent({ message: 'forward_test' }))
  await Promise.all([bus1.waitUntilIdle(), bus2.waitUntilIdle()])
  await parent.done()

  assert.equal(parent.event_children.length, 0)
  assert.equal(parent.eventAreAllChildrenComplete(), true)
})
