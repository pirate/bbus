import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, EventResult } from '../src/index.js'
import { EventHandler } from '../src/event_handler.js'
import type { EventHandlerCallable } from '../src/types.js'

const RootEvent = BaseEvent.extend('RootEvent', { data: z.string().optional() })
const ChildEvent = BaseEvent.extend('ChildEvent', { value: z.number().optional() })
const GrandchildEvent = BaseEvent.extend('GrandchildEvent', { nested: z.record(z.string(), z.number()).optional() })

class ValueError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValueError'
  }
}

const createHandlerEntry = (bus: EventBus, handler_id: string, handler_name: string, event_pattern: string): EventHandler => {
  const handler: EventHandlerCallable = () => undefined
  const { isostring: handler_registered_at, ts: handler_registered_ts } = BaseEvent.eventTimestampNow()
  return new EventHandler({
    id: handler_id,
    handler,
    handler_name,
    handler_timeout: bus.event_timeout,
    handler_registered_at,
    handler_registered_ts,
    event_pattern,
    eventbus_name: bus.name,
    eventbus_id: bus.id,
  })
}

test('logTree: single event', () => {
  const bus = new EventBus('SingleBus')

  const event = RootEvent({ data: 'test' })
  event.event_status = 'completed'
  event.event_completed_at = event.event_created_at

  bus.event_history.set(event.event_id, event)

  const output = bus.logTree()

  assert.ok(output.includes('└── ✅ RootEvent#'))
  assert.ok(output.includes('[') && output.includes(']'))
})

test('logTree: with handler results', () => {
  const bus = new EventBus('HandlerBus')

  const event = RootEvent({ data: 'test' })
  event.event_status = 'completed'
  event.event_completed_at = event.event_created_at

  const handler_id = 'handler-1'
  const result = new EventResult({
    event,
    handler: createHandlerEntry(bus, handler_id, 'test_handler', event.event_type),
  })
  result._markStarted()
  result._markCompleted('status: success')
  event.event_results.set(handler_id, result)

  bus.event_history.set(event.event_id, event)

  const output = bus.logTree()

  assert.ok(output.includes('└── ✅ RootEvent#'))
  assert.ok(output.includes(`${bus.label}.test_handler#`))
  assert.ok(output.includes('"status: success"'))
})

test('logTree: with handler errors', () => {
  const bus = new EventBus('ErrorBus')

  const event = RootEvent({ data: 'test' })
  event.event_status = 'completed'
  event.event_completed_at = event.event_created_at

  const handler_id = 'handler-2'
  const result = new EventResult({
    event,
    handler: createHandlerEntry(bus, handler_id, 'error_handler', event.event_type),
  })
  result._markStarted()
  result._markError(new ValueError('Test error message'))
  event.event_results.set(handler_id, result)

  bus.event_history.set(event.event_id, event)

  const output = bus.logTree()

  assert.ok(output.includes(`${bus.label}.error_handler#`))
  assert.ok(output.includes('ValueError: Test error message'))
})

test('logTree: complex nested', () => {
  const bus = new EventBus('ComplexBus')

  const root = RootEvent({ data: 'root_data' })
  root.event_status = 'completed'
  root.event_completed_at = root.event_created_at

  const root_handler_id = 'handler-root'
  const root_result = new EventResult({
    event: root,
    handler: createHandlerEntry(bus, root_handler_id, 'root_handler', root.event_type),
  })
  root_result._markStarted()
  root_result._markCompleted('Root processed')
  root.event_results.set(root_handler_id, root_result)

  const child = ChildEvent({ value: 100 })
  child.event_parent_id = root.event_id
  child.event_status = 'completed'
  child.event_completed_at = child.event_created_at
  root_result.event_children.push(child)

  const child_handler_id = 'handler-child'
  const child_result = new EventResult({
    event: child,
    handler: createHandlerEntry(bus, child_handler_id, 'child_handler', child.event_type),
  })
  child_result._markStarted()
  child_result._markCompleted([1, 2, 3])
  child.event_results.set(child_handler_id, child_result)

  const grandchild = GrandchildEvent({})
  grandchild.event_parent_id = child.event_id
  grandchild.event_status = 'completed'
  grandchild.event_completed_at = grandchild.event_created_at
  child_result.event_children.push(grandchild)

  const grandchild_handler_id = 'handler-grandchild'
  const grandchild_result = new EventResult({
    event: grandchild,
    handler: createHandlerEntry(bus, grandchild_handler_id, 'grandchild_handler', grandchild.event_type),
  })
  grandchild_result._markStarted()
  grandchild_result._markCompleted(null)
  grandchild.event_results.set(grandchild_handler_id, grandchild_result)

  bus.event_history.set(root.event_id, root)
  bus.event_history.set(child.event_id, child)
  bus.event_history.set(grandchild.event_id, grandchild)

  const output = bus.logTree()

  assert.ok(output.includes('✅ RootEvent#'))
  assert.ok(output.includes(`✅ ${bus.label}.root_handler#`))
  assert.ok(output.includes('✅ ChildEvent#'))
  assert.ok(output.includes(`✅ ${bus.label}.child_handler#`))
  assert.ok(output.includes('✅ GrandchildEvent#'))
  assert.ok(output.includes(`✅ ${bus.label}.grandchild_handler#`))
  assert.ok(output.includes('"Root processed"'))
  assert.ok(output.includes('list(3 items)'))
  assert.ok(output.includes('None'))
})

test('logTree: multiple roots', () => {
  const bus = new EventBus('MultiBus')

  const root1 = RootEvent({ data: 'first' })
  root1.event_status = 'completed'
  root1.event_completed_at = root1.event_created_at

  const root2 = RootEvent({ data: 'second' })
  root2.event_status = 'completed'
  root2.event_completed_at = root2.event_created_at

  bus.event_history.set(root1.event_id, root1)
  bus.event_history.set(root2.event_id, root2)

  const output = bus.logTree()

  assert.equal(output.split('├── ✅ RootEvent#').length - 1, 1)
  assert.equal(output.split('└── ✅ RootEvent#').length - 1, 1)
})

test('logTree: timing info', () => {
  const bus = new EventBus('TimingBus')

  const event = RootEvent({})
  event.event_status = 'completed'
  event.event_completed_at = event.event_created_at

  const handler_id = 'handler-time'
  const result = new EventResult({
    event,
    handler: createHandlerEntry(bus, handler_id, 'timed_handler', event.event_type),
  })
  result._markStarted()
  result._markCompleted('done')
  event.event_results.set(handler_id, result)

  bus.event_history.set(event.event_id, event)

  const output = bus.logTree()

  assert.ok(output.includes('('))
  assert.ok(output.includes('s)'))
})

test('logTree: running handler', () => {
  const bus = new EventBus('RunningBus')

  const event = RootEvent({})
  event.event_status = 'started'

  const handler_id = 'handler-running'
  const result = new EventResult({
    event,
    handler: createHandlerEntry(bus, handler_id, 'running_handler', event.event_type),
  })
  result._markStarted()
  event.event_results.set(handler_id, result)

  bus.event_history.set(event.event_id, event)

  const output = bus.logTree()

  assert.ok(output.includes(`${bus.label}.running_handler#`))
  assert.ok(output.includes('RootEvent#'))
})
