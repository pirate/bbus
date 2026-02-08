import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus } from '../src/index.js'

const ParentEvent = BaseEvent.extend('ParentEvent', {})
const ImmediateChildEvent = BaseEvent.extend('ImmediateChildEvent', {})
const QueuedChildEvent = BaseEvent.extend('QueuedChildEvent', {})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('comprehensive patterns: forwarding, async/sync dispatch, parent tracking', async () => {
  const bus_1 = new EventBus('bus1')
  const bus_2 = new EventBus('bus2')

  const results: Array<[number, string]> = []
  const execution_counter = { count: 0 }

  const child_bus2_event_handler = (event: BaseEvent): string => {
    execution_counter.count += 1
    const seq = execution_counter.count
    const event_type_short = event.event_type.replace(/Event$/, '')
    results.push([seq, `bus2_handler_${event_type_short}`])
    return 'forwarded bus result'
  }

  bus_2.on('*', child_bus2_event_handler)
  bus_1.on('*', bus_2.dispatch)

  const parent_bus1_handler = async (event: BaseEvent): Promise<string> => {
    execution_counter.count += 1
    const seq = execution_counter.count
    results.push([seq, 'parent_start'])

    const child_event_async = event.bus?.emit(QueuedChildEvent({}))!
    assert.notEqual(child_event_async.event_status, 'completed')

    const child_event_sync = await event.bus?.emit(ImmediateChildEvent({})).done()!
    assert.equal(child_event_sync.event_status, 'completed')

    assert.ok(child_event_sync.event_path.includes('bus2'))
    assert.ok(Array.from(child_event_sync.event_results.values()).some((result) => result.handler_name.includes('dispatch')))

    assert.equal(child_event_async.event_parent_id, event.event_id)
    assert.equal(child_event_sync.event_parent_id, event.event_id)

    execution_counter.count += 1
    const end_seq = execution_counter.count
    results.push([end_seq, 'parent_end'])
    return 'parent_done'
  }

  bus_1.on(ParentEvent, parent_bus1_handler)

  const parent_event = bus_1.dispatch(ParentEvent({}))
  await parent_event.done()
  await bus_1.waitUntilIdle()
  await bus_2.waitUntilIdle()

  const event_children = Array.from(bus_1.event_history.values()).filter(
    (event) => event.event_type === 'ImmediateChildEvent' || event.event_type === 'QueuedChildEvent'
  )
  assert.ok(event_children.length > 0)
  assert.ok(event_children.every((event) => event.event_parent_id === parent_event.event_id))

  const sorted_results = results.slice().sort((a, b) => a[0] - b[0])
  const execution_order = sorted_results.map((item) => item[1])

  assert.equal(execution_order[0], 'parent_start')
  assert.ok(execution_order.includes('bus2_handler_ImmediateChild'))

  if (execution_order.includes('parent_end')) {
    const parent_end_idx = execution_order.indexOf('parent_end')
    assert.ok(parent_end_idx > 1)
  }

  assert.equal(execution_order.filter((value) => value === 'bus2_handler_ImmediateChild').length, 1)
  assert.equal(execution_order.filter((value) => value === 'bus2_handler_QueuedChild').length, 1)
  assert.equal(execution_order.filter((value) => value === 'bus2_handler_Parent').length, 1)
})

test('race condition stress', async () => {
  const bus_1 = new EventBus('bus1')
  const bus_2 = new EventBus('bus2')
  const RootEvent = BaseEvent.extend('RootEvent', {})

  const results: string[] = []

  const child_handler = async (event: BaseEvent): Promise<string> => {
    const bus_name = event.event_path[event.event_path.length - 1] ?? 'unknown'
    results.push(`child_${bus_name}`)
    await delay(1)
    return `child_done_${bus_name}`
  }

  const parent_handler = async (event: BaseEvent): Promise<string> => {
    const children: BaseEvent[] = []

    for (let i = 0; i < 3; i += 1) {
      children.push(event.bus?.emit(QueuedChildEvent({}))!)
    }

    for (let i = 0; i < 3; i += 1) {
      const child = await event.bus?.emit(ImmediateChildEvent({})).done()!
      assert.equal(child.event_status, 'completed')
      children.push(child)
    }

    assert.ok(children.every((child) => child.event_parent_id === event.event_id))
    return 'parent_done'
  }

  const bad_handler = (_bad: BaseEvent): void => {}

  bus_1.on('*', bus_2.dispatch)
  bus_1.on(QueuedChildEvent, child_handler)
  bus_1.on(ImmediateChildEvent, child_handler)
  bus_2.on(QueuedChildEvent, child_handler)
  bus_2.on(ImmediateChildEvent, child_handler)
  bus_1.on(RootEvent, parent_handler)
  bus_1.on(RootEvent, bad_handler)

  for (let run = 0; run < 5; run += 1) {
    results.length = 0

    const event = bus_1.dispatch(RootEvent({}))
    await event.done()
    await bus_1.waitUntilIdle()
    await bus_2.waitUntilIdle()

    assert.equal(
      results.filter((value) => value === 'child_bus1').length,
      6,
      `Run ${run}: Expected 6 child_bus1, got ${results.filter((value) => value === 'child_bus1').length}`
    )
    assert.equal(
      results.filter((value) => value === 'child_bus2').length,
      6,
      `Run ${run}: Expected 6 child_bus2, got ${results.filter((value) => value === 'child_bus2').length}`
    )
  }
})

test('awaited child jumps queue without overshoot', async () => {
  const bus = new EventBus('TestBus', { max_history_size: 100 })
  const execution_order: string[] = []
  const debug_order: Array<{ label: string; at: string }> = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})
  const Event3 = BaseEvent.extend('Event3', {})
  const LocalChildEvent = BaseEvent.extend('ChildEvent', {})

  const event1_handler = async (_event: BaseEvent): Promise<string> => {
    execution_order.push('Event1_start')
    debug_order.push({ label: 'Event1_start', at: new Date().toISOString() })
    const child = _event.bus?.emit(LocalChildEvent({}))!
    execution_order.push('Child_dispatched')
    debug_order.push({ label: 'Child_dispatched', at: new Date().toISOString() })
    await child.done()
    execution_order.push('Child_await_returned')
    debug_order.push({ label: 'Child_await_returned', at: new Date().toISOString() })
    execution_order.push('Event1_end')
    debug_order.push({ label: 'Event1_end', at: new Date().toISOString() })
    return 'event1_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Event2_start')
    debug_order.push({ label: 'Event2_start', at: new Date().toISOString() })
    execution_order.push('Event2_end')
    debug_order.push({ label: 'Event2_end', at: new Date().toISOString() })
    return 'event2_done'
  }

  const event3_handler = async (): Promise<string> => {
    execution_order.push('Event3_start')
    debug_order.push({ label: 'Event3_start', at: new Date().toISOString() })
    execution_order.push('Event3_end')
    debug_order.push({ label: 'Event3_end', at: new Date().toISOString() })
    return 'event3_done'
  }

  const child_handler = async (): Promise<string> => {
    execution_order.push('Child_start')
    debug_order.push({ label: 'Child_start', at: new Date().toISOString() })
    execution_order.push('Child_end')
    debug_order.push({ label: 'Child_end', at: new Date().toISOString() })
    return 'child_done'
  }

  bus.on(Event1, event1_handler)
  bus.on(Event2, event2_handler)
  bus.on(Event3, event3_handler)
  bus.on(LocalChildEvent, child_handler)

  const event_1 = bus.dispatch(Event1({}))
  const event_2 = bus.dispatch(Event2({}))
  const event_3 = bus.dispatch(Event3({}))

  // Wait for everything to complete
  await event_1.done()
  await bus.waitUntilIdle()

  // Core assertion: child jumped the queue and ran DURING Event1's handler
  assert.ok(execution_order.includes('Child_start'))
  assert.ok(execution_order.includes('Child_end'))
  const child_start_idx = execution_order.indexOf('Child_start')
  const child_end_idx = execution_order.indexOf('Child_end')
  const event1_end_idx = execution_order.indexOf('Event1_end')
  assert.ok(child_start_idx < event1_end_idx, 'child must start before Event1 handler returns')
  assert.ok(child_end_idx < event1_end_idx, 'child must end before Event1 handler returns')

  // No overshoot: Event2 and Event3 must only start AFTER Event1's handler fully completes.
  // In JS, the microtask-based runloop processes them after Event1 completes (so they may
  // already be done by this point), but the key guarantee is ordering, not timing.
  const event2_start_idx = execution_order.indexOf('Event2_start')
  const event3_start_idx = execution_order.indexOf('Event3_start')
  assert.ok(event2_start_idx > event1_end_idx, 'Event2 must not start until Event1 handler returns')
  assert.ok(event3_start_idx > event1_end_idx, 'Event3 must not start until Event1 handler returns')

  // FIFO preserved among queued events
  assert.ok(event2_start_idx < event3_start_idx, 'Event2 must start before Event3 (FIFO)')

  // All events completed
  assert.equal(event_1.event_status, 'completed')
  assert.equal(event_2.event_status, 'completed')
  assert.equal(event_3.event_status, 'completed')

  // Timestamp ordering confirms the same
  const history_list = Array.from(bus.event_history.values())
  const child_event = history_list.find((event) => event.event_type === 'ChildEvent')
  const event2_from_history = history_list.find((event) => event.event_type === 'Event2')
  const event3_from_history = history_list.find((event) => event.event_type === 'Event3')

  assert.ok(child_event?.event_started_ts !== undefined)
  assert.ok(event2_from_history?.event_started_ts !== undefined)
  assert.ok(event3_from_history?.event_started_ts !== undefined)

  assert.ok(child_event!.event_started_ts! <= event2_from_history!.event_started_ts!)
  assert.ok(child_event!.event_started_ts! <= event3_from_history!.event_started_ts!)
})

test('done() on non-proxied event keeps bus paused during queue-jump', async () => {
  const bus = new EventBus('RawDoneBus', { max_history_size: 100 })
  const Event1 = BaseEvent.extend('Event1', {})
  const ChildEvent = BaseEvent.extend('RawChild', {})

  let paused_after_done = false

  bus.on(ChildEvent, () => {})

  bus.on(Event1, async (_event) => {
    // Dispatch child via the raw bus (not the proxied event.bus)
    const child = bus.dispatch(ChildEvent({}))
    // Get the raw (non-proxied) event
    const raw_child = child._event_original ?? child
    // done() on raw event bypasses handler_result injection from proxy
    await raw_child.done()
    // After done() returns, bus should still be paused because
    // we're still inside a handler doing queue-jump processing
    paused_after_done = bus.locks.isPaused()
  })

  bus.dispatch(Event1({}))
  await bus.waitUntilIdle()

  assert.equal(paused_after_done, true, 'bus should be paused after raw done() but before handler returns')
})

test('bus pause state clears after queue-jump completes', async () => {
  const bus = new EventBus('DepthBalanceBus', { max_history_size: 100 })
  const Event1 = BaseEvent.extend('DepthEvent1', {})
  const ChildA = BaseEvent.extend('DepthChildA', {})
  const ChildB = BaseEvent.extend('DepthChildB', {})

  let paused_during_handler = false
  let paused_between_dones = false
  let paused_after_second_done = false

  bus.on(ChildA, () => {})
  bus.on(ChildB, () => {})

  bus.on(Event1, async (event) => {
    // First queue-jump
    const child_a = event.bus?.emit(ChildA({}))!
    await child_a.done()
    paused_during_handler = bus.locks.isPaused()

    // Second queue-jump — bus should remain paused across both awaits.
    const child_b = event.bus?.emit(ChildB({}))!
    paused_between_dones = bus.locks.isPaused()
    await child_b.done()
    paused_after_second_done = bus.locks.isPaused()
  })

  bus.dispatch(Event1({}))
  await bus.waitUntilIdle()

  // During handler, pause should still be held.
  assert.equal(paused_during_handler, true, 'bus should remain paused after first done()')

  // Between done() calls, pause should still be held.
  assert.equal(paused_between_dones, true, 'bus should remain paused between done() calls')

  // After second done(), pause is still held until handler returns.
  assert.equal(paused_after_second_done, true, 'bus should remain paused after second done()')

  // After handler finishes and bus is idle, pause must be released.
  assert.equal(bus.locks.isPaused(), false, 'bus should no longer be paused after handler completes')
})

test('isInsideHandler() is per-bus, not global', async () => {
  const bus_a = new EventBus('InsideHandlerA', { max_history_size: 100 })
  const bus_b = new EventBus('InsideHandlerB', { max_history_size: 100 })

  const EventA = BaseEvent.extend('InsideHandlerEventA', {})
  const EventB = BaseEvent.extend('InsideHandlerEventB', {})

  let bus_a_inside_during_a_handler = false
  let bus_b_inside_during_a_handler = false
  let bus_a_inside_during_b_handler = false
  let bus_b_inside_during_b_handler = false

  bus_a.on(EventA, () => {
    bus_a_inside_during_a_handler = bus_a.locks.isAnyHandlerActive()
    bus_b_inside_during_a_handler = bus_b.locks.isAnyHandlerActive()
  })

  bus_b.on(EventB, () => {
    bus_a_inside_during_b_handler = bus_a.locks.isAnyHandlerActive()
    bus_b_inside_during_b_handler = bus_b.locks.isAnyHandlerActive()
  })

  // Dispatch to bus_a first, wait for completion so bus_b has no active handlers
  await bus_a.dispatch(EventA({})).done()
  await bus_a.waitUntilIdle()

  // Then dispatch to bus_b so bus_a has no active handlers
  await bus_b.dispatch(EventB({})).done()
  await bus_b.waitUntilIdle()

  // During bus_a's handler: bus_a should report inside, bus_b should not
  assert.equal(bus_a_inside_during_a_handler, true, 'bus_a.locks.isAnyHandlerActive() should be true during bus_a handler')
  assert.equal(bus_b_inside_during_a_handler, false, 'bus_b.locks.isAnyHandlerActive() should be false during bus_a handler')

  // During bus_b's handler: bus_b should report inside, bus_a should not
  assert.equal(bus_b_inside_during_b_handler, true, 'bus_b.locks.isAnyHandlerActive() should be true during bus_b handler')
  assert.equal(bus_a_inside_during_b_handler, false, 'bus_a.locks.isAnyHandlerActive() should be false during bus_b handler')

  // After all handlers complete, neither bus should report inside
  assert.equal(bus_a.locks.isAnyHandlerActive(), false, 'bus_a.locks.isAnyHandlerActive() should be false after idle')
  assert.equal(bus_b.locks.isAnyHandlerActive(), false, 'bus_b.locks.isAnyHandlerActive() should be false after idle')
})

test('dispatch multiple, await one skips others until after handler completes', async () => {
  const bus = new EventBus('MultiDispatchBus', { max_history_size: 100 })
  const execution_order: string[] = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})
  const Event3 = BaseEvent.extend('Event3', {})
  const ChildA = BaseEvent.extend('ChildA', {})
  const ChildB = BaseEvent.extend('ChildB', {})
  const ChildC = BaseEvent.extend('ChildC', {})

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push('Event1_start')

    event.bus?.emit(ChildA({}))
    execution_order.push('ChildA_dispatched')

    const child_b = event.bus?.emit(ChildB({}))!
    execution_order.push('ChildB_dispatched')

    event.bus?.emit(ChildC({}))
    execution_order.push('ChildC_dispatched')

    await child_b.done()
    execution_order.push('ChildB_await_returned')

    execution_order.push('Event1_end')
    return 'event1_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Event2_start')
    execution_order.push('Event2_end')
    return 'event2_done'
  }

  const event3_handler = async (): Promise<string> => {
    execution_order.push('Event3_start')
    execution_order.push('Event3_end')
    return 'event3_done'
  }

  const child_a_handler = async (): Promise<string> => {
    execution_order.push('ChildA_start')
    execution_order.push('ChildA_end')
    return 'child_a_done'
  }

  const child_b_handler = async (): Promise<string> => {
    execution_order.push('ChildB_start')
    execution_order.push('ChildB_end')
    return 'child_b_done'
  }

  const child_c_handler = async (): Promise<string> => {
    execution_order.push('ChildC_start')
    execution_order.push('ChildC_end')
    return 'child_c_done'
  }

  bus.on(Event1, event1_handler)
  bus.on(Event2, event2_handler)
  bus.on(Event3, event3_handler)
  bus.on(ChildA, child_a_handler)
  bus.on(ChildB, child_b_handler)
  bus.on(ChildC, child_c_handler)

  const event_1 = bus.dispatch(Event1({}))
  bus.dispatch(Event2({}))
  bus.dispatch(Event3({}))

  await event_1.done()

  assert.ok(execution_order.includes('ChildB_start'))
  assert.ok(execution_order.includes('ChildB_end'))

  const child_b_end_idx = execution_order.indexOf('ChildB_end')
  const event1_end_idx = execution_order.indexOf('Event1_end')
  assert.ok(child_b_end_idx < event1_end_idx)

  if (execution_order.includes('ChildA_start')) {
    const child_a_start_idx = execution_order.indexOf('ChildA_start')
    assert.ok(child_a_start_idx > event1_end_idx)
  }
  if (execution_order.includes('ChildC_start')) {
    const child_c_start_idx = execution_order.indexOf('ChildC_start')
    assert.ok(child_c_start_idx > event1_end_idx)
  }
  if (execution_order.includes('Event2_start')) {
    const event2_start_idx = execution_order.indexOf('Event2_start')
    assert.ok(event2_start_idx > event1_end_idx)
  }
  if (execution_order.includes('Event3_start')) {
    const event3_start_idx = execution_order.indexOf('Event3_start')
    assert.ok(event3_start_idx > event1_end_idx)
  }

  await bus.waitUntilIdle()

  const event2_start_idx = execution_order.indexOf('Event2_start')
  const event3_start_idx = execution_order.indexOf('Event3_start')
  const child_a_start_idx = execution_order.indexOf('ChildA_start')
  const child_c_start_idx = execution_order.indexOf('ChildC_start')

  assert.ok(event2_start_idx < event3_start_idx)
  assert.ok(event3_start_idx < child_a_start_idx)
  assert.ok(child_a_start_idx < child_c_start_idx)
})

test('multi-bus queues are independent when awaiting child', async () => {
  const bus_1 = new EventBus('Bus1', { max_history_size: 100 })
  const bus_2 = new EventBus('Bus2', { max_history_size: 100 })
  const execution_order: string[] = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})
  const Event3 = BaseEvent.extend('Event3', {})
  const Event4 = BaseEvent.extend('Event4', {})
  const LocalChildEvent = BaseEvent.extend('ChildEvent', {})

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push('Bus1_Event1_start')
    const child = event.bus?.emit(LocalChildEvent({}))!
    execution_order.push('Child_dispatched_to_Bus1')
    await child.done()
    execution_order.push('Child_await_returned')
    execution_order.push('Bus1_Event1_end')
    return 'event1_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Bus1_Event2_start')
    execution_order.push('Bus1_Event2_end')
    return 'event2_done'
  }

  const event3_handler = async (): Promise<string> => {
    execution_order.push('Bus2_Event3_start')
    execution_order.push('Bus2_Event3_end')
    return 'event3_done'
  }

  const event4_handler = async (): Promise<string> => {
    execution_order.push('Bus2_Event4_start')
    execution_order.push('Bus2_Event4_end')
    return 'event4_done'
  }

  const child_handler = async (): Promise<string> => {
    execution_order.push('Child_start')
    execution_order.push('Child_end')
    return 'child_done'
  }

  bus_1.on(Event1, event1_handler)
  bus_1.on(Event2, event2_handler)
  bus_1.on(LocalChildEvent, child_handler)

  bus_2.on(Event3, event3_handler)
  bus_2.on(Event4, event4_handler)

  const event_1 = bus_1.dispatch(Event1({}))
  bus_1.dispatch(Event2({}))
  bus_2.dispatch(Event3({}))
  bus_2.dispatch(Event4({}))

  await delay(0)

  await event_1.done()

  assert.ok(execution_order.includes('Child_start'))
  assert.ok(execution_order.includes('Child_end'))

  const child_end_idx = execution_order.indexOf('Child_end')
  const event1_end_idx = execution_order.indexOf('Bus1_Event1_end')
  assert.ok(child_end_idx < event1_end_idx)

  const bus1_event2_start_idx = execution_order.indexOf('Bus1_Event2_start')
  if (bus1_event2_start_idx !== -1) {
    assert.ok(bus1_event2_start_idx > event1_end_idx)
  }

  const bus2_event3_start_idx = execution_order.indexOf('Bus2_Event3_start')
  const bus2_event4_start_idx = execution_order.indexOf('Bus2_Event4_start')
  assert.ok(bus2_event3_start_idx !== -1 || bus2_event4_start_idx !== -1)
  const bus2_start_idx =
    bus2_event3_start_idx === -1
      ? bus2_event4_start_idx
      : bus2_event4_start_idx === -1
        ? bus2_event3_start_idx
        : Math.min(bus2_event3_start_idx, bus2_event4_start_idx)
  assert.ok(bus2_start_idx < event1_end_idx)

  await bus_1.waitUntilIdle()
  await bus_2.waitUntilIdle()

  assert.ok(execution_order.includes('Bus1_Event2_start'))
  assert.ok(execution_order.includes('Bus2_Event3_start'))
  assert.ok(execution_order.includes('Bus2_Event4_start'))
})

test('awaiting an already completed event is a no-op', async () => {
  const bus = new EventBus('AlreadyCompletedBus', { max_history_size: 100 })
  const execution_order: string[] = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})

  const event1_handler = async (): Promise<string> => {
    execution_order.push('Event1_start')
    execution_order.push('Event1_end')
    return 'event1_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Event2_start')
    execution_order.push('Event2_end')
    return 'event2_done'
  }

  bus.on(Event1, event1_handler)
  bus.on(Event2, event2_handler)

  const event_1 = await bus.dispatch(Event1({})).done()
  assert.equal(event_1.event_status, 'completed')

  const event_2 = bus.dispatch(Event2({}))

  await event_1.done()

  assert.equal(event_2.event_status, 'pending')

  await bus.waitUntilIdle()
})

test('multiple awaits on same event', async () => {
  const bus = new EventBus('MultiAwaitBus', { max_history_size: 100 })
  const execution_order: string[] = []
  const await_results: string[] = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})
  const LocalChildEvent = BaseEvent.extend('ChildEvent', {})

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push('Event1_start')

    const child = event.bus?.emit(LocalChildEvent({}))!

    const await_child = async (name: string): Promise<void> => {
      await child.done()
      await_results.push(`${name}_completed`)
    }

    await Promise.all([await_child('await1'), await_child('await2')])
    execution_order.push('Both_awaits_completed')
    execution_order.push('Event1_end')
    return 'event1_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Event2_start')
    execution_order.push('Event2_end')
    return 'event2_done'
  }

  const child_handler = async (): Promise<string> => {
    execution_order.push('Child_start')
    await delay(10)
    execution_order.push('Child_end')
    return 'child_done'
  }

  bus.on(Event1, event1_handler)
  bus.on(Event2, event2_handler)
  bus.on(LocalChildEvent, child_handler)

  const event_1 = bus.dispatch(Event1({}))
  bus.dispatch(Event2({}))

  await event_1.done()

  assert.equal(await_results.length, 2)
  assert.ok(await_results.includes('await1_completed'))
  assert.ok(await_results.includes('await2_completed'))

  assert.ok(execution_order.includes('Child_start'))
  assert.ok(execution_order.includes('Child_end'))
  const child_end_idx = execution_order.indexOf('Child_end')
  const event1_end_idx = execution_order.indexOf('Event1_end')
  assert.ok(child_end_idx < event1_end_idx)

  assert.ok(!execution_order.includes('Event2_start'))

  await bus.waitUntilIdle()
})

test('deeply nested awaited children', async () => {
  const bus = new EventBus('DeepNestedBus', { max_history_size: 100 })
  const execution_order: string[] = []

  const Event1 = BaseEvent.extend('Event1', {})
  const Event2 = BaseEvent.extend('Event2', {})
  const Child1 = BaseEvent.extend('Child1', {})
  const Child2 = BaseEvent.extend('Child2', {})

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push('Event1_start')
    const child1 = event.bus?.emit(Child1({}))!
    await child1.done()
    execution_order.push('Event1_end')
    return 'event1_done'
  }

  const child1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push('Child1_start')
    const child2 = event.bus?.emit(Child2({}))!
    await child2.done()
    execution_order.push('Child1_end')
    return 'child1_done'
  }

  const child2_handler = async (): Promise<string> => {
    execution_order.push('Child2_start')
    execution_order.push('Child2_end')
    return 'child2_done'
  }

  const event2_handler = async (): Promise<string> => {
    execution_order.push('Event2_start')
    execution_order.push('Event2_end')
    return 'event2_done'
  }

  bus.on(Event1, event1_handler)
  bus.on(Child1, child1_handler)
  bus.on(Child2, child2_handler)
  bus.on(Event2, event2_handler)

  const event_1 = bus.dispatch(Event1({}))
  bus.dispatch(Event2({}))

  await event_1.done()

  assert.ok(execution_order.includes('Child1_start'))
  assert.ok(execution_order.includes('Child1_end'))
  assert.ok(execution_order.includes('Child2_start'))
  assert.ok(execution_order.includes('Child2_end'))

  const child2_end_idx = execution_order.indexOf('Child2_end')
  const child1_end_idx = execution_order.indexOf('Child1_end')
  const event1_end_idx = execution_order.indexOf('Event1_end')
  assert.ok(child2_end_idx < child1_end_idx)
  assert.ok(child1_end_idx < event1_end_idx)

  assert.ok(!execution_order.includes('Event2_start'))

  await bus.waitUntilIdle()

  const event2_start_idx = execution_order.indexOf('Event2_start')
  assert.ok(event2_start_idx > event1_end_idx)
})

// =============================================================================
// Queue-Jump Concurrency Tests (Two-Bus)
//
// BUG: runImmediatelyAcrossBuses passes { bypass_handler_semaphores: true,
// bypass_event_semaphores: true } for ALL buses. This causes:
//   1. Handlers to run in parallel regardless of configured concurrency
//   2. Event semaphores on remote buses to be skipped
//
// The fix requires "yield-and-reacquire":
//   - Before processing the child, temporarily RELEASE the semaphore the parent
//     handler holds (the parent is suspended in `await child.done()` and isn't
//     using it).
//   - Process the child event NORMALLY — handlers acquire/release the real
//     semaphore, serializing among themselves as configured.
//   - After the child completes, RE-ACQUIRE the semaphore for the parent handler
//     before it resumes.
//
// For event semaphores, only bypass on the initiating bus (where the parent holds
// the semaphore). On other buses, respect their event concurrency — bypass only
// if they resolve to the SAME semaphore instance (i.e. global-serial).
//
// All tests use two buses. The pattern is:
//   bus_a: origin bus where TriggerEvent handler dispatches a child
//   bus_b: forward bus that also handles the child event
//   The trigger handler dispatches the child on bus_a and also to bus_b,
//   then awaits child.done(), which queue-jumps the child on both buses.
// =============================================================================

test('BUG: queue-jump two-bus bus-serial handlers should serialize on each bus', async () => {
  const TriggerEvent = BaseEvent.extend('QJ2BS_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJ2BS_Child', {})

  const bus_a = new EventBus('QJ2BS_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('QJ2BS_B', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })

  const log: string[] = []

  // Two handlers per bus. handler_1 is slow (15ms), handler_2 is fast (5ms).
  // With bus-serial, handler_1 must finish before handler_2 starts ON EACH BUS.
  // With buggy parallel, both start simultaneously and handler_2 finishes first.
  const a_handler_1 = async () => {
    log.push('a1_start')
    await delay(15)
    log.push('a1_end')
  }
  const a_handler_2 = async () => {
    log.push('a2_start')
    await delay(5)
    log.push('a2_end')
  }
  const b_handler_1 = async () => {
    log.push('b1_start')
    await delay(15)
    log.push('b1_end')
  }
  const b_handler_2 = async () => {
    log.push('b2_start')
    await delay(5)
    log.push('b2_end')
  }

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })
  bus_a.on(ChildEvent, a_handler_1)
  bus_a.on(ChildEvent, a_handler_2)
  bus_b.on(ChildEvent, b_handler_1)
  bus_b.on(ChildEvent, b_handler_2)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // Bus A: handlers must serialize (a1 finishes before a2 starts)
  const a1_end = log.indexOf('a1_end')
  const a2_start = log.indexOf('a2_start')
  assert.ok(a1_end >= 0 && a2_start >= 0, 'bus_a handlers should have run')
  assert.ok(a1_end < a2_start, `bus_a (bus-serial): a1 should finish before a2 starts. Got: [${log.join(', ')}]`)

  // Bus B: handlers must serialize (b1 finishes before b2 starts)
  const b1_end = log.indexOf('b1_end')
  const b2_start = log.indexOf('b2_start')
  assert.ok(b1_end >= 0 && b2_start >= 0, 'bus_b handlers should have run')
  assert.ok(b1_end < b2_start, `bus_b (bus-serial): b1 should finish before b2 starts. Got: [${log.join(', ')}]`)
})

test('BUG: queue-jump two-bus global-serial handlers should serialize across both buses', async () => {
  const TriggerEvent = BaseEvent.extend('QJ2GS_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJ2GS_Child', {})

  // Global-serial means ONE handler at a time GLOBALLY, across all buses.
  const bus_a = new EventBus('QJ2GS_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'global-serial',
  })
  const bus_b = new EventBus('QJ2GS_B', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'global-serial',
  })

  const log: string[] = []

  const a_handler_1 = async () => {
    log.push('a1_start')
    await delay(15)
    log.push('a1_end')
  }
  const a_handler_2 = async () => {
    log.push('a2_start')
    await delay(5)
    log.push('a2_end')
  }
  const b_handler_1 = async () => {
    log.push('b1_start')
    await delay(15)
    log.push('b1_end')
  }
  const b_handler_2 = async () => {
    log.push('b2_start')
    await delay(5)
    log.push('b2_end')
  }

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })
  bus_a.on(ChildEvent, a_handler_1)
  bus_a.on(ChildEvent, a_handler_2)
  bus_b.on(ChildEvent, b_handler_1)
  bus_b.on(ChildEvent, b_handler_2)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // With global-serial, no two handlers should overlap anywhere.
  // runImmediatelyAcrossBuses processes buses sequentially (bus_a first,
  // then bus_b), so the expected order is strictly serial:
  //   a1_start, a1_end, a2_start, a2_end, b1_start, b1_end, b2_start, b2_end
  //
  // With the bug (bypass), all handlers on a bus run in parallel:
  //   a1_start, a2_start, a2_end, a1_end, b1_start, b2_start, b2_end, b1_end

  // Check: within bus_a, handlers are serial
  const a1_end = log.indexOf('a1_end')
  const a2_start = log.indexOf('a2_start')
  assert.ok(a1_end < a2_start, `global-serial: a1 should finish before a2 starts. Got: [${log.join(', ')}]`)

  // Check: within bus_b, handlers are serial
  const b1_end = log.indexOf('b1_end')
  const b2_start = log.indexOf('b2_start')
  assert.ok(b1_end < b2_start, `global-serial: b1 should finish before b2 starts. Got: [${log.join(', ')}]`)

  // Check: bus_a handlers all finish before bus_b handlers start
  // (because runImmediatelyAcrossBuses processes sequentially and
  // all share LockManager.global_handler_semaphore)
  const a2_end = log.indexOf('a2_end')
  const b1_start = log.indexOf('b1_start')
  assert.ok(a2_end < b1_start, `global-serial: bus_a should finish before bus_b starts. Got: [${log.join(', ')}]`)
})

test('BUG: queue-jump two-bus mixed: bus_a bus-serial, bus_b parallel', async () => {
  const TriggerEvent = BaseEvent.extend('QJ2Mix1_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJ2Mix1_Child', {})

  const bus_a = new EventBus('QJ2Mix1_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('QJ2Mix1_B', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel', // bus_b handlers should run in parallel
  })

  const log: string[] = []

  const a_handler_1 = async () => {
    log.push('a1_start')
    await delay(15)
    log.push('a1_end')
  }
  const a_handler_2 = async () => {
    log.push('a2_start')
    await delay(5)
    log.push('a2_end')
  }
  const b_handler_1 = async () => {
    log.push('b1_start')
    await delay(15)
    log.push('b1_end')
  }
  const b_handler_2 = async () => {
    log.push('b2_start')
    await delay(5)
    log.push('b2_end')
  }

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })
  bus_a.on(ChildEvent, a_handler_1)
  bus_a.on(ChildEvent, a_handler_2)
  bus_b.on(ChildEvent, b_handler_1)
  bus_b.on(ChildEvent, b_handler_2)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // Bus A (bus-serial): a1 must finish before a2 starts
  const a1_end = log.indexOf('a1_end')
  const a2_start = log.indexOf('a2_start')
  assert.ok(a1_end < a2_start, `bus_a (bus-serial): a1 should finish before a2 starts. Got: [${log.join(', ')}]`)

  // Bus B (parallel): both handlers should start before the slower one finishes.
  // b2 (5ms) starts and finishes before b1 (15ms) finishes.
  const b1_end = log.indexOf('b1_end')
  const b2_start = log.indexOf('b2_start')
  assert.ok(b2_start < b1_end, `bus_b (parallel): b2 should start before b1 finishes. Got: [${log.join(', ')}]`)
})

test('BUG: queue-jump two-bus mixed: bus_a parallel, bus_b bus-serial', async () => {
  const TriggerEvent = BaseEvent.extend('QJ2Mix2_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJ2Mix2_Child', {})

  const bus_a = new EventBus('QJ2Mix2_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel', // bus_a handlers should run in parallel
  })
  const bus_b = new EventBus('QJ2Mix2_B', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })

  const log: string[] = []

  const a_handler_1 = async () => {
    log.push('a1_start')
    await delay(15)
    log.push('a1_end')
  }
  const a_handler_2 = async () => {
    log.push('a2_start')
    await delay(5)
    log.push('a2_end')
  }
  const b_handler_1 = async () => {
    log.push('b1_start')
    await delay(15)
    log.push('b1_end')
  }
  const b_handler_2 = async () => {
    log.push('b2_start')
    await delay(5)
    log.push('b2_end')
  }

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })
  bus_a.on(ChildEvent, a_handler_1)
  bus_a.on(ChildEvent, a_handler_2)
  bus_b.on(ChildEvent, b_handler_1)
  bus_b.on(ChildEvent, b_handler_2)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // Bus A (parallel): handlers should overlap
  const a1_end = log.indexOf('a1_end')
  const a2_start = log.indexOf('a2_start')
  assert.ok(a2_start < a1_end, `bus_a (parallel): a2 should start before a1 finishes. Got: [${log.join(', ')}]`)

  // Bus B (bus-serial): b1 must finish before b2 starts
  const b1_end = log.indexOf('b1_end')
  const b2_start = log.indexOf('b2_start')
  assert.ok(b1_end < b2_start, `bus_b (bus-serial): b1 should finish before b2 starts. Got: [${log.join(', ')}]`)
})

// =============================================================================
// Event-level concurrency on the forward bus.
//
// When the forward bus (bus_b) has bus-serial event concurrency and is already
// processing an event, a queue-jumped child should WAIT for bus_b's in-flight
// event to finish. The current code bypasses event semaphores for ALL buses,
// causing the child to cut in front of the in-flight event.
//
// The fix should only bypass event semaphores on the INITIATING bus (where the
// parent event holds the semaphore). On other buses, bypass only if they resolve
// to the SAME semaphore instance (global-serial shares one global semaphore).
// =============================================================================

test('BUG: queue-jump should respect bus-serial event concurrency on forward bus', async () => {
  const TriggerEvent = BaseEvent.extend('QJEvt_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJEvt_Child', {})
  const SlowEvent = BaseEvent.extend('QJEvt_Slow', {})

  const bus_a = new EventBus('QJEvt_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('QJEvt_B', {
    event_concurrency: 'bus-serial', // only one event at a time on bus_b
    event_handler_concurrency: 'bus-serial',
  })

  const log: string[] = []

  // SlowEvent handler: occupies bus_b's event semaphore for 40ms
  bus_b.on(SlowEvent, async () => {
    log.push('slow_start')
    await delay(40)
    log.push('slow_end')
  })

  // ChildEvent handler on bus_b: should only run after SlowEvent finishes
  bus_b.on(ChildEvent, async () => {
    log.push('child_b_start')
    await delay(5)
    log.push('child_b_end')
  })

  // ChildEvent handler on bus_a (so bus_a also processes the child)
  bus_a.on(ChildEvent, async () => {
    log.push('child_a_start')
    await delay(5)
    log.push('child_a_end')
  })

  // TriggerEvent handler: dispatches child to both buses, awaits completion
  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })

  // Step 1: Start a slow event on bus_b so it's busy
  bus_b.dispatch(SlowEvent({ event_timeout: null }))
  await delay(5) // let slow_handler start

  // Step 2: Trigger the queue-jump on bus_a
  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // The child on bus_b should start AFTER the slow event finishes,
  // because bus_b has bus-serial event concurrency.
  const slow_end = log.indexOf('slow_end')
  const child_b_start = log.indexOf('child_b_start')
  assert.ok(slow_end >= 0, 'slow event should have completed')
  assert.ok(child_b_start >= 0, 'child on bus_b should have run')
  assert.ok(
    slow_end < child_b_start,
    `bus_b (bus-serial events): child should wait for slow event to finish. ` + `Got: [${log.join(', ')}]`
  )

  // The child on bus_a should have processed (queue-jumped, bypasses bus_a's event semaphore)
  assert.ok(log.includes('child_a_start'), 'child on bus_a should have run')
  assert.ok(log.includes('child_a_end'), 'child on bus_a should have completed')
})

test('queue-jump with fully-parallel forward bus starts immediately', async () => {
  // When bus_b uses parallel event AND handler concurrency, the queue-jumped
  // child should start immediately even while another event's handler is running.

  const TriggerEvent = BaseEvent.extend('QJFullPar_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJFullPar_Child', {})
  const SlowEvent = BaseEvent.extend('QJFullPar_Slow', {})

  const bus_a = new EventBus('QJFullPar_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('QJFullPar_B', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  const log: string[] = []

  bus_b.on(SlowEvent, async () => {
    log.push('slow_start')
    await delay(40)
    log.push('slow_end')
  })

  bus_b.on(ChildEvent, async () => {
    log.push('child_b_start')
    await delay(5)
    log.push('child_b_end')
  })

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })

  bus_b.dispatch(SlowEvent({ event_timeout: null }))
  await delay(5)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  const slow_end = log.indexOf('slow_end')
  const child_b_start = log.indexOf('child_b_start')
  assert.ok(child_b_start >= 0, 'child on bus_b should have run')
  assert.ok(child_b_start < slow_end, `bus_b (fully parallel): child should start before slow finishes. ` + `Got: [${log.join(', ')}]`)
})

test('queue-jump with parallel events but bus-serial handlers on forward bus serializes handlers', async () => {
  // When bus_b has parallel event concurrency but bus-serial handler concurrency,
  // the child event can start processing immediately (event semaphore is parallel),
  // but its handler must wait for the slow handler to release the handler semaphore.

  const TriggerEvent = BaseEvent.extend('QJEvtParHSer_Trigger', {})
  const ChildEvent = BaseEvent.extend('QJEvtParHSer_Child', {})
  const SlowEvent = BaseEvent.extend('QJEvtParHSer_Slow', {})

  const bus_a = new EventBus('QJEvtParHSer_A', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'bus-serial',
  })
  const bus_b = new EventBus('QJEvtParHSer_B', {
    event_concurrency: 'parallel', // events can start concurrently
    event_handler_concurrency: 'bus-serial', // but handlers serialize
  })

  const log: string[] = []

  bus_b.on(SlowEvent, async () => {
    log.push('slow_start')
    await delay(40)
    log.push('slow_end')
  })

  bus_b.on(ChildEvent, async () => {
    log.push('child_b_start')
    await delay(5)
    log.push('child_b_end')
  })

  bus_a.on(TriggerEvent, async (event: InstanceType<typeof TriggerEvent>) => {
    const child = event.bus?.emit(ChildEvent({ event_timeout: null }))!
    bus_b.dispatch(child)
    await child.done()
  })

  bus_b.dispatch(SlowEvent({ event_timeout: null }))
  await delay(5)

  const top = bus_a.dispatch(TriggerEvent({ event_timeout: null }))
  await top.done()
  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  // With bus-serial handler concurrency, child handler must wait for slow handler
  const slow_end = log.indexOf('slow_end')
  const child_b_start = log.indexOf('child_b_start')
  assert.ok(child_b_start >= 0, 'child on bus_b should have run')
  assert.ok(
    child_b_start > slow_end,
    `bus_b (bus-serial handlers): child handler should wait for slow handler. ` + `Got: [${log.join(', ')}]`
  )
})
