import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const PingEvent = BaseEvent.extend('PingEvent', { value: z.number() })

test('events forward between buses without duplication', async () => {
  const bus_a = new EventBus('BusA')
  const bus_b = new EventBus('BusB')
  const bus_c = new EventBus('BusC')

  const seen_a: string[] = []
  const seen_b: string[] = []
  const seen_c: string[] = []

  bus_a.on(PingEvent, (event) => {
    seen_a.push(event.event_id)
  })

  bus_b.on(PingEvent, (event) => {
    seen_b.push(event.event_id)
  })

  bus_c.on(PingEvent, (event) => {
    seen_c.push(event.event_id)
  })

  bus_a.on('*', bus_b.dispatch)
  bus_b.on('*', bus_c.dispatch)

  const event = bus_a.dispatch(PingEvent({ value: 1 }))

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  assert.equal(seen_a.length, 1)
  assert.equal(seen_b.length, 1)
  assert.equal(seen_c.length, 1)

  assert.equal(seen_a[0], event.event_id)
  assert.equal(seen_b[0], event.event_id)
  assert.equal(seen_c[0], event.event_id)

  assert.deepEqual(event.event_path, [bus_a.label, bus_b.label, bus_c.label])
})

test('forwarding disambiguates buses that share the same name', async () => {
  const bus_a = new EventBus('SharedName')
  const bus_b = new EventBus('SharedName')

  const seen_a: string[] = []
  const seen_b: string[] = []

  bus_a.on(PingEvent, (event) => {
    seen_a.push(event.event_id)
  })

  bus_b.on(PingEvent, (event) => {
    seen_b.push(event.event_id)
  })

  bus_a.on('*', bus_b.dispatch)

  const event = bus_a.dispatch(PingEvent({ value: 99 }))

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  assert.equal(seen_a.length, 1)
  assert.equal(seen_b.length, 1)
  assert.equal(seen_a[0], event.event_id)
  assert.equal(seen_b[0], event.event_id)
  assert.deepEqual(event.event_path, [bus_a.label, bus_b.label])
})

test('await event.done waits for handlers on forwarded buses', async () => {
  const bus_a = new EventBus('BusA')
  const bus_b = new EventBus('BusB')
  const bus_c = new EventBus('BusC')

  const completion_log: string[] = []

  const delay = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      setTimeout(resolve, ms)
    })

  bus_a.on(PingEvent, async () => {
    await delay(10)
    completion_log.push('A')
  })

  bus_b.on(PingEvent, async () => {
    await delay(30)
    completion_log.push('B')
  })

  bus_c.on(PingEvent, async () => {
    await delay(50)
    completion_log.push('C')
  })

  bus_a.on('*', bus_b.dispatch)
  bus_b.on('*', bus_c.dispatch)

  const event = bus_a.dispatch(PingEvent({ value: 2 }))

  await event.done()

  assert.deepEqual(completion_log.sort(), ['A', 'B', 'C'])
  assert.equal(event.event_pending_bus_count, 0)
})

test('circular forwarding A->B->C->A does not loop', async () => {
  const peer1 = new EventBus('Peer1')
  const peer2 = new EventBus('Peer2')
  const peer3 = new EventBus('Peer3')

  const events_at_peer1: string[] = []
  const events_at_peer2: string[] = []
  const events_at_peer3: string[] = []

  peer1.on(PingEvent, (event) => {
    events_at_peer1.push(event.event_id)
  })
  peer2.on(PingEvent, (event) => {
    events_at_peer2.push(event.event_id)
  })
  peer3.on(PingEvent, (event) => {
    events_at_peer3.push(event.event_id)
  })

  // Create a full cycle: Peer1 -> Peer2 -> Peer3 -> Peer1
  peer1.on('*', peer2.dispatch)
  peer2.on('*', peer3.dispatch)
  peer3.on('*', peer1.dispatch) // completes the circle

  const event = peer1.dispatch(PingEvent({ value: 42 }))

  await peer1.waitUntilIdle()
  await peer2.waitUntilIdle()
  await peer3.waitUntilIdle()

  // Each peer must see the event exactly once (no infinite loop)
  assert.equal(events_at_peer1.length, 1)
  assert.equal(events_at_peer2.length, 1)
  assert.equal(events_at_peer3.length, 1)

  // All saw the same event
  assert.equal(events_at_peer1[0], event.event_id)
  assert.equal(events_at_peer2[0], event.event_id)
  assert.equal(events_at_peer3[0], event.event_id)

  // event_path shows propagation order without looping back
  assert.deepEqual(event.event_path, [peer1.label, peer2.label, peer3.label])

  // --- Start from a different peer in the same cycle ---
  events_at_peer1.length = 0
  events_at_peer2.length = 0
  events_at_peer3.length = 0

  const event2 = peer2.dispatch(PingEvent({ value: 99 }))

  await peer1.waitUntilIdle()
  await peer2.waitUntilIdle()
  await peer3.waitUntilIdle()

  // Each peer sees it exactly once
  assert.equal(events_at_peer1.length, 1)
  assert.equal(events_at_peer2.length, 1)
  assert.equal(events_at_peer3.length, 1)

  // Path starts at Peer2, goes to Peer3, then Peer1 (stops before looping back to Peer2)
  assert.deepEqual(event2.event_path, [peer2.label, peer3.label, peer1.label])
})

test('await event.done waits when forwarding handler is async-delayed', async () => {
  const bus_a = new EventBus('BusA')
  const bus_b = new EventBus('BusB')

  const delay = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      setTimeout(resolve, ms)
    })

  let bus_a_done = false
  let bus_b_done = false

  bus_a.on(PingEvent, async () => {
    await delay(20)
    bus_a_done = true
  })

  bus_b.on(PingEvent, async () => {
    await delay(10)
    bus_b_done = true
  })

  bus_a.on('*', async (event) => {
    await delay(30)
    bus_b.dispatch(event)
  })

  const event = bus_a.dispatch(PingEvent({ value: 3 }))
  await event.done()

  assert.equal(bus_a_done, true)
  assert.equal(bus_b_done, true)
  assert.equal(event.event_pending_bus_count, 0)
  assert.deepEqual(event.event_path, [bus_a.label, bus_b.label])
})

// Consolidated from tests/fifo.test.ts

const OrderEvent = BaseEvent.extend('OrderEvent', { order: z.number() })

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('events are processed in FIFO order', async () => {
  const bus = new EventBus('FifoBus')

  const processed_orders: number[] = []
  const handler_start_times: number[] = []

  bus.on(OrderEvent, async (event) => {
    handler_start_times.push(Date.now())
    if (event.order % 2 === 0) {
      await delay(30)
    } else {
      await delay(5)
    }
    processed_orders.push(event.order)
  })

  for (let i = 0; i < 10; i += 1) {
    bus.dispatch(OrderEvent({ order: i }))
  }

  await bus.waitUntilIdle()

  assert.deepEqual(
    processed_orders,
    Array.from({ length: 10 }, (_, i) => i)
  )
  for (let i = 1; i < handler_start_times.length; i += 1) {
    assert.ok(handler_start_times[i] >= handler_start_times[i - 1])
  }
})
