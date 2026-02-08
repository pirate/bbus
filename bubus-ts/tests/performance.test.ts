import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError } from '../src/index.js'

const SimpleEvent = BaseEvent.extend('SimpleEvent', {})

const mb = (bytes: number) => (bytes / 1024 / 1024).toFixed(1)

test('processes 50k events within reasonable time', { timeout: 30_000 }, async () => {
  const total_events = 50_000
  // Keep full history to avoid trimming inflight events during perf runs.
  const bus = new EventBus('PerfBus', { max_history_size: total_events })

  let processed_count = 0
  bus.on(SimpleEvent, () => {
    processed_count += 1
  })

  global.gc?.()
  const mem_before = process.memoryUsage()

  const t0 = Date.now()

  const pending: Array<ReturnType<typeof SimpleEvent>> = []
  for (let i = 0; i < total_events; i += 1) {
    pending.push(bus.dispatch(SimpleEvent({})))
  }

  const t_dispatch = Date.now()
  const mem_dispatch = process.memoryUsage()

  await Promise.all(pending.map((event) => event.done()))
  await bus.waitUntilIdle()

  const t_done = Date.now()
  const mem_done = process.memoryUsage()

  global.gc?.()
  const mem_gc = process.memoryUsage()

  const dispatch_ms = t_dispatch - t0
  const await_ms = t_done - t_dispatch
  const total_ms = t_done - t0

  console.log(
    `\n  perf: ${total_events} events in ${total_ms}ms (${Math.round(total_events / (total_ms / 1000))}/s)` +
      `\n    dispatch: ${dispatch_ms}ms | await: ${await_ms}ms` +
      `\n    memory: before=${mb(mem_before.heapUsed)}MB → dispatch=${mb(mem_dispatch.heapUsed)}MB → done=${mb(mem_done.heapUsed)}MB → gc=${mb(mem_gc.heapUsed)}MB` +
      `\n    per-event: time=${(total_ms / total_events).toFixed(4)}ms | heap=${((mem_done.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB | heap_gc=${((mem_gc.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB` +
      `\n    rss: before=${mb(mem_before.rss)}MB → done=${mb(mem_done.rss)}MB → gc=${mb(mem_gc.rss)}MB`
  )

  assert.equal(processed_count, total_events)
  assert.ok(total_ms < 30_000, `Processing took ${total_ms}ms`)
  assert.ok(bus.event_history.size <= bus.max_history_size!)

  bus.destroy()
})

// Simulates a fastify backend where each request creates its own bus with handlers,
// processes events, then tears down. Tests that bus creation/destruction at scale
// doesn't leak memory or degrade performance.
test('500 ephemeral buses with 100 events each', { timeout: 30_000 }, async () => {
  const total_buses = 500
  const events_per_bus = 100
  const total_events = total_buses * events_per_bus

  let processed_count = 0

  global.gc?.()
  const mem_before = process.memoryUsage()
  const t0 = Date.now()

  for (let b = 0; b < total_buses; b += 1) {
    // Avoid trimming inflight events during perf runs.
    const bus = new EventBus(`ReqBus-${b}`, { max_history_size: events_per_bus })

    bus.on(SimpleEvent, () => {
      processed_count += 1
    })

    const pending: Array<ReturnType<typeof SimpleEvent>> = []
    for (let i = 0; i < events_per_bus; i += 1) {
      pending.push(bus.dispatch(SimpleEvent({})))
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()

    bus.destroy()
  }

  const t_done = Date.now()
  const mem_done = process.memoryUsage()

  global.gc?.()
  const mem_gc = process.memoryUsage()

  const total_ms = t_done - t0

  console.log(
    `\n  perf: ${total_buses} buses × ${events_per_bus} events = ${total_events} total in ${total_ms}ms (${Math.round(total_events / (total_ms / 1000))}/s)` +
      `\n    memory: before=${mb(mem_before.heapUsed)}MB → done=${mb(mem_done.heapUsed)}MB → gc=${mb(mem_gc.heapUsed)}MB` +
      `\n    per-event: time=${(total_ms / total_events).toFixed(4)}ms | heap=${((mem_done.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB | heap_gc=${((mem_gc.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB` +
      `\n    rss: before=${mb(mem_before.rss)}MB → done=${mb(mem_done.rss)}MB → gc=${mb(mem_gc.rss)}MB` +
      `\n    live bus instances: ${EventBus._all_instances.size}`
  )

  assert.equal(processed_count, total_events)
  assert.ok(total_ms < 30_000, `Processing took ${total_ms}ms`)
  // All buses should have been cleaned up from the registry
  assert.equal(EventBus._all_instances.size, 0, 'All buses should be destroyed')
})

// Simulates per-request handler registration pattern: a shared bus where each
// "request" registers a handler with .on(), dispatches events, then removes the
// handler with .off(). Tests for handler map churn overhead and cleanup leaks.
test('50k events with ephemeral on/off handler registration across 2 buses', { timeout: 30_000 }, async () => {
  const RequestEvent = BaseEvent.extend('RequestEvent', {})

  const total_events = 50_000
  // Keep full history to avoid trimming inflight events during perf runs.
  const bus_a = new EventBus('SharedBusA', { max_history_size: total_events })
  const bus_b = new EventBus('SharedBusB', { max_history_size: total_events })
  let processed_a = 0
  let processed_b = 0
  let on_ms = 0
  let off_ms = 0
  let dispatch_a_ms = 0
  let dispatch_b_ms = 0
  let done_ms = 0
  let process_a_ms = 0
  let process_b_ms = 0
  let handler_a_ms = 0
  let handler_b_ms = 0

  // Persistent handler on bus_b that forwards count
  bus_b.on(RequestEvent, () => {
    processed_b += 1
  })

  const bus_a_any = bus_a as any
  const bus_b_any = bus_b as any
  const original_process_a = typeof bus_a_any.processEvent === 'function' ? bus_a_any.processEvent.bind(bus_a) : null
  const original_process_b = typeof bus_b_any.processEvent === 'function' ? bus_b_any.processEvent.bind(bus_b) : null
  const original_run_handler_a = typeof bus_a_any.runEventHandler === 'function' ? bus_a_any.runEventHandler.bind(bus_a) : null
  const original_run_handler_b = typeof bus_b_any.runEventHandler === 'function' ? bus_b_any.runEventHandler.bind(bus_b) : null

  if (original_process_a) {
    bus_a_any.processEvent = async (event: any) => {
      const t = performance.now()
      try {
        return await original_process_a(event)
      } finally {
        process_a_ms += performance.now() - t
      }
    }
  }
  if (original_process_b) {
    bus_b_any.processEvent = async (event: any) => {
      const t = performance.now()
      try {
        return await original_process_b(event)
      } finally {
        process_b_ms += performance.now() - t
      }
    }
  }
  if (original_run_handler_a) {
    bus_a_any.runEventHandler = async (...args: any[]) => {
      const t = performance.now()
      try {
        return await original_run_handler_a(...args)
      } finally {
        handler_a_ms += performance.now() - t
      }
    }
  }
  if (original_run_handler_b) {
    bus_b_any.runEventHandler = async (...args: any[]) => {
      const t = performance.now()
      try {
        return await original_run_handler_b(...args)
      } finally {
        handler_b_ms += performance.now() - t
      }
    }
  }

  global.gc?.()
  const mem_before = process.memoryUsage()
  const t0 = Date.now()

  for (let i = 0; i < total_events; i += 1) {
    // Register ephemeral handler
    const ephemeral_handler = () => {
      processed_a += 1
    }
    let t = performance.now()
    bus_a.on(RequestEvent, ephemeral_handler)
    on_ms += performance.now() - t

    // Dispatch on bus_a, forward to bus_b
    const event = RequestEvent({})
    t = performance.now()
    const ev_a = bus_a.dispatch(event)
    dispatch_a_ms += performance.now() - t
    t = performance.now()
    bus_b.dispatch(event)
    dispatch_b_ms += performance.now() - t

    t = performance.now()
    await ev_a.done()
    done_ms += performance.now() - t

    // Tear down ephemeral handler
    t = performance.now()
    bus_a.off(RequestEvent, ephemeral_handler)
    off_ms += performance.now() - t
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()

  const t_done = Date.now()
  const mem_done = process.memoryUsage()

  global.gc?.()
  const mem_gc = process.memoryUsage()

  const total_ms = t_done - t0

  console.log(
    `\n  perf: ${total_events} events with ephemeral on/off in ${total_ms}ms (${Math.round(total_events / (total_ms / 1000))}/s)` +
      `\n    dispatch: bus_a=${processed_a} | bus_b=${processed_b}` +
      `\n    timings: on=${on_ms.toFixed(0)}ms | off=${off_ms.toFixed(0)}ms | dispatch_a=${dispatch_a_ms.toFixed(0)}ms | dispatch_b=${dispatch_b_ms.toFixed(0)}ms | done=${done_ms.toFixed(0)}ms` +
      `\n    processing: bus_a=${process_a_ms.toFixed(0)}ms | bus_b=${process_b_ms.toFixed(0)}ms | handlers_a=${handler_a_ms.toFixed(0)}ms | handlers_b=${handler_b_ms.toFixed(0)}ms` +
      `\n    memory: before=${mb(mem_before.heapUsed)}MB → done=${mb(mem_done.heapUsed)}MB → gc=${mb(mem_gc.heapUsed)}MB` +
      `\n    per-event: time=${(total_ms / total_events).toFixed(4)}ms | heap=${((mem_done.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB | heap_gc=${((mem_gc.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB` +
      `\n    rss: before=${mb(mem_before.rss)}MB → done=${mb(mem_done.rss)}MB → gc=${mb(mem_gc.rss)}MB` +
      `\n    bus_a handlers: ${bus_a.handlers.size} | bus_b handlers: ${bus_b.handlers.size}`
  )

  assert.equal(processed_a, total_events)
  assert.equal(processed_b, total_events)
  assert.ok(total_ms < 30_000, `Processing took ${total_ms}ms`)
  // Ephemeral handlers should all be cleaned up
  assert.equal(bus_a.handlers.size, 0, 'All ephemeral handlers should be removed from bus_a')
  assert.equal(bus_b.handlers.size, 1, 'bus_b should still have its persistent handler')
  assert.ok(bus_a.event_history.size <= bus_a.max_history_size!)
  assert.ok(bus_b.event_history.size <= bus_b.max_history_size!)

  bus_a.destroy()
  bus_b.destroy()
})

// Worst-case memory leak stress test. Exercises every retention path simultaneously:
// multi-bus forwarding, queue-jumping (done() inside handler), timeouts that cancel
// pending handlers, nested parent-child-grandchild trees, Proxy accumulation from
// getEventProxyScopedToThisBus, ephemeral on/off handler churn, and find() waiter timeouts.
// If any code path leaks references, memory will grow unbounded across 2000 iterations.
test('worst-case: forwarding + queue-jump + timeouts + cancellation at scale', { timeout: 60_000 }, async () => {
  const ParentEvent = BaseEvent.extend('WC_Parent', {
    iteration: z.number(),
  })
  const ChildEvent = BaseEvent.extend('WC_Child', {
    iteration: z.number(),
  })
  const GrandchildEvent = BaseEvent.extend('WC_Grandchild', {
    iteration: z.number(),
  })

  const total_iterations = 2000
  const history_limit = total_iterations * 2
  // Keep enough history to avoid trimming inflight events during perf runs.
  const bus_a = new EventBus('WC_A', { max_history_size: history_limit })
  const bus_b = new EventBus('WC_B', { max_history_size: history_limit })
  const bus_c = new EventBus('WC_C', { max_history_size: history_limit })
  let parent_handled_a = 0
  let parent_handled_b = 0
  let child_handled_c = 0
  let grandchild_handled = 0
  let timeout_count = 0
  let cancel_count = 0

  // Persistent handler on bus_b — just counts
  bus_b.on(ParentEvent, () => {
    parent_handled_b += 1
  })

  // Persistent handler on bus_c — processes child, emits grandchild
  bus_c.on(ChildEvent, async (event) => {
    child_handled_c += 1
    const gc = event.bus?.emit(GrandchildEvent({ iteration: (event as any).iteration }))!
    bus_c.dispatch(gc)
    await gc.done()
  })

  // Persistent handler on bus_c for grandchild — slow on timeout iterations
  // so the child's 5ms timeout fires while this is still sleeping.
  // This creates EventHandlerTimeoutError → EventHandlerCancelledError chains.
  // Sleep is 50ms but child timeout is 5ms — with cancellation of started handlers,
  // the child completes immediately when timeout fires. Background sleep continues
  // silently (JS can't cancel async functions, but the event system moves on).
  bus_c.on(GrandchildEvent, async (event) => {
    grandchild_handled += 1
    if ((event as any).iteration % 5 === 0) {
      await new Promise((r) => setTimeout(r, 50))
    }
  })

  global.gc?.()
  const mem_before = process.memoryUsage()
  const t0 = Date.now()

  for (let i = 0; i < total_iterations; i += 1) {
    const should_timeout = i % 5 === 0

    // Ephemeral handler on bus_a — queue-jumps a child to bus_c
    const ephemeral_handler = async (event: any) => {
      parent_handled_a += 1
      const child_timeout = should_timeout ? 0.005 : null // 5ms timeout → fires while grandchild sleeps 50ms
      const child = event.bus?.emit(
        ChildEvent({
          iteration: i,
          event_timeout: child_timeout,
        })
      )!
      bus_c.dispatch(child)
      try {
        await child.done()
      } catch {
        // Swallow — timeout errors are expected
      }
    }
    bus_a.on(ParentEvent, ephemeral_handler)

    // Dispatch parent to bus_a (with handler) and bus_b (forwarding)
    const parent = ParentEvent({ iteration: i })
    const ev_a = bus_a.dispatch(parent)
    bus_b.dispatch(parent)

    await ev_a.done()
    // Don't waitUntilIdle on bus_c here — timed-out grandchild handlers are
    // still sleeping in the background (JS can't cancel async functions).
    // Let them pile up; the final waitUntilIdle() outside the loop will drain.

    // Deregister ephemeral handler
    bus_a.off(ParentEvent, ephemeral_handler)

    // Periodic find() with short timeout — exercises find_waiter cleanup
    if (i % 10 === 0) {
      // Don't await — let it timeout in the background
      bus_a.find(ParentEvent, { future: 0.001 })
    }
  }

  await bus_a.waitUntilIdle()
  await bus_b.waitUntilIdle()
  await bus_c.waitUntilIdle()

  // Count timeouts and cancellations from bus_c's history
  for (const event of bus_c.event_history.values()) {
    for (const result of event.event_results.values()) {
      if (result.error instanceof EventHandlerTimeoutError) timeout_count += 1
      if (result.error instanceof EventHandlerCancelledError) cancel_count += 1
    }
  }

  const t_done = Date.now()
  const mem_done = process.memoryUsage()

  global.gc?.()
  // Short delay to let find() timeouts and timed-out handler promises settle
  await new Promise((r) => setTimeout(r, 50))
  global.gc?.()
  const mem_gc = process.memoryUsage()

  const total_ms = t_done - t0
  const estimated_events = total_iterations * 3
  const mem_delta_mb = (mem_gc.heapUsed - mem_before.heapUsed) / 1024 / 1024

  console.log(
    `\n  worst-case: ${total_iterations} iterations in ${total_ms}ms (${Math.round(total_iterations / (total_ms / 1000))}/s)` +
      `\n    parent: bus_a=${parent_handled_a} bus_b=${parent_handled_b}` +
      `\n    child: bus_c=${child_handled_c} | grandchild=${grandchild_handled}` +
      `\n    timeouts=${timeout_count} cancellations=${cancel_count}` +
      `\n    memory: before=${mb(mem_before.heapUsed)}MB → done=${mb(mem_done.heapUsed)}MB → gc=${mb(mem_gc.heapUsed)}MB (delta=${mem_delta_mb.toFixed(1)}MB)` +
      `\n    per-event (est): time=${(total_ms / estimated_events).toFixed(4)}ms | heap=${((mem_done.heapUsed - mem_before.heapUsed) / estimated_events / 1024).toFixed(2)}KB | heap_gc=${((mem_gc.heapUsed - mem_before.heapUsed) / estimated_events / 1024).toFixed(2)}KB` +
      `\n    rss: before=${mb(mem_before.rss)}MB → done=${mb(mem_done.rss)}MB → gc=${mb(mem_gc.rss)}MB` +
      `\n    history: a=${bus_a.event_history.size} b=${bus_b.event_history.size} c=${bus_c.event_history.size}` +
      `\n    handlers: a=${bus_a.handlers.size} b=${bus_b.handlers.size} c=${bus_c.handlers.size}` +
      `\n    instances: ${EventBus._all_instances.size}`
  )

  // All iterations processed
  assert.equal(parent_handled_a, total_iterations)
  assert.equal(parent_handled_b, total_iterations)

  // History bounded by max_history_size
  assert.ok(bus_a.event_history.size <= history_limit, `bus_a history ${bus_a.event_history.size} > ${history_limit}`)
  assert.ok(bus_b.event_history.size <= history_limit, `bus_b history ${bus_b.event_history.size} > ${history_limit}`)
  assert.ok(bus_c.event_history.size <= history_limit, `bus_c history ${bus_c.event_history.size} > ${history_limit}`)

  // Ephemeral handlers all cleaned up
  assert.equal(bus_a.handlers.size, 0, 'All ephemeral handlers removed from bus_a')

  // Memory should not grow unbounded — allow 50MB over baseline
  assert.ok(mem_delta_mb < 50, `Memory grew ${mem_delta_mb.toFixed(1)}MB over baseline (limit 50MB)`)

  bus_a.destroy()
  bus_b.destroy()
  bus_c.destroy()

  assert.equal(EventBus._all_instances.size, 0, 'All buses destroyed')
})
