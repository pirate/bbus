import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerAbortedError, EventHandlerTimeoutError } from '../src/index.js'
import { LockManager } from '../src/lock_manager.js'

const TimeoutEvent = BaseEvent.extend('TimeoutEvent', {})

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

test('handler timeout marks EventResult as error', async () => {
  const bus = new EventBus('TimeoutBus')

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    return 'slow'
  })

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerTimeoutError)
})

test('handler completes within timeout', async () => {
  const bus = new EventBus('TimeoutOkBus')

  bus.on(TimeoutEvent, async () => {
    await delay(5)
    return 'fast'
  })

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'fast')
})

test('event handler errors expose event_result, cause, and timeout metadata', async () => {
  const bus = new EventBus('ErrorMetadataBus')

  const ParentCancelEvent = BaseEvent.extend('ParentCancelEvent', {})
  const PendingChildEvent = BaseEvent.extend('PendingChildEvent', {})
  const ParentAbortEvent = BaseEvent.extend('ParentAbortEvent', {})
  const AbortChildEvent = BaseEvent.extend('AbortChildEvent', {})

  bus.on(TimeoutEvent, async () => {
    await delay(40)
    return 'slow'
  })

  bus.on(PendingChildEvent, async () => {
    await delay(5)
    return 'pending_child'
  })

  let pending_child: BaseEvent | null = null
  bus.on(ParentCancelEvent, async (event) => {
    pending_child = event.bus?.emit(PendingChildEvent({ event_timeout: 0.5 })) ?? null
    await delay(80)
  })

  bus.on(AbortChildEvent, async () => {
    await delay(120)
    return 'abort_child'
  })

  let aborted_child: BaseEvent | null = null
  bus.on(ParentAbortEvent, async (event) => {
    aborted_child = event.bus?.emit(AbortChildEvent({ event_timeout: 0.5 })) ?? null
    await aborted_child?.done()
  })

  const timeout_event = bus.dispatch(TimeoutEvent({ event_timeout: 0.02 }))
  await timeout_event.done()

  const timeout_result = Array.from(timeout_event.event_results.values())[0]
  const timeout_error = timeout_result.error as EventHandlerTimeoutError
  assert.ok(timeout_error.cause instanceof Error)
  assert.equal(timeout_error.cause.name, 'TimeoutError')
  assert.equal(timeout_error.event_result, timeout_result)
  assert.equal(timeout_error.timeout_seconds, timeout_event.event_timeout)
  assert.equal(timeout_error.event.event_id, timeout_event.event_id)
  assert.equal(timeout_error.event_type, timeout_event.event_type)
  assert.equal(timeout_error.handler_name, timeout_result.handler_name)
  assert.equal(timeout_error.handler_id, timeout_result.handler_id)
  assert.equal(timeout_error.event_timeout, timeout_event.event_timeout)

  const cancel_parent = bus.dispatch(ParentCancelEvent({ event_timeout: 0.02 }))
  await cancel_parent.done()
  await bus.waitUntilIdle()

  assert.ok(pending_child, 'pending_child should have been emitted')
  const pending_result = Array.from(pending_child!.event_results.values())[0]
  const cancelled_error = pending_result.error as EventHandlerCancelledError
  const cancel_parent_result = Array.from(cancel_parent.event_results.values())[0]
  const cancel_parent_error = cancel_parent_result.error as EventHandlerTimeoutError
  assert.equal(cancelled_error.cause, cancel_parent_error)
  assert.equal(cancelled_error.event_result, pending_result)
  assert.equal(cancelled_error.event.event_id, pending_child!.event_id)
  assert.equal(cancelled_error.timeout_seconds, pending_child!.event_timeout)
  assert.equal(cancelled_error.event_type, pending_child!.event_type)
  assert.equal(cancelled_error.handler_name, pending_result.handler_name)
  assert.equal(cancelled_error.handler_id, pending_result.handler_id)

  const abort_parent = bus.dispatch(ParentAbortEvent({ event_timeout: 0.05 }))
  await abort_parent.done()
  await bus.waitUntilIdle()

  assert.ok(aborted_child, 'aborted_child should have been emitted')
  const aborted_result = Array.from(aborted_child!.event_results.values())[0]
  const aborted_error = aborted_result.error as EventHandlerAbortedError
  const abort_parent_result = Array.from(abort_parent.event_results.values())[0]
  const abort_parent_error = abort_parent_result.error as EventHandlerTimeoutError
  assert.equal(aborted_error.cause, abort_parent_error)
  assert.equal(aborted_error.event_result, aborted_result)
  assert.equal(aborted_error.event.event_id, aborted_child!.event_id)
  assert.equal(aborted_error.timeout_seconds, aborted_child!.event_timeout)
  assert.equal(aborted_error.event_type, aborted_child!.event_type)
  assert.equal(aborted_error.handler_name, aborted_result.handler_name)
  assert.equal(aborted_error.handler_id, aborted_result.handler_id)
})

test('handler timeouts fire across concurrency modes', async () => {
  const modes = ['global-serial', 'bus-serial', 'parallel'] as const

  for (const event_mode of modes) {
    for (const handler_mode of modes) {
      const bus = new EventBus(`Timeout-${event_mode}-${handler_mode}`, {
        event_concurrency: event_mode,
        handler_concurrency: handler_mode,
      })

      bus.on(TimeoutEvent, async () => {
        await delay(50)
        return 'slow'
      })

      const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }))
      await event.done()

      const result = Array.from(event.event_results.values())[0]
      assert.equal(result.status, 'error', `Expected timeout error for event=${event_mode} handler=${handler_mode}`)
      assert.ok(
        result.error instanceof EventHandlerTimeoutError,
        `Expected EventHandlerTimeoutError for event=${event_mode} handler=${handler_mode}`
      )

      await bus.waitUntilIdle()
    }
  }
})

test('timeout still marks event failed when other handlers finish', async () => {
  const bus = new EventBus('TimeoutParallelHandlers', {
    event_concurrency: 'parallel',
    handler_concurrency: 'parallel',
  })

  const results: string[] = []

  bus.on(TimeoutEvent, async () => {
    await delay(1)
    results.push('fast')
    return 'fast'
  })

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    results.push('slow')
    return 'slow'
  })

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done()

  const statuses = Array.from(event.event_results.values()).map((result) => result.status)
  assert.ok(statuses.includes('completed'))
  assert.ok(statuses.includes('error'))
  assert.equal(event.event_status, 'completed')
  assert.ok(event.event_errors.length > 0)
  assert.ok(results.includes('fast'))
})

test('slow event warning fires when event exceeds event_slow_timeout', async () => {
  const bus = new EventBus('SlowEventWarnBus', {
    event_slow_timeout: 0.01,
    event_handler_slow_timeout: null,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event processing')),
    'Expected slow event warning'
  )
})

test('slow handler warning fires when handler runs long', async () => {
  const bus = new EventBus('SlowHandlerWarnBus', {
    event_handler_slow_timeout: 0.01,
    event_slow_timeout: null,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event handler')),
    'Expected slow handler warning'
  )
})

test('slow handler and slow event warnings can both fire', async () => {
  const bus = new EventBus('SlowComboWarnBus', {
    event_handler_slow_timeout: 0.01,
    event_slow_timeout: 0.01,
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
    bus.on(TimeoutEvent, async () => {
      await delay(25)
      return 'ok'
    })

    const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }))
    await event.done()
  } finally {
    console.warn = original_warn
  }

  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event handler')),
    'Expected slow handler warning'
  )
  assert.ok(
    warnings.some((message) => message.toLowerCase().includes('slow event processing')),
    'Expected slow event warning'
  )
})

test('event-level concurrency overrides do not bypass timeouts', async () => {
  const bus = new EventBus('TimeoutEventOverrideBus', {
    event_concurrency: 'global-serial',
    handler_concurrency: 'global-serial',
  })

  bus.on(TimeoutEvent, async () => {
    await delay(50)
    return 'slow'
  })

  const event = bus.dispatch(
    TimeoutEvent({
      event_timeout: 0.01,
      event_concurrency: 'parallel',
      handler_concurrency: 'parallel',
    })
  )
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerTimeoutError)
})

test('handler-level concurrency overrides do not bypass timeouts', async () => {
  const bus = new EventBus('TimeoutHandlerOverrideBus', {
    event_concurrency: 'parallel',
    handler_concurrency: 'global-serial',
  })

  const order: string[] = []

  bus.on(
    TimeoutEvent,
    async () => {
      order.push('slow_start')
      await delay(50)
      order.push('slow_end')
      return 'slow'
    },
    { handler_concurrency: 'bus-serial' }
  )

  bus.on(
    TimeoutEvent,
    async () => {
      order.push('fast_start')
      await delay(1)
      order.push('fast_end')
      return 'fast'
    },
    { handler_concurrency: 'parallel' }
  )

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done()

  const statuses = Array.from(event.event_results.values()).map((result) => result.status)
  assert.ok(statuses.includes('error'))
  assert.ok(statuses.includes('completed'))
  assert.ok(order.includes('fast_start'))
})

test('forwarded event timeouts apply across buses', async () => {
  const bus_a = new EventBus('TimeoutForwardA', { event_concurrency: 'bus-serial' })
  const bus_b = new EventBus('TimeoutForwardB', { event_concurrency: 'bus-serial' })

  bus_a.on(TimeoutEvent, async (event) => {
    bus_b.dispatch(event)
  })

  bus_b.on(TimeoutEvent, async () => {
    await delay(50)
    return 'slow'
  })

  const event = bus_a.dispatch(TimeoutEvent({ event_timeout: 0.01 }))
  await event.done()

  const results = Array.from(event.event_results.values())
  const bus_b_result = results.find((result) => result.eventbus_name === 'TimeoutForwardB')
  assert.ok(bus_b_result)
  assert.equal(bus_b_result?.status, 'error')
  assert.ok(bus_b_result?.error instanceof EventHandlerTimeoutError)
})

test('queue-jump awaited child timeouts still fire across buses', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutParentEvent', {})
  const ChildEvent = BaseEvent.extend('TimeoutChildEvent', {})

  const bus_a = new EventBus('TimeoutQueueJumpA', { event_concurrency: 'global-serial' })
  const bus_b = new EventBus('TimeoutQueueJumpB', { event_concurrency: 'global-serial' })

  let child_ref: InstanceType<typeof ChildEvent> | null = null

  bus_b.on(ChildEvent, async () => {
    await delay(50)
    return 'slow'
  })

  bus_a.on(ParentEvent, async (event) => {
    // Use scoped bus emit to set parent tracking (event_parent_id, event_emitted_by_handler_id),
    // then also dispatch on bus_b for cross-bus handler execution.
    // Without parent tracking, processEventImmediately can't detect the queue-jump context
    // and falls back to waitForCompletion(), which deadlocks with global-serial.
    const child = event.bus?.emit(ChildEvent({ event_timeout: 0.01 }))!
    bus_b.dispatch(child)
    child_ref = child
    await child.done()
  })

  const parent = bus_a.dispatch(ParentEvent({ event_timeout: 0.5 }))
  await parent.done()

  assert.ok(child_ref)
  const child_results = Array.from(child_ref!.event_results.values())
  const timeout_result = child_results.find((result) => result.error instanceof EventHandlerTimeoutError)
  assert.ok(timeout_result)
})

const STEP1_HANDLER_MODES = ['bus-serial', 'global-serial'] as const
type Step1HandlerMode = (typeof STEP1_HANDLER_MODES)[number]

const getHandlerSemaphore = (bus: EventBus, mode: Step1HandlerMode) =>
  mode === 'global-serial' ? LockManager.global_handler_semaphore : bus.locks.bus_handler_semaphore

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: timeout during awaited child.done() does not leak handler semaphore lock [${handler_mode}]`, async () => {
    const ParentEvent = BaseEvent.extend(`TimeoutLeakParent-${handler_mode}`, {})
    const ChildEvent = BaseEvent.extend(`TimeoutLeakChild-${handler_mode}`, {})

    const bus = new EventBus(`TimeoutLeakBus-${handler_mode}`, {
      event_concurrency: 'bus-serial',
      handler_concurrency: handler_mode,
    })
    const semaphore = getHandlerSemaphore(bus, handler_mode)
    const baseline_in_use = semaphore.in_use
    const original_acquire = semaphore.acquire.bind(semaphore)
    let acquire_count = 0

    semaphore.acquire = async () => {
      acquire_count += 1
      // Third acquire is the parent reclaim in processEventImmediately finally.
      // Delay it so the parent handler timeout can fire in the middle.
      if (acquire_count === 3) {
        await delay(30)
      }
      await original_acquire()
    }

    try {
      bus.on(ChildEvent, async () => {
        await delay(1)
        return 'child_done'
      })

      bus.on(ParentEvent, async (event) => {
        const child = event.bus?.emit(ChildEvent({ event_timeout: 0.2 }))!
        await child.done()
        return 'parent_done'
      })

      const parent = bus.dispatch(ParentEvent({ event_timeout: 0.01 }))
      await parent.done()
      await bus.waitUntilIdle()

      const parent_result = Array.from(parent.event_results.values())[0]
      assert.equal(parent_result.status, 'error')
      assert.ok(parent_result.error instanceof EventHandlerTimeoutError)
      assert.equal(
        semaphore.in_use,
        baseline_in_use,
        `handler semaphore leaked lock (mode=${handler_mode}, in_use=${semaphore.in_use}, baseline=${baseline_in_use}, acquires=${acquire_count})`
      )
    } finally {
      semaphore.acquire = original_acquire
      while (semaphore.in_use > baseline_in_use) {
        semaphore.release()
      }
    }
  })
}

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: parent timeout while reacquire waits behind third serial handler is lock-safe [${handler_mode}]`, async () => {
    const ParentEvent = BaseEvent.extend(`TimeoutContentionParent-${handler_mode}`, {})
    const ChildEvent = BaseEvent.extend(`TimeoutContentionChild-${handler_mode}`, {})

    const bus = new EventBus(`TimeoutContentionBus-${handler_mode}`, {
      event_concurrency: 'bus-serial',
      handler_concurrency: handler_mode,
    })
    const semaphore = getHandlerSemaphore(bus, handler_mode)
    const baseline_in_use = semaphore.in_use

    bus.on(ChildEvent, async () => {
      await delay(2)
      return 'child_done'
    })

    bus.on(ParentEvent, async (event) => {
      const child = event.bus?.emit(ChildEvent({ event_timeout: 0.2, handler_concurrency: 'parallel' }))!
      await child.done()
      return 'parent_main'
    })

    // This handler queues behind parent_main, then holds the serial semaphore
    // while parent_main is trying to reclaim after child.done() completes.
    bus.on(ParentEvent, async () => {
      await delay(40)
      return 'parent_blocker'
    })

    const parent = bus.dispatch(ParentEvent({ event_timeout: 0.01 }))
    await parent.done()
    await bus.waitUntilIdle()

    const parent_results = Array.from(parent.event_results.values())
    const timeout_results = parent_results.filter((result) => result.error instanceof EventHandlerTimeoutError)
    assert.ok(timeout_results.length >= 1, `expected at least one timeout result in ${handler_mode}`)
    assert.equal(semaphore.in_use, baseline_in_use)
  })
}

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: next event still runs on same bus after timeout queue-jump path [${handler_mode}]`, async () => {
    const ParentEvent = BaseEvent.extend(`TimeoutFollowupParent-${handler_mode}`, {})
    const ChildEvent = BaseEvent.extend(`TimeoutFollowupChild-${handler_mode}`, {})
    const FollowupEvent = BaseEvent.extend(`TimeoutFollowupTail-${handler_mode}`, {})

    const bus = new EventBus(`TimeoutFollowupBus-${handler_mode}`, {
      event_concurrency: 'bus-serial',
      handler_concurrency: handler_mode,
    })
    const semaphore = getHandlerSemaphore(bus, handler_mode)
    const baseline_in_use = semaphore.in_use
    const original_acquire = semaphore.acquire.bind(semaphore)
    let acquire_count = 0
    semaphore.acquire = async () => {
      acquire_count += 1
      if (acquire_count === 3) {
        await delay(30)
      }
      await original_acquire()
    }

    let followup_runs = 0

    try {
      bus.on(ChildEvent, async () => {
        await delay(1)
      })

      bus.on(ParentEvent, async (event) => {
        const child = event.bus?.emit(ChildEvent({ event_timeout: 0.2 }))!
        await child.done()
      })

      bus.on(FollowupEvent, async () => {
        followup_runs += 1
        return 'followup_done'
      })

      const parent = bus.dispatch(ParentEvent({ event_timeout: 0.01 }))
      await parent.done()
      await bus.waitUntilIdle()

      const followup = bus.dispatch(FollowupEvent({ event_timeout: 0.05 }))
      const followup_completed = await Promise.race([followup.done().then(() => true), delay(100).then(() => false)])

      assert.equal(
        followup_completed,
        true,
        `follow-up event stalled after timeout queue-jump path (mode=${handler_mode}, in_use=${semaphore.in_use}, acquires=${acquire_count})`
      )
      assert.equal(followup_runs, 1)
      assert.equal(semaphore.in_use, baseline_in_use)
    } finally {
      semaphore.acquire = original_acquire
      while (semaphore.in_use > baseline_in_use) {
        semaphore.release()
      }
    }
  })
}

for (const handler_mode of STEP1_HANDLER_MODES) {
  test(`regression: nested queue-jump with timeout cancellation remains lock-safe [${handler_mode}]`, async () => {
    const ParentEvent = BaseEvent.extend(`NestedPermitParent-${handler_mode}`, {})
    const ChildEvent = BaseEvent.extend(`NestedPermitChild-${handler_mode}`, {})
    const GrandchildEvent = BaseEvent.extend(`NestedPermitGrandchild-${handler_mode}`, {})
    const QueuedSiblingEvent = BaseEvent.extend(`NestedPermitQueuedSibling-${handler_mode}`, {})
    const TailEvent = BaseEvent.extend(`NestedPermitTail-${handler_mode}`, {})

    const bus = new EventBus(`NestedPermitBus-${handler_mode}`, {
      event_concurrency: 'bus-serial',
      handler_concurrency: handler_mode,
    })
    const semaphore = getHandlerSemaphore(bus, handler_mode)
    const baseline_in_use = semaphore.in_use

    let queued_sibling_runs = 0
    let tail_runs = 0
    let queued_sibling_ref: InstanceType<typeof QueuedSiblingEvent> | null = null

    bus.on(GrandchildEvent, async () => {
      await delay(1)
      return 'grandchild_done'
    })

    bus.on(ChildEvent, async (event) => {
      const grandchild = event.bus?.emit(GrandchildEvent({ event_timeout: 0.2 }))!
      await grandchild.done()
      await delay(40)
      return 'child_done'
    })

    bus.on(QueuedSiblingEvent, async () => {
      queued_sibling_runs += 1
      return 'queued_sibling_done'
    })

    bus.on(ParentEvent, async (event) => {
      queued_sibling_ref = event.bus?.emit(QueuedSiblingEvent({ event_timeout: 0.2 }))!
      const child = event.bus?.emit(ChildEvent({ event_timeout: 0.02 }))!
      await child.done()
      await delay(40)
    })

    bus.on(TailEvent, async () => {
      tail_runs += 1
      return 'tail_done'
    })

    const parent = bus.dispatch(ParentEvent({ event_timeout: 0.03 }))
    await parent.done()
    await bus.waitUntilIdle()

    const parent_result = Array.from(parent.event_results.values())[0]
    assert.equal(parent_result.status, 'error')
    assert.ok(parent_result.error instanceof EventHandlerTimeoutError)

    assert.ok(queued_sibling_ref)
    assert.equal(queued_sibling_runs, 0)
    const queued_sibling_results = Array.from(queued_sibling_ref!.event_results.values())
    assert.ok(queued_sibling_results.some((result) => result.error instanceof EventHandlerCancelledError))

    assert.equal(semaphore.in_use, baseline_in_use)

    const tail = bus.dispatch(TailEvent({ event_timeout: 0.05 }))
    const tail_completed = await Promise.race([tail.done().then(() => true), delay(100).then(() => false)])
    assert.equal(tail_completed, true)
    assert.equal(tail_runs, 1)
    assert.equal(semaphore.in_use, baseline_in_use)
  })
}

test('parent timeout cancels pending child handler results under serial handler semaphore', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutCancelParentEvent', {})
  const ChildEvent = BaseEvent.extend('TimeoutCancelChildEvent', {})

  const bus = new EventBus('TimeoutCancelBus', {
    event_concurrency: 'bus-serial',
    handler_concurrency: 'bus-serial',
  })

  let child_runs = 0

  bus.on(ChildEvent, async () => {
    child_runs += 1
    await delay(30)
    return 'first'
  })

  bus.on(ChildEvent, async () => {
    child_runs += 1
    await delay(10)
    return 'second'
  })

  bus.on(ParentEvent, async (event) => {
    event.bus?.emit(ChildEvent({ event_timeout: 0.2 }))
    await delay(50)
  })

  const parent = bus.dispatch(ParentEvent({ event_timeout: 0.01 }))
  await parent.done()
  await bus.waitUntilIdle()

  const child = parent.event_children[0]
  assert.ok(child)

  assert.equal(child_runs, 0)

  const cancelled_results = Array.from(child.event_results.values()).filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.ok(cancelled_results.length > 0)
})

test('event_timeout null falls back to bus default', async () => {
  const bus = new EventBus('TimeoutDefaultBus', { event_timeout: 0.01 })

  bus.on(TimeoutEvent, async (_event: BaseEvent) => {
    await delay(50)
    return 'slow'
  })

  const event = bus.dispatch(TimeoutEvent({ event_timeout: null }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof EventHandlerTimeoutError)
})

test('bus default null disables timeouts when event_timeout is null', async () => {
  const bus = new EventBus('TimeoutDisabledBus', { event_timeout: null })

  bus.on(TimeoutEvent, async () => {
    await delay(20)
    return 'ok'
  })

  const event = bus.dispatch(TimeoutEvent({ event_timeout: null }))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'ok')
})

test('multi-level timeout cascade with mixed cancellations', async () => {
  const TopEvent = BaseEvent.extend('TimeoutCascadeTop', {})
  const QueuedChildEvent = BaseEvent.extend('TimeoutCascadeQueuedChild', {})
  const AwaitedChildEvent = BaseEvent.extend('TimeoutCascadeAwaitedChild', {})
  const ImmediateGrandchildEvent = BaseEvent.extend('TimeoutCascadeImmediateGrandchild', {})
  const QueuedGrandchildEvent = BaseEvent.extend('TimeoutCascadeQueuedGrandchild', {})

  const bus = new EventBus('TimeoutCascadeBus', {
    event_concurrency: 'bus-serial',
    handler_concurrency: 'bus-serial',
  })

  let queued_child: InstanceType<typeof QueuedChildEvent> | null = null
  let awaited_child: InstanceType<typeof AwaitedChildEvent> | null = null
  let immediate_grandchild: InstanceType<typeof ImmediateGrandchildEvent> | null = null
  let queued_grandchild: InstanceType<typeof QueuedGrandchildEvent> | null = null

  let queued_child_runs = 0
  let immediate_grandchild_runs = 0
  let queued_grandchild_runs = 0

  const queued_child_fast = async () => {
    queued_child_runs += 1
    await delay(5)
    return 'queued_fast'
  }

  const queued_child_slow = async () => {
    queued_child_runs += 1
    await delay(50)
    return 'queued_slow'
  }

  const awaited_child_fast = async () => {
    await delay(5)
    return 'awaited_fast'
  }

  const awaited_child_slow = async (event: BaseEvent) => {
    queued_grandchild = event.bus?.emit(QueuedGrandchildEvent({ event_timeout: 0.2 }))!
    immediate_grandchild = event.bus?.emit(ImmediateGrandchildEvent({ event_timeout: 0.2 }))!
    await immediate_grandchild.done()
    await delay(100)
    return 'awaited_slow'
  }

  const immediate_grandchild_slow = async () => {
    immediate_grandchild_runs += 1
    await delay(50)
    return 'immediate_grandchild_slow'
  }

  const immediate_grandchild_fast = async () => {
    immediate_grandchild_runs += 1
    await delay(10)
    return 'immediate_grandchild_fast'
  }

  const queued_grandchild_slow = async () => {
    queued_grandchild_runs += 1
    await delay(50)
    return 'queued_grandchild_slow'
  }

  const queued_grandchild_fast = async () => {
    queued_grandchild_runs += 1
    await delay(10)
    return 'queued_grandchild_fast'
  }

  bus.on(QueuedChildEvent, queued_child_fast)
  bus.on(QueuedChildEvent, queued_child_slow)
  bus.on(AwaitedChildEvent, awaited_child_fast)
  bus.on(AwaitedChildEvent, awaited_child_slow)
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_slow)
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_fast)
  bus.on(QueuedGrandchildEvent, queued_grandchild_slow)
  bus.on(QueuedGrandchildEvent, queued_grandchild_fast)

  bus.on(TopEvent, async (event) => {
    queued_child = event.bus?.emit(QueuedChildEvent({ event_timeout: 0.2 }))!
    awaited_child = event.bus?.emit(AwaitedChildEvent({ event_timeout: 0.03 }))!
    await awaited_child.done()
    await delay(80)
  })

  const top = bus.dispatch(TopEvent({ event_timeout: 0.04 }))
  await top.done()
  await bus.waitUntilIdle()

  const top_result = Array.from(top.event_results.values())[0]
  assert.equal(top_result.status, 'error')
  assert.ok(top_result.error instanceof EventHandlerTimeoutError)

  assert.ok(queued_child)
  const queued_results = Array.from(queued_child!.event_results.values())
  assert.equal(queued_child_runs, 0)
  assert.ok(queued_results.length >= 2)
  for (const result of queued_results) {
    assert.equal(result.status, 'error')
    assert.ok(result.error instanceof EventHandlerCancelledError)
    assert.ok((result.error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError)
  }

  assert.ok(awaited_child)
  const awaited_results = Array.from(awaited_child!.event_results.values())
  const awaited_completed = awaited_results.filter((result) => result.status === 'completed')
  const awaited_timeouts = awaited_results.filter((result) => result.error instanceof EventHandlerTimeoutError)
  assert.equal(awaited_completed.length, 1)
  assert.equal(awaited_timeouts.length, 1)

  assert.ok(immediate_grandchild)
  const immediate_results = Array.from(immediate_grandchild!.event_results.values())
  // With bus-serial handler concurrency (no longer bypassed during queue-jump),
  // only the first grandchild handler starts before the awaited child's 30ms timeout fires.
  // The second handler is still pending (waiting for semaphore) → cancelled.
  // The first handler was already started → aborted (EventHandlerAbortedError).
  assert.equal(immediate_grandchild_runs, 1)
  const immediate_aborted = immediate_results.filter((result) => result.error instanceof EventHandlerAbortedError)
  assert.equal(immediate_aborted.length, 1)
  const immediate_cancelled = immediate_results.filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.equal(immediate_cancelled.length, 1)

  assert.ok(queued_grandchild)
  const queued_grandchild_results = Array.from(queued_grandchild!.event_results.values())
  assert.equal(queued_grandchild_runs, 0)
  const queued_cancelled = queued_grandchild_results.filter((result) => result.error instanceof EventHandlerCancelledError)
  assert.ok(queued_cancelled.length >= 2)
})

// =============================================================================
// Three-level timeout cascade (mirrors Python test_handler_timeout.py)
//
// This test creates a deep event hierarchy:
//   TopEvent (250ms timeout)
//     ├── ChildEvent (80ms timeout) — awaited by top_handler_main
//     │     ├── GrandchildEvent (35ms timeout) — awaited by child_handler
//     │     │     └── 5 handlers (parallel): 3 slow (timeout), 2 fast (complete)
//     │     └── QueuedGrandchildEvent — emitted but NOT awaited, stays in queue
//     │           └── 1 handler: never runs, CANCELLED when child_handler times out
//     └── SiblingEvent — emitted but NOT awaited, stays in queue
//           └── 1 handler: never runs, CANCELLED when top_handler_main times out
//
// KEY MECHANIC: When a child event is awaited via event.done() inside a handler,
// it triggers "queue-jumping" via processEventImmediately → runImmediatelyAcrossBuses.
// Queue-jumped events use yield-and-reacquire: the parent handler's semaphore is
// temporarily released so child handlers can acquire it normally. This means
// child handlers run SERIALLY on a bus-serial bus (respecting concurrency limits).
// Non-awaited child events stay in the pending_event_queue and are blocked by
// immediate_processing_stack_depth > 0 (runloop is paused during queue-jump).
//
// TIMEOUT BEHAVIOR: Each handler gets its OWN timeout window starting from when
// that handler begins execution — NOT from when the event was dispatched.
// With serial handlers, each timeout starts when the handler acquires the semaphore.
//
// CANCELLATION CASCADE: When a handler times out, bus.cancelPendingDescendants()
// walks the event's children tree and marks any "pending" handler results as
// EventHandlerCancelledError. Only "pending" results are cancelled — handlers
// that already started ("started" status) continue running in the background.
// =============================================================================

test('three-level timeout cascade with per-level timeouts and cascading cancellation', async () => {
  const TopEvent = BaseEvent.extend('Cascade3LTop', {})
  const ChildEvent = BaseEvent.extend('Cascade3LChild', {})
  const GrandchildEvent = BaseEvent.extend('Cascade3LGrandchild', {})
  const QueuedGrandchildEvent = BaseEvent.extend('Cascade3LQueuedGC', {})
  const SiblingEvent = BaseEvent.extend('Cascade3LSibling', {})

  const bus = new EventBus('Cascade3LevelBus', {
    event_concurrency: 'bus-serial',
    handler_concurrency: 'bus-serial',
  })

  const execution_log: string[] = []
  let child_ref: InstanceType<typeof ChildEvent> | null = null
  let grandchild_ref: InstanceType<typeof GrandchildEvent> | null = null
  let queued_grandchild_ref: InstanceType<typeof QueuedGrandchildEvent> | null = null
  let sibling_ref: InstanceType<typeof SiblingEvent> | null = null

  // ── GrandchildEvent handlers ──────────────────────────────────────────
  // These run SERIALLY because queue-jumped events respect the bus-serial
  // handler semaphore (yield-and-reacquire). Each handler gets its own 35ms
  // timeout window starting from when that handler acquires the semaphore.
  //
  // Serial order: a(35ms timeout) → b(sync) → c(35ms timeout) → d(10ms) → e(35ms timeout)
  // Total time for all 5: ~35+0+35+10+35 = ~115ms (within child's 150ms timeout)

  const gc_handler_a = async () => {
    execution_log.push('gc_a_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_a_end') // should never reach here before assertions
    return 'gc_a_done'
  }

  const gc_handler_b = () => {
    execution_log.push('gc_b_complete')
    return 'gc_b_done'
  }

  const gc_handler_c = async () => {
    execution_log.push('gc_c_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_c_end') // should never reach here before assertions
    return 'gc_c_done'
  }

  const gc_handler_d = async () => {
    execution_log.push('gc_d_start')
    await delay(10) // fast enough to complete within 35ms
    execution_log.push('gc_d_complete')
    return 'gc_d_done'
  }

  const gc_handler_e = async () => {
    execution_log.push('gc_e_start')
    await delay(500) // will be interrupted by 35ms timeout (500ms > total test time)
    execution_log.push('gc_e_end') // should never reach here before assertions
    return 'gc_e_done'
  }

  // ── QueuedGrandchildEvent handler ─────────────────────────────────────
  // This event is emitted by child_handler but NOT awaited, so it sits in
  // pending_event_queue. When child_handler times out at 80ms,
  // bus.cancelPendingDescendants walks ChildEvent.event_children and finds
  // this event still pending → its handler results are marked as cancelled.
  const queued_gc_handler = () => {
    execution_log.push('queued_gc_start') // should never reach here
    return 'queued_gc_done'
  }

  // ── ChildEvent handler ────────────────────────────────────────────────
  // Emits GrandchildEvent (awaited → queue-jump, ~35ms to complete)
  // Emits QueuedGrandchildEvent (NOT awaited → stays in queue)
  // After grandchild completes, sleeps 300ms → times out at 80ms total
  const child_handler = async (event: InstanceType<typeof ChildEvent>) => {
    execution_log.push('child_start')
    grandchild_ref = event.bus?.emit(GrandchildEvent({ event_timeout: 0.035 }))!
    queued_grandchild_ref = event.bus?.emit(QueuedGrandchildEvent({ event_timeout: 0.5 }))!
    // Queue-jump: processes GrandchildEvent immediately via yield-and-reacquire.
    // All 5 GC handlers run serially. Completes in ~115ms (within 150ms child timeout).
    await grandchild_ref.done()
    execution_log.push('child_after_grandchild')
    await delay(300) // will be interrupted: child started at ~t=0, timeout at 150ms
    execution_log.push('child_end') // should never reach here
    return 'child_done'
  }

  // ── SiblingEvent handler ──────────────────────────────────────────────
  // This event is emitted by top_handler_main but NOT awaited. Stays in
  // pending_event_queue until top_handler_main times out at 250ms →
  // cancelled by bus.cancelPendingDescendants.
  const sibling_handler = () => {
    execution_log.push('sibling_start') // should never reach here
    return 'sibling_done'
  }

  // ── TopEvent handlers ─────────────────────────────────────────────────
  // These run SERIALLY (via bus.locks.bus_handler_semaphore) because TopEvent is
  // processed by the normal runloop (not queue-jumped). top_handler_fast
  // goes first, completes quickly, then top_handler_main starts.

  const top_handler_fast = async () => {
    execution_log.push('top_fast_start')
    await delay(2)
    execution_log.push('top_fast_complete')
    return 'top_fast_done'
  }

  const top_handler_main = async (event: InstanceType<typeof TopEvent>) => {
    execution_log.push('top_main_start')
    child_ref = event.bus?.emit(ChildEvent({ event_timeout: 0.15 }))!
    sibling_ref = event.bus?.emit(SiblingEvent({ event_timeout: 0.5 }))!
    // Queue-jump: processes ChildEvent immediately (which in turn queue-jumps
    // GrandchildEvent). This entire subtree resolves in ~80ms (child timeout).
    await child_ref.done()
    execution_log.push('top_main_after_child')
    await delay(300) // will be interrupted: top_handler_main started at ~t=2, timeout at 250ms
    execution_log.push('top_main_end') // should never reach here
    return 'top_main_done'
  }

  // Register handlers (registration order = execution order for serial)
  bus.on(TopEvent, top_handler_fast)
  bus.on(TopEvent, top_handler_main)
  bus.on(ChildEvent, child_handler)
  bus.on(GrandchildEvent, gc_handler_a)
  bus.on(GrandchildEvent, gc_handler_b)
  bus.on(GrandchildEvent, gc_handler_c)
  bus.on(GrandchildEvent, gc_handler_d)
  bus.on(GrandchildEvent, gc_handler_e)
  bus.on(QueuedGrandchildEvent, queued_gc_handler)
  bus.on(SiblingEvent, sibling_handler)

  // ── Dispatch and wait ─────────────────────────────────────────────────
  const top = bus.dispatch(TopEvent({ event_timeout: 0.25 }))
  await top.done()
  await bus.waitUntilIdle()

  // ═══════════════════════════════════════════════════════════════════════
  // ASSERTIONS
  // ═══════════════════════════════════════════════════════════════════════

  // ── TopEvent: 2 handler results (1 completed, 1 timed out) ──────────
  assert.equal(top.event_status, 'completed')
  assert.ok(top.event_errors.length >= 1, 'TopEvent should have at least 1 error')

  const top_results = Array.from(top.event_results.values())
  assert.equal(top_results.length, 2, 'TopEvent should have 2 handler results')

  const top_fast_result = top_results.find((r) => r.handler_name === 'top_handler_fast')
  assert.ok(top_fast_result, 'top_handler_fast result should exist')
  assert.equal(top_fast_result!.status, 'completed')
  assert.equal(top_fast_result!.result, 'top_fast_done')

  const top_main_result = top_results.find((r) => r.handler_name === 'top_handler_main')
  assert.ok(top_main_result, 'top_handler_main result should exist')
  assert.equal(top_main_result!.status, 'error')
  assert.ok(top_main_result!.error instanceof EventHandlerTimeoutError, 'top_handler_main should have timed out')

  // ── ChildEvent: 1 handler result (timed out at 150ms) ────────────────
  assert.ok(child_ref, 'ChildEvent should have been emitted')
  assert.equal(child_ref!.event_status, 'completed')

  const child_results = Array.from(child_ref!.event_results.values())
  assert.equal(child_results.length, 1, 'ChildEvent should have 1 handler result')
  assert.equal(child_results[0].handler_name, 'child_handler')
  assert.equal(child_results[0].status, 'error')
  assert.ok(child_results[0].error instanceof EventHandlerTimeoutError, 'child_handler should have timed out')

  // ── GrandchildEvent: 5 handler results (2 completed, 3 timed out) ──
  assert.ok(grandchild_ref, 'GrandchildEvent should have been emitted')
  assert.equal(grandchild_ref!.event_status, 'completed')

  const gc_results = Array.from(grandchild_ref!.event_results.values())
  assert.equal(gc_results.length, 5, 'GrandchildEvent should have 5 handler results')

  // Handlers a, c, e: slow → individually timed out
  for (const name of ['gc_handler_a', 'gc_handler_c', 'gc_handler_e']) {
    const result = gc_results.find((r) => r.handler_name === name)
    assert.ok(result, `${name} result should exist`)
    assert.equal(result!.status, 'error', `${name} should have status error`)
    assert.ok(result!.error instanceof EventHandlerTimeoutError, `${name} should be EventHandlerTimeoutError`)
  }

  // Handlers b, d: fast → completed successfully
  const gc_b_result = gc_results.find((r) => r.handler_name === 'gc_handler_b')
  assert.ok(gc_b_result, 'gc_handler_b result should exist')
  assert.equal(gc_b_result!.status, 'completed')
  assert.equal(gc_b_result!.result, 'gc_b_done')

  const gc_d_result = gc_results.find((r) => r.handler_name === 'gc_handler_d')
  assert.ok(gc_d_result, 'gc_handler_d result should exist')
  assert.equal(gc_d_result!.status, 'completed')
  assert.equal(gc_d_result!.result, 'gc_d_done')

  // ── QueuedGrandchildEvent: CANCELLED by child_handler timeout ───────
  // This event was emitted but never awaited. It sat in pending_event_queue
  // until child_handler timed out, which triggered bus.cancelPendingDescendants
  // to walk ChildEvent.event_children and cancel all pending handlers.
  assert.ok(queued_grandchild_ref, 'QueuedGrandchildEvent should have been emitted')
  assert.equal(queued_grandchild_ref!.event_status, 'completed')

  const queued_gc_results = Array.from(queued_grandchild_ref!.event_results.values())
  assert.equal(queued_gc_results.length, 1, 'QueuedGC should have 1 handler result')
  assert.equal(queued_gc_results[0].status, 'error')
  assert.ok(
    queued_gc_results[0].error instanceof EventHandlerCancelledError,
    'QueuedGC handler should be EventHandlerCancelledError (not timeout — it never ran)'
  )
  // Verify the cancellation error chain: CancelledError.cause → TimeoutError
  assert.ok(
    (queued_gc_results[0].error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError,
    "QueuedGC cancellation should reference the child_handler's timeout as cause"
  )

  // ── SiblingEvent: CANCELLED by top_handler_main timeout ─────────────
  // Same pattern: emitted but never awaited, stays in queue, cancelled when
  // top_handler_main times out and bus.cancelPendingDescendants runs.
  assert.ok(sibling_ref, 'SiblingEvent should have been emitted')
  assert.equal(sibling_ref!.event_status, 'completed')

  const sibling_results = Array.from(sibling_ref!.event_results.values())
  assert.equal(sibling_results.length, 1, 'SiblingEvent should have 1 handler result')
  assert.equal(sibling_results[0].status, 'error')
  assert.ok(sibling_results[0].error instanceof EventHandlerCancelledError, 'SiblingEvent handler should be EventHandlerCancelledError')
  assert.ok(
    (sibling_results[0].error as EventHandlerCancelledError).cause instanceof EventHandlerTimeoutError,
    "SiblingEvent cancellation should reference top_handler_main's timeout as cause"
  )

  // ── Execution log: verify what ran and what didn't ──────────────────
  // These handlers started AND completed:
  assert.ok(execution_log.includes('top_fast_start'), 'top_fast should have started')
  assert.ok(execution_log.includes('top_fast_complete'), 'top_fast should have completed')
  assert.ok(execution_log.includes('gc_b_complete'), 'gc_b (sync) should have completed')
  assert.ok(execution_log.includes('gc_d_start'), 'gc_d should have started')
  assert.ok(execution_log.includes('gc_d_complete'), 'gc_d should have completed')

  // These handlers started but were interrupted by their own timeout:
  assert.ok(execution_log.includes('gc_a_start'), 'gc_a should have started')
  assert.ok(!execution_log.includes('gc_a_end'), 'gc_a should NOT have finished (timed out)')
  assert.ok(execution_log.includes('gc_c_start'), 'gc_c should have started')
  assert.ok(!execution_log.includes('gc_c_end'), 'gc_c should NOT have finished (timed out)')
  assert.ok(execution_log.includes('gc_e_start'), 'gc_e should have started')
  assert.ok(!execution_log.includes('gc_e_end'), 'gc_e should NOT have finished (timed out)')

  // These handlers started and progressed, then parent timeout interrupted:
  assert.ok(execution_log.includes('top_main_start'), 'top_main should have started')
  assert.ok(execution_log.includes('child_start'), 'child should have started')
  assert.ok(execution_log.includes('child_after_grandchild'), 'child should have continued after grandchild completed')
  assert.ok(execution_log.includes('top_main_after_child'), 'top_main should have continued after child completed')
  assert.ok(!execution_log.includes('child_end'), 'child should NOT have finished (timed out)')
  assert.ok(!execution_log.includes('top_main_end'), 'top_main should NOT have finished (timed out)')

  // These handlers never ran at all (cancelled before starting):
  assert.ok(!execution_log.includes('queued_gc_start'), 'queued_gc should never have started')
  assert.ok(!execution_log.includes('sibling_start'), 'sibling should never have started')

  // ── Parent-child tree structure ─────────────────────────────────────
  assert.ok(
    top.event_children.some((c) => c.event_id === child_ref!.event_id),
    'ChildEvent should be in TopEvent.event_children'
  )
  assert.ok(
    top.event_children.some((c) => c.event_id === sibling_ref!.event_id),
    'SiblingEvent should be in TopEvent.event_children'
  )
  assert.ok(
    child_ref!.event_children.some((c) => c.event_id === grandchild_ref!.event_id),
    'GrandchildEvent should be in ChildEvent.event_children'
  )
  assert.ok(
    child_ref!.event_children.some((c) => c.event_id === queued_grandchild_ref!.event_id),
    'QueuedGrandchildEvent should be in ChildEvent.event_children'
  )

  // ── Timing invariants ──────────────────────────────────────────────
  // All events should have completion timestamps
  for (const evt of [top, child_ref!, grandchild_ref!, queued_grandchild_ref!, sibling_ref!]) {
    assert.ok(evt.event_completed_at, `${evt.event_type} should have event_completed_at`)
  }
  // All handler results should have started_at and completed_at
  for (const result of top_results) {
    assert.ok(result.started_at, `${result.handler_name} should have started_at`)
    assert.ok(result.completed_at, `${result.handler_name} should have completed_at`)
  }
  for (const result of gc_results) {
    assert.ok(result.started_at, `${result.handler_name} should have started_at`)
    assert.ok(result.completed_at, `${result.handler_name} should have completed_at`)
  }
})

// =============================================================================
// Verify the timeout→cancellation error chain is intact at every level.
// When a parent handler times out and cancels a child's pending handlers,
// the EventHandlerCancelledError.cause must reference the specific
// EventHandlerTimeoutError that caused the cascade. This test creates a
// 2-level chain where each level's cancellation error can be inspected.
// =============================================================================

test('cancellation error chain preserves cause references through hierarchy', async () => {
  const OuterEvent = BaseEvent.extend('ErrorChainOuter', {})
  const InnerEvent = BaseEvent.extend('ErrorChainInner', {})
  const DeepEvent = BaseEvent.extend('ErrorChainDeep', {})

  const bus = new EventBus('ErrorChainBus', {
    event_concurrency: 'bus-serial',
    handler_concurrency: 'bus-serial',
  })

  let inner_ref: InstanceType<typeof InnerEvent> | null = null
  let deep_ref: InstanceType<typeof DeepEvent> | null = null

  // DeepEvent handler: sleeps long, will be still pending when inner times out
  // Because DeepEvent is emitted but NOT awaited, it stays in the queue.
  const deep_handler = async () => {
    await delay(200)
    return 'deep_done'
  }

  // InnerEvent handler: emits DeepEvent (not awaited), then sleeps long → times out
  const inner_handler = async (event: InstanceType<typeof InnerEvent>) => {
    deep_ref = event.bus?.emit(DeepEvent({ event_timeout: 0.5 }))!
    await delay(200) // interrupted by inner timeout
    return 'inner_done'
  }

  // OuterEvent handler: emits InnerEvent (awaited), then sleeps long → times out
  const outer_handler = async (event: InstanceType<typeof OuterEvent>) => {
    inner_ref = event.bus?.emit(InnerEvent({ event_timeout: 0.04 }))!
    await inner_ref.done()
    await delay(200) // interrupted by outer timeout
    return 'outer_done'
  }

  bus.on(OuterEvent, outer_handler)
  bus.on(InnerEvent, inner_handler)
  bus.on(DeepEvent, deep_handler)

  const outer = bus.dispatch(OuterEvent({ event_timeout: 0.15 }))
  await outer.done()
  await bus.waitUntilIdle()

  // Outer handler timed out
  const outer_result = Array.from(outer.event_results.values())[0]
  assert.equal(outer_result.status, 'error')
  assert.ok(outer_result.error instanceof EventHandlerTimeoutError)
  // Inner handler timed out (its own 40ms timeout, not outer's)
  assert.ok(inner_ref)
  const inner_result = Array.from(inner_ref!.event_results.values())[0]
  assert.equal(inner_result.status, 'error')
  assert.ok(inner_result.error instanceof EventHandlerTimeoutError)
  const inner_timeout = inner_result.error as EventHandlerTimeoutError

  // Inner's timeout is from InnerEvent's own event_timeout (40ms),
  // not inherited from outer
  assert.ok(inner_timeout.message.includes('inner_handler'), 'Inner timeout should name inner_handler')

  // DeepEvent was cancelled when inner_handler timed out.
  // The cancellation error should reference inner_handler's timeout (not outer's).
  assert.ok(deep_ref)
  const deep_result = Array.from(deep_ref!.event_results.values())[0]
  assert.equal(deep_result.status, 'error')
  assert.ok(
    deep_result.error instanceof EventHandlerCancelledError,
    'DeepEvent handler should be cancelled, not timed out (it never started)'
  )
  const deep_cancel = deep_result.error as EventHandlerCancelledError
  assert.ok(deep_cancel.cause instanceof EventHandlerTimeoutError, 'Cancellation should reference parent timeout')
  // The cause should be the INNER handler's timeout, because that's
  // the handler whose bus.cancelPendingDescendants actually cancelled DeepEvent.
  assert.ok(
    deep_cancel.cause.message.includes('inner_handler') || deep_cancel.cause.message.includes('child_handler'),
    'cause should reference the handler that directly caused cancellation'
  )
})

// =============================================================================
// When a parent has a timeout but a child has event_timeout: null (no timeout),
// the child's handlers run indefinitely on their own — but if the PARENT times
// out, bus.cancelPendingDescendants still cancels any pending child handlers.
// This tests that cancellation works across timeout/no-timeout boundaries.
// =============================================================================

test('parent timeout cancels children that have no timeout of their own', async () => {
  const ParentEvent = BaseEvent.extend('TimeoutBoundaryParent', {})
  const NoTimeoutChild = BaseEvent.extend('TimeoutBoundaryChild', {})

  const bus = new EventBus('TimeoutBoundaryBus', {
    event_concurrency: 'bus-serial',
    handler_concurrency: 'bus-serial',
    event_timeout: null, // no bus-level default
  })

  let child_ref: InstanceType<typeof NoTimeoutChild> | null = null
  let child_handler_ran = false

  // Child handler: would run forever but should be cancelled
  const child_slow_handler = async () => {
    child_handler_ran = true
    await delay(500)
    return 'child_done'
  }

  // Parent handler: emits child (not awaited), then sleeps → parent times out
  const parent_handler = async (event: InstanceType<typeof ParentEvent>) => {
    // event_timeout: null means the child has no timeout of its own.
    // It would run forever if the parent didn't cancel it.
    child_ref = event.bus?.emit(NoTimeoutChild({ event_timeout: null }))!
    await delay(200)
    return 'parent_done'
  }

  bus.on(ParentEvent, parent_handler)
  bus.on(NoTimeoutChild, child_slow_handler)

  const parent = bus.dispatch(ParentEvent({ event_timeout: 0.03 }))
  await parent.done()
  await bus.waitUntilIdle()

  // Parent timed out
  const parent_result = Array.from(parent.event_results.values())[0]
  assert.equal(parent_result.status, 'error')
  assert.ok(parent_result.error instanceof EventHandlerTimeoutError)

  // Child should exist and be cancelled (it was in the queue, never started)
  assert.ok(child_ref, 'Child event should have been emitted')
  assert.equal(child_ref!.event_status, 'completed')
  assert.equal(child_handler_ran, false, 'Child handler should never have started')

  const child_results = Array.from(child_ref!.event_results.values())
  assert.equal(child_results.length, 1)
  assert.ok(
    child_results[0].error instanceof EventHandlerCancelledError,
    'Child handler should be cancelled by parent timeout, even though it has no timeout'
  )
})
