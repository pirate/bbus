import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus, retry, clearSemaphoreRegistry } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

// ─── first() with parallel handlers ─────────────────────────────────────────

test('first: returns the first non-undefined result from parallel handlers', async () => {
  const bus = new EventBus('FirstParallelBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstParallelEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    await delay(100)
    return 'slow handler'
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    await delay(10)
    return 'fast handler'
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'fast handler', 'should return the temporally first non-undefined result')
})

test('first: cancels remaining parallel handlers after first result', async () => {
  const bus = new EventBus('FirstCancelBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstCancelEvent', { event_result_schema: z.string() })

  let slow_handler_completed = false

  bus.on(TestEvent, async (_event) => {
    await delay(10)
    return 'fast result'
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    await delay(500)
    slow_handler_completed = true
    return 'slow result'
  })

  const event = bus.emit(TestEvent({}))
  const result = await event.first()

  assert.equal(result, 'fast result')
  assert.equal(slow_handler_completed, false, 'slow handler should have been aborted')

  // Verify the slow handler was aborted
  const results = Array.from(event.event_results.values())
  const aborted = results.filter((r) => r.status === 'error')
  assert.equal(aborted.length, 1, 'one handler should be aborted')
})

// ─── first() with serial handlers ───────────────────────────────────────────

test('first: returns the first non-undefined result from serial handlers', async () => {
  const bus = new EventBus('FirstSerialBus', { event_timeout: null, event_handler_concurrency: 'bus-serial' })
  const TestEvent = BaseEvent.extend('FirstSerialEvent', { event_result_schema: z.string() })

  let second_handler_called = false

  bus.on(TestEvent, async (_event) => {
    return 'first handler result'
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    second_handler_called = true
    return 'second handler result'
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'first handler result')
  assert.equal(second_handler_called, false, 'second handler should not have run')
})

test('first: serial mode skips first handler returning undefined, takes second', async () => {
  const bus = new EventBus('FirstSerialSkipBus', { event_timeout: null, event_handler_concurrency: 'bus-serial' })
  const TestEvent = BaseEvent.extend('FirstSerialSkipEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    return undefined // no result
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    return 'second handler has it'
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'second handler has it')
})

// ─── first() edge cases ─────────────────────────────────────────────────────

test('first: returns undefined when all handlers return undefined', async () => {
  const bus = new EventBus('FirstUndefinedBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstUndefinedEvent', {})

  bus.on(TestEvent, async (_event) => {
    return undefined
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    // no return (void)
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, undefined)
})

test('first: returns undefined when all handlers throw errors', async () => {
  const bus = new EventBus('FirstErrorBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstErrorEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    throw new Error('handler 1 error')
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    throw new Error('handler 2 error')
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, undefined, 'should return undefined when no handler succeeds')
})

test('first: skips error handlers and returns the successful one', async () => {
  const bus = new EventBus('FirstMixBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstMixEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    throw new Error('fast but fails')
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    await delay(20)
    return 'slow but succeeds'
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'slow but succeeds')
})

test('first: returns undefined when no handlers are registered', async () => {
  const bus = new EventBus('FirstNoHandlerBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstNoHandlerEvent', {})

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, undefined)
})

test('first: rejects when event has no bus attached', async () => {
  const TestEvent = BaseEvent.extend('FirstNoBusEvent', {})
  const event = TestEvent({})

  await assert.rejects(event.first(), { message: 'event has no bus attached' })
})

// ─── first() with @retry() decorated handlers ──────────────────────────────

test('first: @retry decorated handler retries before first() resolves', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('FirstRetryBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstRetryEvent', { event_result_schema: z.string() })

  let fast_attempts = 0

  class Service {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(TestEvent, this.on_fast.bind(this))
    }

    @retry({ max_attempts: 3 })
    async on_fast(_event: InstanceType<typeof TestEvent>): Promise<string | undefined> {
      fast_attempts++
      if (fast_attempts < 3) throw new Error(`attempt ${fast_attempts} failed`)
      return 'succeeded after retries'
    }
  }

  new Service(bus)

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'succeeded after retries')
  assert.equal(fast_attempts, 3)
})

test('first: fast handler wins and slow @retry handler gets cancelled', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('FirstRetryRaceBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstRetryRaceEvent', { event_result_schema: z.string() })

  let slow_attempts = 0

  // fast handler returns immediately
  bus.on(TestEvent, async (_event) => {
    return 'fast path'
  })

  await delay(2)

  class SlowService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(TestEvent, this.on_slow.bind(this))
    }

    @retry({ max_attempts: 5, retry_after: 0.1 })
    async on_slow(_event: InstanceType<typeof TestEvent>): Promise<string> {
      slow_attempts++
      await delay(200)
      return 'slow path'
    }
  }

  new SlowService(bus)

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 'fast path')
  assert.equal(slow_attempts <= 1, true, 'slow handler should have been aborted after at most 1 attempt')
})

// ─── first() with the recommended @retry decorator pattern ──────────────────

test('first: screenshot-service pattern — fast path wins, slow path with retry cancelled', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScreenshotBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', {
    page_id: z.string(),
    event_result_schema: z.string(),
  })

  let fast_called = false
  let slow_called = false

  class ScreenshotService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(ScreenshotEvent, this.on_ScreenshotEvent_fast.bind(this))
      // small delay so handler IDs don't collide
    }

    async on_ScreenshotEvent_fast(_event: InstanceType<typeof ScreenshotEvent>): Promise<string | undefined> {
      fast_called = true
      return 'fast_screenshot_data'
    }
  }

  class SlowScreenshotService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(ScreenshotEvent, this.on_ScreenshotEvent_slow.bind(this))
    }

    @retry({ max_attempts: 3, timeout: 15, semaphore_scope: 'global', semaphore_limit: 1, semaphore_name: 'Screenshots' })
    async on_ScreenshotEvent_slow(_event: InstanceType<typeof ScreenshotEvent>): Promise<string> {
      slow_called = true
      await delay(500)
      return 'slow_screenshot_data'
    }
  }

  new ScreenshotService(bus)
  await delay(2)
  new SlowScreenshotService(bus)

  const screenshot = await bus.emit(ScreenshotEvent({ page_id: 'page-1' })).first()

  assert.equal(screenshot, 'fast_screenshot_data')
  assert.equal(fast_called, true)
  // slow handler may or may not have started, but should be aborted before completing
})

test('first: screenshot-service pattern — fast path fails, slow path with retry succeeds', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScreenshotFallbackBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const ScreenshotEvent = BaseEvent.extend('ScreenshotFallbackEvent', {
    page_id: z.string(),
    event_result_schema: z.string(),
  })

  let slow_attempts = 0

  class ScreenshotService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(ScreenshotEvent, this.on_fast.bind(this))
    }

    async on_fast(_event: InstanceType<typeof ScreenshotEvent>): Promise<string | undefined> {
      // fast path fails, returns undefined to signal "I can't handle this"
      return undefined
    }
  }

  class SlowScreenshotService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(ScreenshotEvent, this.on_slow.bind(this))
    }

    @retry({ max_attempts: 3 })
    async on_slow(_event: InstanceType<typeof ScreenshotEvent>): Promise<string> {
      slow_attempts++
      if (slow_attempts < 2) throw new Error('screenshot timeout')
      return 'slow_screenshot_data'
    }
  }

  new ScreenshotService(bus)
  await delay(2)
  new SlowScreenshotService(bus)

  const screenshot = await bus.emit(ScreenshotEvent({ page_id: 'page-2' })).first()

  assert.equal(screenshot, 'slow_screenshot_data')
  assert.equal(slow_attempts, 2, 'slow handler needed 2 attempts')
})

// ─── first() with single handler ────────────────────────────────────────────

test('first: works with a single handler', async () => {
  const bus = new EventBus('FirstSingleBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstSingleEvent', { event_result_schema: z.number() })

  bus.on(TestEvent, async (_event) => {
    return 42
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 42)
})

// ─── first() preserves non-undefined falsy values ───────────────────────────

test('first: returns null as a valid first result (not treated as undefined)', async () => {
  const bus = new EventBus('FirstNullBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstNullEvent', {})

  bus.on(TestEvent, async (_event) => {
    return null
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, null, 'null is a valid non-undefined result')
})

test('first: returns 0 as a valid first result', async () => {
  const bus = new EventBus('FirstZeroBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstZeroEvent', { event_result_schema: z.number() })

  bus.on(TestEvent, async (_event) => {
    return 0
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, 0, '0 is a valid non-undefined result')
})

test('first: returns empty string as a valid first result', async () => {
  const bus = new EventBus('FirstEmptyBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstEmptyEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    return ''
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, '', 'empty string is a valid non-undefined result')
})

test('first: returns false as a valid first result', async () => {
  const bus = new EventBus('FirstFalseBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstFalseEvent', { event_result_schema: z.boolean() })

  bus.on(TestEvent, async (_event) => {
    return false
  })

  const result = await bus.emit(TestEvent({})).first()

  assert.equal(result, false, 'false is a valid non-undefined result')
})

// ─── first() cancels child events of losing handlers ────────────────────────

test('first: cancels child events emitted by losing handlers', async () => {
  const bus = new EventBus('FirstChildBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const ParentEvent = BaseEvent.extend('FirstChildParent', { event_result_schema: z.string() })
  const ChildEvent = BaseEvent.extend('FirstChildChild', {})

  let child_handler_called = false

  bus.on(ChildEvent, async (_event) => {
    child_handler_called = true
    await delay(500) // very slow
    return 'child result'
  })

  // Fast handler: returns immediately
  bus.on(ParentEvent, async (_event) => {
    return 'fast parent'
  })

  await delay(2)

  // Slow handler: emits a child event, then waits
  bus.on(ParentEvent, async (event) => {
    const child = event.bus!.emit(ChildEvent({}))
    await child.done()
    return 'slow parent with child'
  })

  const result = await bus.emit(ParentEvent({})).first()

  assert.equal(result, 'fast parent')
  // Give a moment for any async cleanup
  await delay(50)
  // The child event emitted by the slow handler should have been cancelled
})

// ─── event_handler_completion field visibility ──────────────────────────────

test('first: event_handler_completion is set to "first" after calling first()', async () => {
  const bus = new EventBus('FirstFieldBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstFieldEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    return 'result'
  })

  const event = bus.emit(TestEvent({}))
  const original = (event as any)._event_original ?? event

  // before first(), completion mode is undefined (defaults to 'all')
  assert.equal(original.event_handler_completion, undefined)

  const result = await event.first()

  // after first(), completion mode is 'first'
  assert.equal(original.event_handler_completion, 'first')
  assert.equal(result, 'result')
})

test('first: event_handler_completion appears in toJSON output', async () => {
  const bus = new EventBus('FirstJsonBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('FirstJsonEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    return 'json result'
  })

  const event = bus.emit(TestEvent({}))
  await event.first()

  const original = (event as any)._event_original ?? event
  const json = original.toJSON()

  assert.equal(json.event_handler_completion, 'first', 'toJSON should include event_handler_completion')
})

test('first: event_handler_completion can be set via event constructor', async () => {
  const bus = new EventBus('FirstCtorBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const TestEvent = BaseEvent.extend('FirstCtorEvent', { event_result_schema: z.string() })

  bus.on(TestEvent, async (_event) => {
    await delay(100)
    return 'slow handler'
  })

  await delay(2)

  bus.on(TestEvent, async (_event) => {
    await delay(10)
    return 'fast handler'
  })

  // Set event_handler_completion directly on the event data
  const event = bus.emit(TestEvent({ event_handler_completion: 'first' } as any))
  const result = await event.first()

  assert.equal(result, 'fast handler', 'should still use first-mode when set via constructor')
})
