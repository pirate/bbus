import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  BaseEvent,
  EventBus,
  retry,
  clearSemaphoreRegistry,
  RetryTimeoutError,
  SemaphoreTimeoutError,
} from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

// ─── Basic retry behavior ────────────────────────────────────────────────────

test('retry: function succeeds on first attempt with no retries needed', async () => {
  const fn = retry({ max_attempts: 3 })(async () => 'ok')
  assert.equal(await fn(), 'ok')
})

test('retry: function retries on failure and eventually succeeds', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(async () => {
    calls++
    if (calls < 3) throw new Error(`fail ${calls}`)
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: throws after exhausting all attempts', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(async () => {
    calls++
    throw new Error('always fails')
  })
  await assert.rejects(fn, { message: 'always fails' })
  assert.equal(calls, 3)
})

test('retry: max_attempts=1 means no retries (single attempt)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 1 })(async () => {
    calls++
    throw new Error('fail')
  })
  await assert.rejects(fn, { message: 'fail' })
  assert.equal(calls, 1)
})

test('retry: default max_attempts=1 means single attempt', async () => {
  let calls = 0
  const fn = retry()(async () => {
    calls++
    throw new Error('fail')
  })
  await assert.rejects(fn, { message: 'fail' })
  assert.equal(calls, 1)
})

// ─── retry_after delay ───────────────────────────────────────────────────────

test('retry: retry_after introduces delay between attempts', async () => {
  let calls = 0
  const timestamps: number[] = []
  const fn = retry({ max_attempts: 3, retry_after: 0.05 })(async () => {
    calls++
    timestamps.push(performance.now())
    if (calls < 3) throw new Error('fail')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)

  // Check that delays were at least ~50ms between attempts
  const gap1 = timestamps[1] - timestamps[0]
  const gap2 = timestamps[2] - timestamps[1]
  assert.ok(gap1 >= 40, `expected >=40ms gap, got ${gap1.toFixed(1)}ms`)
  assert.ok(gap2 >= 40, `expected >=40ms gap, got ${gap2.toFixed(1)}ms`)
})

// ─── Exponential backoff ─────────────────────────────────────────────────────

test('retry: retry_backoff_factor increases delay between attempts', async () => {
  let calls = 0
  const timestamps: number[] = []
  const fn = retry({ max_attempts: 4, retry_after: 0.03, retry_backoff_factor: 2.0 })(async () => {
    calls++
    timestamps.push(performance.now())
    if (calls < 4) throw new Error('fail')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 4)

  // Delays: 30ms, 60ms, 120ms (0.03 * 2^0, 0.03 * 2^1, 0.03 * 2^2)
  const gap1 = timestamps[1] - timestamps[0]
  const gap2 = timestamps[2] - timestamps[1]
  const gap3 = timestamps[3] - timestamps[2]

  assert.ok(gap1 >= 20, `gap1=${gap1.toFixed(1)}ms, expected >=20ms`)
  assert.ok(gap2 >= 45, `gap2=${gap2.toFixed(1)}ms, expected >=45ms (should be ~60ms)`)
  assert.ok(gap3 >= 90, `gap3=${gap3.toFixed(1)}ms, expected >=90ms (should be ~120ms)`)
  // Verify backoff is actually increasing
  assert.ok(gap2 > gap1, 'gap2 should be larger than gap1')
  assert.ok(gap3 > gap2, 'gap3 should be larger than gap2')
})

// ─── retry_on_errors filtering ───────────────────────────────────────────────

class NetworkError extends Error {
  constructor(message: string = 'network error') {
    super(message)
    this.name = 'NetworkError'
  }
}

class ValidationError extends Error {
  constructor(message: string = 'validation error') {
    super(message)
    this.name = 'ValidationError'
  }
}

test('retry: retry_on_errors retries only matching error types', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError()
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors does not retry non-matching errors', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async () => {
    calls++
    throw new ValidationError()
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  // Should have thrown immediately without retrying
  assert.equal(calls, 1)
})

test('retry: retry_on_errors accepts string error name', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: ['NetworkError'] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError()
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors string matcher does not retry non-matching names', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: ['NetworkError'] })(async () => {
    calls++
    throw new ValidationError()
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  assert.equal(calls, 1)
})

test('retry: retry_on_errors accepts RegExp pattern', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [/network/i] })(async () => {
    calls++
    if (calls < 3) throw new NetworkError('Network timeout occurred')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

test('retry: retry_on_errors RegExp does not retry non-matching errors', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, retry_on_errors: [/network/i] })(async () => {
    calls++
    throw new ValidationError('bad input')
  })
  await assert.rejects(fn, { name: 'ValidationError' })
  assert.equal(calls, 1)
})

test('retry: retry_on_errors mixes class, string, and RegExp matchers', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 5, retry_on_errors: [TypeError, 'NetworkError', /timeout/i] })(async () => {
    calls++
    if (calls === 1) throw new TypeError('type error')
    if (calls === 2) throw new NetworkError()
    if (calls === 3) throw new Error('Connection timeout')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 4)
})

test('retry: retry_on_errors with multiple error types', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 5, retry_on_errors: [NetworkError, TypeError] })(async () => {
    calls++
    if (calls === 1) throw new NetworkError()
    if (calls === 2) throw new TypeError('type error')
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

// ─── Per-attempt timeout ─────────────────────────────────────────────────────

test('retry: timeout triggers RetryTimeoutError on slow attempts', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 1, timeout: 0.05 })(async () => {
    calls++
    await delay(200)
    return 'ok'
  })
  await assert.rejects(fn, (error: unknown) => {
    assert.ok(error instanceof RetryTimeoutError)
    assert.equal(error.attempt, 1)
    return true
  })
  assert.equal(calls, 1)
})

test('retry: timeout allows fast attempts to succeed', async () => {
  const fn = retry({ max_attempts: 1, timeout: 1 })(async () => {
    await delay(5)
    return 'fast'
  })
  assert.equal(await fn(), 'fast')
})

test('retry: timed-out attempts are retried when max_attempts > 1', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3, timeout: 0.05 })(async () => {
    calls++
    if (calls < 3) {
      await delay(200) // will timeout
      return 'slow'
    }
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 3)
})

// ─── Semaphore concurrency control ──────────────────────────────────────────

test('retry: semaphore_limit controls max concurrent executions', async (t) => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({ max_attempts: 1, semaphore_limit: 2, semaphore_name: 'test_sem_limit' })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(50)
    active--
  })

  // Launch 6 concurrent calls — should only run 2 at a time
  await Promise.all([fn(), fn(), fn(), fn(), fn(), fn()])
  assert.equal(max_active, 2, 'should never exceed semaphore_limit=2')
})

test('retry: semaphore_lax=false throws SemaphoreTimeoutError when slots are full', async () => {
  clearSemaphoreRegistry()

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_lax_false',
    semaphore_lax: false,
    semaphore_timeout: 0.05,
  })(async () => {
    await delay(200) // hold the semaphore for a while
    return 'ok'
  })

  // Start one call to grab the semaphore
  const first = fn()

  // Give the first call time to acquire the semaphore
  await delay(10)

  // Second call should timeout trying to acquire semaphore
  await assert.rejects(
    fn(),
    (error: unknown) => {
      assert.ok(error instanceof SemaphoreTimeoutError)
      assert.equal(error.semaphore_name, 'test_sem_lax_false')
      return true
    }
  )

  // Let the first call finish
  assert.equal(await first, 'ok')
})

test('retry: semaphore_lax=true (default) proceeds without semaphore on timeout', async () => {
  clearSemaphoreRegistry()

  let calls = 0
  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_lax_true',
    semaphore_lax: true,
    semaphore_timeout: 0.05,
  })(async () => {
    calls++
    await delay(200)
    return 'ok'
  })

  // Start first call to grab the semaphore
  const first = fn()
  await delay(10)

  // Second call should proceed anyway (lax mode)
  const second = fn()
  const results = await Promise.all([first, second])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(calls, 2)
})

// ─── Preserves function metadata ─────────────────────────────────────────────

test('retry: preserves function name', () => {
  async function myNamedFunction(): Promise<string> {
    return 'ok'
  }
  const wrapped = retry()(myNamedFunction)
  assert.equal(wrapped.name, 'myNamedFunction')
})

// ─── Preserves `this` context ────────────────────────────────────────────────

test('retry: preserves this context for methods', async () => {
  class MyService {
    value = 42
    fetch = retry({ max_attempts: 2 })(async function (this: MyService) {
      return this.value
    })
  }

  const svc = new MyService()
  assert.equal(await svc.fetch(), 42)
})

// ─── Works with synchronous functions ────────────────────────────────────────

test('retry: wraps sync functions (result becomes a promise)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 3 })(() => {
    calls++
    if (calls < 2) throw new Error('sync fail')
    return 'sync ok'
  })
  assert.equal(await fn(), 'sync ok')
  assert.equal(calls, 2)
})

// ─── Integration with EventBus ───────────────────────────────────────────────

test('retry: works as event bus handler wrapper', async () => {
  const bus = new EventBus('RetryBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3 })(async (_event) => {
      calls++
      if (calls < 3) throw new Error(`handler fail ${calls}`)
      return 'handler ok'
    })
  )

  const event = bus.dispatch(TestEvent({}))
  await event.done()

  assert.equal(calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.equal(result.result, 'handler ok')
})

test('retry: bus handler with retry_on_errors only retries matching errors', async () => {
  const bus = new EventBus('RetryFilterBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  let calls = 0
  bus.on(
    TestEvent,
    retry({ max_attempts: 3, retry_on_errors: [NetworkError] })(async (_event) => {
      calls++
      throw new ValidationError()
    })
  )

  const event = bus.dispatch(TestEvent({}))
  await event.done()

  // Should have failed immediately without retrying
  assert.equal(calls, 1)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
})

// ─── Edge cases ──────────────────────────────────────────────────────────────

test('retry: max_attempts=0 is treated as 1 (minimum)', async () => {
  let calls = 0
  const fn = retry({ max_attempts: 0 })(async () => {
    calls++
    return 'ok'
  })
  assert.equal(await fn(), 'ok')
  assert.equal(calls, 1)
})

test('retry: passes arguments through to wrapped function', async () => {
  const fn = retry({ max_attempts: 1 })(async (a: number, b: string) => `${a}-${b}`)
  assert.equal(await fn(1, 'hello'), '1-hello')
})

test('retry: semaphore is held across all retry attempts', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0
  let total_calls = 0

  const fn = retry({
    max_attempts: 3,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_across_retries',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    total_calls++
    await delay(10)
    active--
    // Odd calls fail, even calls succeed — each invocation needs 2 attempts
    if (total_calls % 2 === 1) throw new Error('fail')
    return 'ok'
  })

  // Run 3 calls concurrently — they should run serially because semaphore_limit=1
  // The semaphore should be held across retries, so only 1 active at a time
  const results = await Promise.all([fn(), fn(), fn()])
  assert.equal(max_active, 1, 'semaphore should enforce serial execution even during retries')
  assert.deepEqual(results, ['ok', 'ok', 'ok'])
  assert.equal(total_calls, 6, 'each of 3 calls should have taken 2 attempts')
})

test('retry: semaphore released even when all attempts fail', async () => {
  clearSemaphoreRegistry()

  const fn = retry({
    max_attempts: 2,
    semaphore_limit: 1,
    semaphore_name: 'test_sem_release_on_fail',
  })(async () => {
    throw new Error('always fails')
  })

  // First call fails, should release semaphore
  await assert.rejects(fn)

  // Second call should be able to acquire the semaphore (not deadlocked)
  await assert.rejects(fn)
})

// ─── TC39 decorator syntax on class methods ──────────────────────────────────

test('retry: works on class method via manual wrapping pattern', async () => {
  // Since TC39 Stage 3 decorators require experimentalDecorators or TS 5.0+ native support,
  // we test the equivalent pattern: applying retry() to a method post-definition.
  class ApiClient {
    base_url = 'https://example.com'
    calls = 0

    fetchData = retry({ max_attempts: 3 })(async function (this: ApiClient) {
      this.calls++
      if (this.calls < 3) throw new Error('api error')
      return `data from ${this.base_url}`
    })
  }

  const client = new ApiClient()
  assert.equal(await client.fetchData(), 'data from https://example.com')
  assert.equal(client.calls, 3)
})

// ─── Re-entrancy / deadlock prevention ───────────────────────────────────────

test('retry: re-entrant call on same semaphore does not deadlock', async () => {
  clearSemaphoreRegistry()

  const inner = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'shared_sem',
  })(async () => {
    return 'inner ok'
  })

  const outer = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'shared_sem',
  })(async () => {
    // This would deadlock without re-entrancy tracking:
    // outer holds the semaphore, inner tries to acquire the same one
    const result = await inner()
    return `outer got: ${result}`
  })

  assert.equal(await outer(), 'outer got: inner ok')
})

test('retry: recursive function with semaphore does not deadlock', async () => {
  clearSemaphoreRegistry()

  let depth = 0
  const recurse: (n: number) => Promise<number> = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'recursive_sem',
  })(async (n: number): Promise<number> => {
    depth++
    if (n <= 1) return 1
    return n + (await recurse(n - 1))
  })

  const result = await recurse(5)
  assert.equal(result, 15) // 5 + 4 + 3 + 2 + 1
  assert.equal(depth, 5)
})

test('retry: different semaphore names do not interfere with re-entrancy', async () => {
  clearSemaphoreRegistry()

  let inner_active = 0
  let inner_max_active = 0

  const inner = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'inner_sem',
  })(async () => {
    inner_active++
    inner_max_active = Math.max(inner_max_active, inner_active)
    await delay(20)
    inner_active--
    return 'inner ok'
  })

  const outer = retry({
    max_attempts: 1,
    semaphore_limit: 2,
    semaphore_name: 'outer_sem',
  })(async () => {
    return await inner()
  })

  // Run 3 outer calls concurrently
  // outer_sem allows 2 concurrent, but inner_sem only allows 1
  const results = await Promise.all([outer(), outer(), outer()])
  assert.deepEqual(results, ['inner ok', 'inner ok', 'inner ok'])
  assert.equal(inner_max_active, 1, 'inner semaphore should still enforce limit=1')
})

test('retry: three-level nested re-entrancy does not deadlock', async () => {
  clearSemaphoreRegistry()

  const level3 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => 'level3')

  const level2 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => {
    const r = await level3()
    return `level2>${r}`
  })

  const level1 = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_name: 'nested_sem',
  })(async () => {
    const r = await level2()
    return `level1>${r}`
  })

  assert.equal(await level1(), 'level1>level2>level3')
})

// ─── Semaphore scope ─────────────────────────────────────────────────────────

test('retry: semaphore_scope=class shares semaphore across instances of same class', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  const b = new Worker()
  const c = new Worker()

  await Promise.all([a.run(), b.run(), c.run()])
  assert.equal(max_active, 1, 'class scope: all instances should share one semaphore')
})

test('retry: semaphore_scope=instance gives each instance its own semaphore', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  const b = new Worker()

  // Same instance: serialized (limit=1 per instance)
  // Different instances: can run in parallel (separate semaphores)
  await Promise.all([a.run(), b.run()])
  assert.equal(max_active, 2, 'instance scope: different instances should get separate semaphores')
})

test('retry: semaphore_scope=instance serializes calls on same instance', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Worker {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'work',
    })(async function (this: Worker) {
      active++
      max_active = Math.max(max_active, active)
      await delay(20)
      active--
      return 'done'
    })
  }

  const a = new Worker()
  await Promise.all([a.run(), a.run(), a.run()])
  assert.equal(max_active, 1, 'instance scope: same instance calls should serialize')
})

test('retry: semaphore_scope=class isolates different classes', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Alpha {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'run',
    })(async function (this: Alpha) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
    })
  }

  class Beta {
    run = retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'run',
    })(async function (this: Beta) {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
    })
  }

  await Promise.all([new Alpha().run(), new Beta().run()])
  assert.equal(max_active, 2, 'class scope: different classes should get separate semaphores')
})

// ─── TC39 Stage 3 decorator syntax ──────────────────────────────────────────

test('retry: @retry() TC39 decorator on class method retries on failure', async () => {
  clearSemaphoreRegistry()

  class ApiService {
    calls = 0

    @retry({ max_attempts: 3 })
    async fetchData(): Promise<string> {
      this.calls++
      if (this.calls < 3) throw new Error('api error')
      return 'data'
    }
  }

  const svc = new ApiService()
  assert.equal(await svc.fetchData(), 'data')
  assert.equal(svc.calls, 3)
})

test('retry: @retry() TC39 decorator preserves this context', async () => {
  class Config {
    endpoint = 'https://api.example.com'

    @retry({ max_attempts: 2 })
    async getEndpoint(): Promise<string> {
      return this.endpoint
    }
  }

  const cfg = new Config()
  assert.equal(await cfg.getEndpoint(), 'https://api.example.com')
})

test('retry: @retry() TC39 decorator with semaphore_scope=class', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Service {
    @retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'class',
      semaphore_name: 'handle',
    })
    async handle(): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  const a = new Service()
  const b = new Service()
  await Promise.all([a.handle(), b.handle()])
  assert.equal(max_active, 1, '@retry class scope: all instances share one semaphore')
})

test('retry: @retry() TC39 decorator with semaphore_scope=instance', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  class Service {
    @retry({
      max_attempts: 1,
      semaphore_limit: 1,
      semaphore_scope: 'instance',
      semaphore_name: 'handle',
    })
    async handle(): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  const a = new Service()
  const b = new Service()
  await Promise.all([a.handle(), b.handle()])
  assert.equal(max_active, 2, '@retry instance scope: different instances get separate semaphores')
})

test('retry: @retry() decorated method works with bus.on via bind', async () => {
  const bus = new EventBus('DecoratorBus', { event_timeout: null })
  const TestEvent = BaseEvent.extend('TestEvent', {})

  class Handler {
    calls = 0

    @retry({ max_attempts: 3 })
    async onTest(_event: InstanceType<typeof TestEvent>): Promise<string> {
      this.calls++
      if (this.calls < 3) throw new Error('handler fail')
      return 'handler ok'
    }
  }

  const handler = new Handler()
  bus.on(TestEvent, handler.onTest.bind(handler))

  const event = bus.dispatch(TestEvent({}))
  await event.done()
  assert.equal(handler.calls, 3)
  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.result, 'handler ok')
})

// ─── Scope fallback to global ───────────────────────────────────────────────

test('retry: semaphore_scope=class falls back to global for standalone functions', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_scope: 'class',
    semaphore_name: 'standalone_class',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  // Two concurrent calls should serialize since they share the same global-fallback semaphore
  const results = await Promise.all([fn(), fn()])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(max_active, 1, 'class scope on standalone fn should fall back to global and serialize')
})

test('retry: semaphore_scope=instance falls back to global for standalone functions', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const fn = retry({
    max_attempts: 1,
    semaphore_limit: 1,
    semaphore_scope: 'instance',
    semaphore_name: 'standalone_instance',
  })(async () => {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  // Two concurrent calls should serialize since they share the same global-fallback semaphore
  const results = await Promise.all([fn(), fn()])
  assert.deepEqual(results, ['ok', 'ok'])
  assert.equal(max_active, 1, 'instance scope on standalone fn should fall back to global and serialize')
})

// ─── Full usage patterns: @retry() decorator + bus.on via .bind(this) ───────

test('retry: @retry(scope=class) + bus.on via .bind — serializes across instances', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeClassBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeClassEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'class', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  // Two instances register handlers on the same bus
  // Small delay between registrations to ensure unique handler IDs (bus uses ms-precision timestamps in handler ID hash)
  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.dispatch(SomeEvent({}))
  await event.done()

  // class scope + limit=1: only 1 handler should run at a time across both instances
  assert.equal(max_active, 1, 'class scope should serialize across instances')
})

test('retry: @retry(scope=instance) + bus.on via .bind — isolates per instance', async () => {
  const bus = new EventBus('ScopeInstanceBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeInstanceEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'instance', semaphore_limit: 1, semaphore_name: 'on_SomeEvent_inst' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      total_calls++
      await delay(200)
      active--
      return 'ok'
    }
  }

  let total_calls = 0

  // Two instances register handlers — each gets its own semaphore
  // Small delay between registrations to ensure unique handler IDs (bus uses ms-precision timestamps in handler ID hash)
  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.dispatch(SomeEvent({}))
  await event.done()

  // instance scope: 2 different instances can run in parallel
  assert.equal(total_calls, 2, 'both handlers should have run')
  assert.equal(max_active, 2, `instance scope should allow different instances to run in parallel (got max_active=${max_active}, total_calls=${total_calls})`)
})

test('retry: @retry(scope=global) + bus.on via .bind — all calls share one semaphore', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('ScopeGlobalBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('ScopeGlobalEvent', {})

  let active = 0
  let max_active = 0

  class SomeService {
    constructor(b: InstanceType<typeof EventBus>) {
      b.on(SomeEvent, this.on_SomeEvent.bind(this))
    }

    @retry({ max_attempts: 1, semaphore_scope: 'global', semaphore_limit: 1, semaphore_name: 'on_SomeEvent' })
    async on_SomeEvent(_event: InstanceType<typeof SomeEvent>): Promise<string> {
      active++
      max_active = Math.max(max_active, active)
      await delay(30)
      active--
      return 'ok'
    }
  }

  // Small delay between registrations to ensure unique handler IDs
  new SomeService(bus)
  await delay(2)
  new SomeService(bus)

  const event = bus.dispatch(SomeEvent({}))
  await event.done()

  // global scope: all calls serialized
  assert.equal(max_active, 1, 'global scope should serialize all calls')
})

// ─── HOF pattern: retry({...})(fn).bind(instance) — bind AFTER wrapping ─────

test('retry: HOF retry()(fn).bind(instance) — instance scope works when bind is after wrap', async () => {
  clearSemaphoreRegistry()

  const bus = new EventBus('HOFBindBus', { event_timeout: null, event_handler_concurrency: 'parallel' })
  const SomeEvent = BaseEvent.extend('HOFBindEvent', {})

  let active = 0
  let max_active = 0

  const some_instance_a = { name: 'a' }
  const some_instance_b = { name: 'b' }

  const handler = retry({
    max_attempts: 1,
    semaphore_scope: 'instance',
    semaphore_limit: 1,
    semaphore_name: 'handler',
  })(async function (this: any, _event: InstanceType<typeof SomeEvent>): Promise<string> {
    active++
    max_active = Math.max(max_active, active)
    await delay(30)
    active--
    return 'ok'
  })

  // bind AFTER wrapping → wrapper receives correct `this` for scoping
  bus.on(SomeEvent, handler.bind(some_instance_a))
  bus.on(SomeEvent, handler.bind(some_instance_b))

  const event = bus.dispatch(SomeEvent({}))
  await event.done()

  // Two different instances → separate semaphores → can run in parallel
  assert.equal(max_active, 2, 'bind-after-wrap: different instances should run in parallel')
})

// ─── HOF pattern: retry({...})(fn.bind(instance)) — bind BEFORE wrapping ────
// NOTE: This falls back to global scope because JS cannot extract [[BoundThis]]
// from a bound function. The handler works correctly (this is preserved inside
// the handler), but the semaphore scoping cannot see the bound instance.
// Recommendation: use retry({...})(fn).bind(instance) instead.

test('retry: HOF retry()(fn.bind(instance)) — scope falls back to global (bind before wrap)', async () => {
  clearSemaphoreRegistry()

  let active = 0
  let max_active = 0

  const instance_a = { name: 'a' }
  const instance_b = { name: 'b' }

  const make_handler = (inst: object) =>
    retry({
      max_attempts: 1,
      semaphore_scope: 'instance',
      semaphore_limit: 1,
      semaphore_name: 'handler_bind_before',
    })(
      (async function (this: any, _event: any): Promise<string> {
        active++
        max_active = Math.max(max_active, active)
        await delay(30)
        active--
        return 'ok'
      }).bind(inst)
    )

  const handler_a = make_handler(instance_a)
  const handler_b = make_handler(instance_b)

  // Both handlers fall back to global scope (same semaphore), so they serialize
  await Promise.all([handler_a('event1'), handler_b('event2')])
  assert.equal(max_active, 1, 'bind-before-wrap: scoping falls back to global (serialized)')
})
