import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerTimeoutError } from '../dist/esm/index.js'
import { runAllPerfScenarios } from './performance.scenarios.js'

declare const Bun: { gc?: (full?: boolean) => void } | undefined
declare const Deno:
  | {
      memoryUsage?: () => { rss: number; heapUsed: number }
    }
  | undefined
declare const process:
  | {
      versions?: { node?: string; bun?: string }
      memoryUsage?: () => { rss: number; heapUsed: number }
    }
  | undefined

const runtime = typeof Bun !== 'undefined' && Bun ? 'bun' : typeof Deno !== 'undefined' && Deno ? 'deno' : 'node'

const getMemoryUsage = () => {
  if (typeof process !== 'undefined' && typeof process.memoryUsage === 'function') {
    return process.memoryUsage()
  }
  if (typeof Deno !== 'undefined' && Deno && typeof Deno.memoryUsage === 'function') {
    return Deno.memoryUsage()
  }
  return { heapUsed: 0, rss: 0 }
}

const forceGc = () => {
  const maybeGc = (globalThis as { gc?: () => void }).gc
  if (typeof maybeGc === 'function') {
    maybeGc()
    return
  }
  if (typeof Bun !== 'undefined' && Bun && typeof Bun.gc === 'function') {
    Bun.gc(true)
  }
}

const main = async () => {
  console.log(`[${runtime}] runtime perf harness starting`)

  await runAllPerfScenarios({
    runtimeName: runtime,
    api: { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError },
    now: () => performance.now(),
    sleep: (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
    log: (message: string) => console.log(message),
    forceGc,
    getMemoryUsage,
    limits: {
      singleRunMs: 30_000,
      worstCaseMs: 60_000,
      // Bun's heap accounting can be noisy; keep runtime harness tolerant.
      worstCaseMemoryDeltaMb: 150,
      // Runtime harness focuses on comparative perf metrics; strict post-GC
      // zero-delta checks are handled in the node:test suite.
      enforceNonPositiveHeapDeltaAfterGc: false,
    },
  })

  console.log(`[${runtime}] runtime perf harness complete`)
}

await main()
