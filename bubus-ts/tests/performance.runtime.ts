import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerTimeoutError } from '../dist/esm/index.js'
import { runAllPerfScenarios } from './performance.scenarios.js'

declare const Bun: { gc?: (full?: boolean) => void } | undefined
declare const Deno:
  | {
      memoryUsage?: () => { rss: number; heapUsed: number }
      [key: symbol]: unknown
    }
  | undefined
declare const process:
  | {
      versions?: { node?: string; bun?: string }
      memoryUsage?: () => { rss: number; heapUsed: number }
    }
  | undefined

const runtime = typeof Bun !== 'undefined' && Bun ? 'bun' : typeof Deno !== 'undefined' && Deno ? 'deno' : 'node'

const getDenoInternalCore = () => {
  if (typeof Deno === 'undefined' || !Deno) return null
  try {
    const sym = Object.getOwnPropertySymbols(Deno).find((key) => String(key).includes('Deno.internal'))
    if (!sym) return null
    const denoWithInternal = Deno as unknown as Record<symbol, { core?: Record<string, (...args: unknown[]) => unknown> }>
    return denoWithInternal[sym]?.core ?? null
  } catch {
    return null
  }
}

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
  const denoCore = getDenoInternalCore()

  for (let i = 0; i < 16; i += 1) {
    try {
      maybeGc?.()
    } catch {
      // ignored on runtimes without exposed V8 GC.
    }
    try {
      if (typeof Bun !== 'undefined' && Bun && typeof Bun.gc === 'function') {
        Bun.gc(true)
      }
    } catch {
      // ignored on non-Bun runtimes.
    }
    try {
      denoCore?.runImmediateCallbacks?.()
    } catch {
      // best effort only
    }
    try {
      denoCore?.eventLoopTick?.()
    } catch {
      // best effort only
    }
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
      enforceNonPositiveHeapDeltaAfterGc: true,
    },
  })

  console.log(`[${runtime}] runtime perf harness complete`)
}

await main()
