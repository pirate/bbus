import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerTimeoutError } from '../src/index.js'
import { runPerf50kEvents, runPerfEphemeralBuses, runPerfOnOffChurn, runPerfWorstCase } from './performance.scenarios.js'

const nodePerfInput = {
  runtimeName: 'node:test',
  api: { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError },
  now: () => performance.now(),
  sleep: (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
  log: (message: string) => console.log(message),
  forceGc: () => global.gc?.(),
  getMemoryUsage: () => process.memoryUsage(),
  limits: {
    singleRunMs: 30_000,
    worstCaseMs: 60_000,
    // Keep the original stricter leak budget for node:test.
    worstCaseMemoryDeltaMb: 50,
  },
}

test('processes 50k events within reasonable time', { timeout: 30_000 }, async () => {
  const result = await runPerf50kEvents(nodePerfInput)
  assert.equal(result.scenario, '50k events')
})

test('500 ephemeral buses with 100 events each', { timeout: 30_000 }, async () => {
  const result = await runPerfEphemeralBuses(nodePerfInput)
  assert.equal(result.scenario, '500 buses x 100 events')
})

test('50k events with ephemeral on/off handler registration across 2 buses', { timeout: 30_000 }, async () => {
  const result = await runPerfOnOffChurn(nodePerfInput)
  assert.equal(result.scenario, '50k on/off handler churn')
})

test('worst-case: forwarding + queue-jump + timeouts + cancellation at scale', { timeout: 60_000 }, async () => {
  const result = await runPerfWorstCase(nodePerfInput)
  assert.equal(result.scenario, 'worst-case forwarding + timeouts')
})
