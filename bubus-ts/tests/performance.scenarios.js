const defaultNow = () => performance.now()
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1)
const kb = (bytes) => bytes / 1024
const clampNonNegative = (value) => (value < 0 ? 0 : value)
const formatMsPerEvent = (value) => `${value.toFixed(3)}ms/event`
const formatKbPerEvent = (value) => `${value.toFixed(3)}kb/event`
const formatMs = (value) => `${value.toFixed(3)}ms`

const measureMemory = (hooks) => {
  if (typeof hooks.getMemoryUsage !== 'function') {
    return null
  }
  return hooks.getMemoryUsage()
}

const maybeForceGc = (hooks) => {
  if (typeof hooks.forceGc === 'function') {
    hooks.forceGc()
  }
}

const measureHeapDeltaAfterGc = async (hooks, baseline) => {
  if (!baseline) return null
  // Let pending microtasks settle, then force GC multiple times for a stable end snapshot.
  await hooks.sleep(10)
  maybeForceGc(hooks)
  await hooks.sleep(10)
  maybeForceGc(hooks)
  const end = measureMemory(hooks)
  if (!end) return null
  return (end.heapUsed - baseline.heapUsed) / 1024 / 1024
}

const createMemoryTracker = (hooks) => {
  const baselineRaw = measureMemory(hooks)
  if (!baselineRaw) {
    return {
      baseline: null,
      peak: null,
      sample: () => null,
      peakRssKbPerEvent: () => null,
    }
  }

  const baseline = { rss: baselineRaw.rss, heapUsed: baselineRaw.heapUsed }
  const peak = { rss: baselineRaw.rss, heapUsed: baselineRaw.heapUsed }

  const sample = () => {
    const current = measureMemory(hooks)
    if (!current) return null
    if (current.rss > peak.rss) peak.rss = current.rss
    if (current.heapUsed > peak.heapUsed) peak.heapUsed = current.heapUsed
    return current
  }

  const peakRssKbPerEvent = (events) => {
    if (!events || !baseline) return null
    const deltaBytes = clampNonNegative(peak.rss - baseline.rss)
    return kb(deltaBytes) / events
  }

  return { baseline, peak, sample, peakRssKbPerEvent }
}

const record = (hooks, name, metrics) => {
  if (typeof hooks.log === 'function') {
    const perEventOnly = name === 'worst-case forwarding + timeouts'
    const parts = []
    if (!perEventOnly && typeof metrics.totalEvents === 'number') parts.push(`events=${metrics.totalEvents}`)
    if (!perEventOnly && typeof metrics.totalMs === 'number') parts.push(`total=${formatMs(metrics.totalMs)}`)
    if (typeof metrics.msPerEvent === 'number') parts.push(`latency=${formatMsPerEvent(metrics.msPerEvent)}`)
    if (typeof metrics.ramKbPerEvent === 'number') parts.push(`ram=${formatKbPerEvent(metrics.ramKbPerEvent)}`)
    if (typeof metrics.throughput === 'number') parts.push(`throughput=${metrics.throughput}/s`)
    if (typeof metrics.timeoutCount === 'number') parts.push(`timeouts=${metrics.timeoutCount}`)
    if (typeof metrics.cancelCount === 'number') parts.push(`cancels=${metrics.cancelCount}`)
    if (typeof metrics.heapDeltaGcMb === 'number') parts.push(`heap_delta_gc=${metrics.heapDeltaGcMb.toFixed(3)}mb`)
    hooks.log(`[${hooks.runtimeName}] ${name}: ${parts.join(' ')}`)
  }
}

const withDefaults = (input) => {
  const hooks = {
    runtimeName: input.runtimeName ?? 'runtime',
    now: input.now ?? defaultNow,
    sleep: input.sleep ?? defaultSleep,
    log: input.log ?? (() => {}),
    forceGc: input.forceGc,
    getMemoryUsage: input.getMemoryUsage,
    limits: {
      singleRunMs: input.limits?.singleRunMs ?? 30_000,
      worstCaseMs: input.limits?.worstCaseMs ?? 60_000,
      worstCaseMemoryDeltaMb: input.limits?.worstCaseMemoryDeltaMb ?? null,
      enforceNonPositiveHeapDeltaAfterGc: input.limits?.enforceNonPositiveHeapDeltaAfterGc ?? true,
    },
    api: input.api,
  }
  return hooks
}

export const runPerf50kEvents = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalEvents = 50_000
  const SimpleEvent = BaseEvent.extend('PerfSimpleEvent', {})
  const bus = new EventBus('PerfBus', { max_history_size: totalEvents })

  let processedCount = 0
  bus.on(SimpleEvent, () => {
    processedCount += 1
  })

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  const pending = []
  for (let i = 0; i < totalEvents; i += 1) {
    pending.push(bus.dispatch(SimpleEvent({})))
    if (i % 1000 === 0) memory.sample()
  }

  const tDispatch = hooks.now()
  memory.sample()
  await Promise.all(pending.map((event) => event.done()))
  await bus.waitUntilIdle()
  // Drop strong references before measuring post-GC leak delta.
  pending.length = 0
  const tDone = hooks.now()
  memory.sample()
  const memDone = measureMemory(hooks)
  maybeForceGc(hooks)
  const memGc = measureMemory(hooks)

  const dispatchMs = tDispatch - t0
  const awaitMs = tDone - tDispatch
  const totalMs = tDone - t0
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `50k events processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k events took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(
    bus.event_history.size <= bus.max_history_size,
    `50k events history exceeded limit: ${bus.event_history.size}/${bus.max_history_size}`
  )

  const result = {
    scenario: '50k events',
    totalEvents,
    totalMs,
    dispatchMs,
    awaitMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
  }

  if (memory.baseline && memDone && memGc) {
    result.heapBeforeMb = Number(mb(memory.baseline.heapUsed))
    result.heapDoneMb = Number(mb(memDone.heapUsed))
    result.heapGcMb = Number(mb(memGc.heapUsed))
    result.rssBeforeMb = Number(mb(memory.baseline.rss))
    result.rssDoneMb = Number(mb(memDone.rss))
    result.rssPeakMb = Number(mb(memory.peak.rss))
  }

  bus.destroy()
  const heapDeltaGcMb = await measureHeapDeltaAfterGc(hooks, memory.baseline)
  result.heapDeltaGcMb = heapDeltaGcMb
  if (hooks.limits.enforceNonPositiveHeapDeltaAfterGc && typeof heapDeltaGcMb === 'number') {
    assert(heapDeltaGcMb <= 0, `50k events heap delta after GC is positive: ${heapDeltaGcMb.toFixed(3)}MB`)
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfEphemeralBuses = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalBuses = 500
  const eventsPerBus = 100
  const totalEvents = totalBuses * eventsPerBus
  const SimpleEvent = BaseEvent.extend('PerfSimpleEvent', {})

  let processedCount = 0
  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let b = 0; b < totalBuses; b += 1) {
    const bus = new EventBus(`ReqBus-${b}`, { max_history_size: eventsPerBus })
    bus.on(SimpleEvent, () => {
      processedCount += 1
    })

    const pending = []
    for (let i = 0; i < eventsPerBus; i += 1) {
      pending.push(bus.dispatch(SimpleEvent({})))
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
    bus.destroy()
    if (b % 10 === 0) memory.sample()
  }

  const totalMs = hooks.now() - t0
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `500x100 buses processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `500x100 buses took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(EventBus._all_instances.size === 0, `500x100 buses leaked instances: ${EventBus._all_instances.size}`)

  const heapDeltaGcMb = await measureHeapDeltaAfterGc(hooks, memory.baseline)
  if (hooks.limits.enforceNonPositiveHeapDeltaAfterGc && typeof heapDeltaGcMb === 'number') {
    assert(heapDeltaGcMb <= 0, `500x100 buses heap delta after GC is positive: ${heapDeltaGcMb.toFixed(3)}MB`)
  }

  const result = {
    scenario: '500 buses x 100 events',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
    heapDeltaGcMb,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfOnOffChurn = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const RequestEvent = BaseEvent.extend('PerfRequestEvent', {})

  const totalEvents = 50_000
  const busA = new EventBus('SharedBusA', { max_history_size: totalEvents })
  const busB = new EventBus('SharedBusB', { max_history_size: totalEvents })

  let processedA = 0
  let processedB = 0

  busB.on(RequestEvent, () => {
    processedB += 1
  })

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()
  for (let i = 0; i < totalEvents; i += 1) {
    const ephemeralHandler = () => {
      processedA += 1
    }
    busA.on(RequestEvent, ephemeralHandler)

    const event = RequestEvent({})
    const evA = busA.dispatch(event)
    busB.dispatch(event)
    await evA.done()

    busA.off(RequestEvent, ephemeralHandler)
    if (i % 1000 === 0) memory.sample()
  }

  await busA.waitUntilIdle()
  await busB.waitUntilIdle()
  const totalMs = hooks.now() - t0
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedA === totalEvents, `50k on/off busA processed ${processedA}/${totalEvents}`)
  assert(processedB === totalEvents, `50k on/off busB processed ${processedB}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k on/off took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(busA.handlers.size === 0, `50k on/off leaked busA handlers: ${busA.handlers.size}`)
  assert(busB.handlers.size === 1, `50k on/off busB handlers expected 1, got ${busB.handlers.size}`)

  busA.destroy()
  busB.destroy()
  const heapDeltaGcMb = await measureHeapDeltaAfterGc(hooks, memory.baseline)
  if (hooks.limits.enforceNonPositiveHeapDeltaAfterGc && typeof heapDeltaGcMb === 'number') {
    assert(heapDeltaGcMb <= 0, `50k on/off heap delta after GC is positive: ${heapDeltaGcMb.toFixed(3)}MB`)
  }

  const result = {
    scenario: '50k on/off handler churn',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedA,
    processedB,
    heapDeltaGcMb,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfWorstCase = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError } = hooks.api

  const ParentEvent = BaseEvent.extend('WCParent', {})
  const ChildEvent = BaseEvent.extend('WCChild', {})
  const GrandchildEvent = BaseEvent.extend('WCGrandchild', {})

  const totalIterations = 500
  const historyLimit = totalIterations * 2
  const busA = new EventBus('WCA', { max_history_size: historyLimit })
  const busB = new EventBus('WCB', { max_history_size: historyLimit })
  const busC = new EventBus('WCC', { max_history_size: historyLimit })

  let parentHandledA = 0
  let parentHandledB = 0
  let childHandled = 0
  let grandchildHandled = 0
  let timeoutCount = 0
  let cancelCount = 0

  busB.on(ParentEvent, () => {
    parentHandledB += 1
  })

  busC.on(ChildEvent, async (event) => {
    childHandled += 1
    const gc = event.bus.emit(GrandchildEvent({}))
    busC.dispatch(gc)
    await gc.done()
  })

  busC.on(GrandchildEvent, async () => {
    grandchildHandled += 1
    // Deterministically slow path so child timeout iterations reliably trigger.
    await hooks.sleep(20)
  })

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let i = 0; i < totalIterations; i += 1) {
    const shouldTimeout = i % 5 === 0

    const ephemeralHandler = async (event) => {
      parentHandledA += 1
      const child = event.bus.emit(
        ChildEvent({
          event_timeout: shouldTimeout ? 0.005 : null,
        })
      )
      busC.dispatch(child)
      try {
        await child.done()
      } catch {
        // Timeouts are expected for timeout iterations.
      }
    }

    busA.on(ParentEvent, ephemeralHandler)
    const parent = ParentEvent({})
    const evA = busA.dispatch(parent)
    busB.dispatch(parent)
    await evA.done()
    busA.off(ParentEvent, ephemeralHandler)

    if (i % 10 === 0) {
      busA.find(ParentEvent, { future: 0.001 })
    }
    if (i % 5 === 0) memory.sample()
  }

  await busA.waitUntilIdle()
  await busB.waitUntilIdle()
  await busC.waitUntilIdle()
  memory.sample()

  for (const event of busC.event_history.values()) {
    for (const result of event.event_results.values()) {
      if (result.error instanceof EventHandlerTimeoutError) timeoutCount += 1
      if (result.error instanceof EventHandlerCancelledError) cancelCount += 1
    }
  }

  const totalMs = hooks.now() - t0
  const estimatedEvents = totalIterations * 3
  const msPerEvent = totalMs / estimatedEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(estimatedEvents)

  assert(parentHandledA === totalIterations, `worst-case parentA ${parentHandledA}/${totalIterations}`)
  assert(parentHandledB === totalIterations, `worst-case parentB ${parentHandledB}/${totalIterations}`)
  assert(busA.handlers.size === 0, `worst-case leaked busA handlers: ${busA.handlers.size}`)
  assert(busA.event_history.size <= historyLimit, `worst-case busA history ${busA.event_history.size}/${historyLimit}`)
  assert(busB.event_history.size <= historyLimit, `worst-case busB history ${busB.event_history.size}/${historyLimit}`)
  assert(busC.event_history.size <= historyLimit, `worst-case busC history ${busC.event_history.size}/${historyLimit}`)
  assert(totalMs < hooks.limits.worstCaseMs, `worst-case took ${Math.round(totalMs)}ms (limit ${hooks.limits.worstCaseMs}ms)`)

  busA.destroy()
  busB.destroy()
  busC.destroy()
  const heapDeltaGcMb = await measureHeapDeltaAfterGc(hooks, memory.baseline)
  if (hooks.limits.enforceNonPositiveHeapDeltaAfterGc && typeof heapDeltaGcMb === 'number') {
    assert(heapDeltaGcMb <= 0, `worst-case heap delta after GC is positive: ${heapDeltaGcMb.toFixed(3)}MB`)
    if (hooks.limits.worstCaseMemoryDeltaMb !== null) {
      assert(
        heapDeltaGcMb < hooks.limits.worstCaseMemoryDeltaMb,
        `worst-case memory delta after GC was ${heapDeltaGcMb.toFixed(1)}MB (limit ${hooks.limits.worstCaseMemoryDeltaMb}MB)`
      )
    }
  }

  const result = {
    scenario: 'worst-case forwarding + timeouts',
    totalEvents: estimatedEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    parentHandledA,
    parentHandledB,
    childHandled,
    grandchildHandled,
    timeoutCount,
    cancelCount,
    heapDeltaGcMb,
  }
  record(hooks, result.scenario, result)
  assert(EventBus._all_instances.size === 0, `worst-case leaked instances: ${EventBus._all_instances.size}`)

  return result
}

export const runAllPerfScenarios = async (input) => {
  const results = []
  results.push(await runPerf50kEvents(input))
  results.push(await runPerfEphemeralBuses(input))
  results.push(await runPerfOnOffChurn(input))
  results.push(await runPerfWorstCase(input))
  return results
}
