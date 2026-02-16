export async function withTimeout<T>(timeout_seconds: number | null, on_timeout: () => Error, fn: () => Promise<T>): Promise<T> {
  const task = Promise.resolve().then(fn)
  if (timeout_seconds === null) {
    return await task
  }
  const timeout_ms = timeout_seconds * 1000
  return await new Promise<T>((resolve, reject) => {
    let settled = false
    const finish_resolve = (value: T) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      resolve(value)
    }
    const finish_reject = (error: unknown) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      reject(error)
    }
    const timer = setTimeout(() => {
      if (settled) {
        return
      }
      settled = true
      reject(on_timeout())
      void task.catch(() => undefined)
    }, timeout_ms)
    task.then(finish_resolve).catch(finish_reject)
  })
}

export async function withSlowMonitor<T>(slow_timer: ReturnType<typeof setTimeout> | null, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } finally {
    if (slow_timer) {
      clearTimeout(slow_timer)
    }
  }
}

export async function runWithAbortMonitor<T>(fn: () => T | Promise<T>, abort_signal: Promise<never>): Promise<T> {
  return await Promise.race([Promise.resolve().then(fn), abort_signal])
}
