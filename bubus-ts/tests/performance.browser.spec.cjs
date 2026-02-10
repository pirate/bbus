const fs = require('fs')
const path = require('path')
const { test, expect } = require('playwright/test')

test.describe('browser runtime perf', () => {
  test.setTimeout(120_000)

  test('runs shared perf scenarios in Chromium JS runtime', async ({ page, browserName }) => {
    expect(browserName).toBe('chromium')

    const distCode = fs.readFileSync(path.resolve(__dirname, '../dist/esm/index.js'), 'utf8')
    const scenariosCode = fs.readFileSync(path.resolve(__dirname, './performance.scenarios.js'), 'utf8')

    const result = await page.evaluate(
      async ({ distCode, scenariosCode }) => {
        const importFromCode = async (code) => {
          const url = URL.createObjectURL(new Blob([code], { type: 'text/javascript' }))
          try {
            return await import(url)
          } finally {
            URL.revokeObjectURL(url)
          }
        }

        const [api, scenarios] = await Promise.all([importFromCode(distCode), importFromCode(scenariosCode)])
        const logs = []

        const results = await scenarios.runAllPerfScenarios({
          runtimeName: 'chromium-js',
          api: {
            BaseEvent: api.BaseEvent,
            EventBus: api.EventBus,
            EventHandlerTimeoutError: api.EventHandlerTimeoutError,
            EventHandlerCancelledError: api.EventHandlerCancelledError,
          },
          now: () => performance.now(),
          sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
          log: (message) => logs.push(message),
          limits: {
            singleRunMs: 30_000,
            worstCaseMs: 60_000,
            // Browsers don't expose stable heap APIs for this benchmark.
            worstCaseMemoryDeltaMb: null,
          },
        })

        return { logs, results }
      },
      { distCode, scenariosCode }
    )

    for (const line of result.logs) {
      console.log(line)
    }

    expect(result.results.length).toBe(6)
  })
})
