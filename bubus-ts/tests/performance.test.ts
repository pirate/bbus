import assert from "node:assert/strict";
import { test } from "node:test";

import { BaseEvent, EventBus } from "../src/index.js";

const SimpleEvent = BaseEvent.extend("SimpleEvent", {});

test(
  "processes 20k events within reasonable time",
  { timeout: 120_000 },
  async () => {
    const bus = new EventBus("PerfBus", { max_history_size: 1000 });

    let processed_count = 0;
    bus.on(SimpleEvent, () => {
      processed_count += 1;
    });

    const total_events = 20_000;
    const start = Date.now();

    const pending: Array<ReturnType<typeof SimpleEvent>> = [];
    for (let i = 0; i < total_events; i += 1) {
      pending.push(bus.dispatch(SimpleEvent({})));
    }

    await Promise.all(pending.map((event) => event.done()));
    await bus.waitUntilIdle();

    const duration_ms = Date.now() - start;

    assert.equal(processed_count, total_events);
    assert.ok(duration_ms < 120_000, `Processing took ${duration_ms}ms`);
    assert.ok(bus.event_history.size <= bus.max_history_size);
  }
);
