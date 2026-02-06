import assert from "node:assert/strict";
import { test } from "node:test";

import {
  BaseEvent,
  EventBus,
  EventHandlerCancelledError,
  EventHandlerTimeoutError
} from "../src/index.js";

const TimeoutEvent = BaseEvent.extend("TimeoutEvent", {});

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("handler timeout marks EventResult as error", async () => {
  const bus = new EventBus("TimeoutBus");

  bus.on(TimeoutEvent, async () => {
    await delay(50);
    return "slow";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "error");
  assert.ok(result.error instanceof EventHandlerTimeoutError);
});

test("handler completes within timeout", async () => {
  const bus = new EventBus("TimeoutOkBus");

  bus.on(TimeoutEvent, async () => {
    await delay(5);
    return "fast";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.equal(result.result, "fast");
});

test("handler timeouts fire across concurrency modes", async () => {
  const modes = ["global-serial", "bus-serial", "parallel"] as const;

  for (const event_mode of modes) {
    for (const handler_mode of modes) {
      const bus = new EventBus(`Timeout-${event_mode}-${handler_mode}`, {
        event_concurrency: event_mode,
        handler_concurrency: handler_mode
      });

      bus.on(TimeoutEvent, async () => {
        await delay(50);
        return "slow";
      });

      const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
      await event.done();

      const result = Array.from(event.event_results.values())[0];
      assert.equal(
        result.status,
        "error",
        `Expected timeout error for event=${event_mode} handler=${handler_mode}`
      );
      assert.ok(
        result.error instanceof EventHandlerTimeoutError,
        `Expected EventHandlerTimeoutError for event=${event_mode} handler=${handler_mode}`
      );

      await bus.waitUntilIdle();
    }
  }
});

test("timeout still marks event failed when other handlers finish", async () => {
  const bus = new EventBus("TimeoutParallelHandlers", {
    event_concurrency: "parallel",
    handler_concurrency: "parallel"
  });

  const results: string[] = [];

  bus.on(TimeoutEvent, async () => {
    await delay(1);
    results.push("fast");
    return "fast";
  });

  bus.on(TimeoutEvent, async () => {
    await delay(50);
    results.push("slow");
    return "slow";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
  await event.done();

  const statuses = Array.from(event.event_results.values()).map((result) => result.status);
  assert.ok(statuses.includes("completed"));
  assert.ok(statuses.includes("error"));
  assert.equal(event.event_status, "completed");
  assert.ok(event.event_errors.length > 0);
  assert.ok(results.includes("fast"));
});

test("deadlock warning triggers when event exceeds timeout", async () => {
  const bus = new EventBus("DeadlockWarnBus");
  const warnings: string[] = [];
  const original_warn = console.warn;
  console.warn = (message?: unknown, ...args: unknown[]) => {
    warnings.push(String(message));
    if (args.length > 0) {
      warnings.push(args.map(String).join(" "));
    }
  };

  try {
    bus.on(TimeoutEvent, async () => {
      await new Promise(() => {
        // never resolve
      });
    });

    const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
    await event.done();
  } finally {
    console.warn = original_warn;
  }

  assert.ok(
    warnings.some((message) => message.includes("Possible deadlock")),
    "Expected deadlock warning"
  );
});

test("slow handler warning fires when handler runs long", async () => {
  const bus = new EventBus("SlowHandlerWarnBus");
  const warnings: string[] = [];
  const original_warn = console.warn;
  const original_set_timeout = global.setTimeout;
  const original_clear_timeout = global.clearTimeout;

  console.warn = (message?: unknown, ...args: unknown[]) => {
    warnings.push(String(message));
    if (args.length > 0) {
      warnings.push(args.map(String).join(" "));
    }
  };

  // Force the slow-handler warning timer to fire immediately
  global.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
    if (delay === 15000) {
      return original_set_timeout(callback, 0, ...args);
    }
    return original_set_timeout(callback, delay as number, ...args);
  }) as typeof setTimeout;

  global.clearTimeout = ((timeout: ReturnType<typeof setTimeout>) => {
    return original_clear_timeout(timeout);
  }) as typeof clearTimeout;

  try {
    bus.on(TimeoutEvent, async () => {
      await delay(5);
      return "ok";
    });

    const event = bus.dispatch(TimeoutEvent({ event_timeout: null }));
    await event.done();
  } finally {
    console.warn = original_warn;
    global.setTimeout = original_set_timeout;
    global.clearTimeout = original_clear_timeout;
  }

  assert.ok(
    warnings.some((message) => message.includes("Slow handler")),
    "Expected slow handler warning"
  );
});

test("event-level concurrency overrides do not bypass timeouts", async () => {
  const bus = new EventBus("TimeoutEventOverrideBus", {
    event_concurrency: "global-serial",
    handler_concurrency: "global-serial"
  });

  bus.on(TimeoutEvent, async () => {
    await delay(50);
    return "slow";
  });

  const event = bus.dispatch(
    TimeoutEvent({
      event_timeout: 0.01,
      event_concurrency: "parallel",
      handler_concurrency: "parallel"
    })
  );
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "error");
  assert.ok(result.error instanceof EventHandlerTimeoutError);
});

test("handler-level concurrency overrides do not bypass timeouts", async () => {
  const bus = new EventBus("TimeoutHandlerOverrideBus", {
    event_concurrency: "parallel",
    handler_concurrency: "global-serial"
  });

  const order: string[] = [];

  bus.on(
    TimeoutEvent,
    async () => {
      order.push("slow_start");
      await delay(50);
      order.push("slow_end");
      return "slow";
    },
    { handler_concurrency: "bus-serial" }
  );

  bus.on(
    TimeoutEvent,
    async () => {
      order.push("fast_start");
      await delay(1);
      order.push("fast_end");
      return "fast";
    },
    { handler_concurrency: "parallel" }
  );

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
  await event.done();

  const statuses = Array.from(event.event_results.values()).map((result) => result.status);
  assert.ok(statuses.includes("error"));
  assert.ok(statuses.includes("completed"));
  assert.ok(order.includes("fast_start"));
});

test("forwarded event timeouts apply across buses", async () => {
  const bus_a = new EventBus("TimeoutForwardA", { event_concurrency: "bus-serial" });
  const bus_b = new EventBus("TimeoutForwardB", { event_concurrency: "bus-serial" });

  bus_a.on(TimeoutEvent, async (event) => {
    bus_b.dispatch(event);
  });

  bus_b.on(TimeoutEvent, async () => {
    await delay(50);
    return "slow";
  });

  const event = bus_a.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
  await event.done();

  const results = Array.from(event.event_results.values());
  const bus_b_result = results.find((result) => result.eventbus_name === "TimeoutForwardB");
  assert.ok(bus_b_result);
  assert.equal(bus_b_result?.status, "error");
  assert.ok(bus_b_result?.error instanceof EventHandlerTimeoutError);
});

test("queue-jump awaited child timeouts still fire across buses", async () => {
  const ParentEvent = BaseEvent.extend("TimeoutParentEvent", {});
  const ChildEvent = BaseEvent.extend("TimeoutChildEvent", {});

  const bus_a = new EventBus("TimeoutQueueJumpA", { event_concurrency: "global-serial" });
  const bus_b = new EventBus("TimeoutQueueJumpB", { event_concurrency: "global-serial" });

  let child_ref: InstanceType<typeof ChildEvent> | null = null;

  bus_b.on(ChildEvent, async () => {
    await delay(50);
    return "slow";
  });

  bus_a.on(ParentEvent, async () => {
    const child = bus_b.dispatch(ChildEvent({ event_timeout: 0.01 }));
    child_ref = child;
    await child.done();
  });

  const parent = bus_a.dispatch(ParentEvent({ event_timeout: 0.5 }));
  await parent.done();

  assert.ok(child_ref);
  const child_results = Array.from(child_ref!.event_results.values());
  const timeout_result = child_results.find(
    (result) => result.error instanceof EventHandlerTimeoutError
  );
  assert.ok(timeout_result);
});

test("parent timeout cancels pending child handler results under serial handler limiter", async () => {
  const ParentEvent = BaseEvent.extend("TimeoutCancelParentEvent", {});
  const ChildEvent = BaseEvent.extend("TimeoutCancelChildEvent", {});

  const bus = new EventBus("TimeoutCancelBus", {
    event_concurrency: "bus-serial",
    handler_concurrency: "bus-serial"
  });

  let child_runs = 0;

  bus.on(ChildEvent, async () => {
    child_runs += 1;
    await delay(30);
    return "first";
  });

  bus.on(ChildEvent, async () => {
    child_runs += 1;
    await delay(10);
    return "second";
  });

  bus.on(ParentEvent, async (event) => {
    event.bus?.emit(ChildEvent({ event_timeout: 0.2 }));
    await delay(50);
  });

  const parent = bus.dispatch(ParentEvent({ event_timeout: 0.01 }));
  await parent.done();
  await bus.waitUntilIdle();

  const child = parent.event_children[0];
  assert.ok(child);

  assert.equal(child_runs, 0);

  const cancelled_results = Array.from(child.event_results.values()).filter(
    (result) => result.error instanceof EventHandlerCancelledError
  );
  assert.ok(cancelled_results.length > 0);
});

test("event_timeout null falls back to bus default", async () => {
  const bus = new EventBus("TimeoutDefaultBus", { event_timeout: 0.01 });

  bus.on(TimeoutEvent, async () => {
    await delay(50);
    return "slow";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: null }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "error");
  assert.ok(result.error instanceof EventHandlerTimeoutError);
});

test("bus default null disables timeouts when event_timeout is null", async () => {
  const bus = new EventBus("TimeoutDisabledBus", { event_timeout: null });

  bus.on(TimeoutEvent, async () => {
    await delay(20);
    return "ok";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: null }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.equal(result.result, "ok");
});

test("multi-level timeout cascade with mixed cancellations", async () => {
  const TopEvent = BaseEvent.extend("TimeoutCascadeTop", {});
  const QueuedChildEvent = BaseEvent.extend("TimeoutCascadeQueuedChild", {});
  const AwaitedChildEvent = BaseEvent.extend("TimeoutCascadeAwaitedChild", {});
  const ImmediateGrandchildEvent = BaseEvent.extend("TimeoutCascadeImmediateGrandchild", {});
  const QueuedGrandchildEvent = BaseEvent.extend("TimeoutCascadeQueuedGrandchild", {});

  const bus = new EventBus("TimeoutCascadeBus", {
    event_concurrency: "bus-serial",
    handler_concurrency: "bus-serial"
  });

  let queued_child: InstanceType<typeof QueuedChildEvent> | null = null;
  let awaited_child: InstanceType<typeof AwaitedChildEvent> | null = null;
  let immediate_grandchild: InstanceType<typeof ImmediateGrandchildEvent> | null = null;
  let queued_grandchild: InstanceType<typeof QueuedGrandchildEvent> | null = null;

  let queued_child_runs = 0;
  let immediate_grandchild_runs = 0;
  let queued_grandchild_runs = 0;

  const queued_child_fast = async () => {
    queued_child_runs += 1;
    await delay(5);
    return "queued_fast";
  };

  const queued_child_slow = async () => {
    queued_child_runs += 1;
    await delay(50);
    return "queued_slow";
  };

  const awaited_child_fast = async () => {
    await delay(5);
    return "awaited_fast";
  };

  const awaited_child_slow = async (event: BaseEvent) => {
    queued_grandchild = event.bus?.emit(
      QueuedGrandchildEvent({ event_timeout: 0.2 })
    )!;
    immediate_grandchild = event.bus?.emit(
      ImmediateGrandchildEvent({ event_timeout: 0.2 })
    )!;
    await immediate_grandchild.done();
    await delay(100);
    return "awaited_slow";
  };

  const immediate_grandchild_slow = async () => {
    immediate_grandchild_runs += 1;
    await delay(50);
    return "immediate_grandchild_slow";
  };

  const immediate_grandchild_fast = async () => {
    immediate_grandchild_runs += 1;
    await delay(10);
    return "immediate_grandchild_fast";
  };

  const queued_grandchild_slow = async () => {
    queued_grandchild_runs += 1;
    await delay(50);
    return "queued_grandchild_slow";
  };

  const queued_grandchild_fast = async () => {
    queued_grandchild_runs += 1;
    await delay(10);
    return "queued_grandchild_fast";
  };

  bus.on(QueuedChildEvent, queued_child_fast);
  bus.on(QueuedChildEvent, queued_child_slow);
  bus.on(AwaitedChildEvent, awaited_child_fast);
  bus.on(AwaitedChildEvent, awaited_child_slow);
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_slow);
  bus.on(ImmediateGrandchildEvent, immediate_grandchild_fast);
  bus.on(QueuedGrandchildEvent, queued_grandchild_slow);
  bus.on(QueuedGrandchildEvent, queued_grandchild_fast);

  bus.on(TopEvent, async (event) => {
    queued_child = event.bus?.emit(QueuedChildEvent({ event_timeout: 0.2 }))!;
    awaited_child = event.bus?.emit(AwaitedChildEvent({ event_timeout: 0.03 }))!;
    await awaited_child.done();
    await delay(80);
  });

  const top = bus.dispatch(TopEvent({ event_timeout: 0.04 }));
  await top.done();
  await bus.waitUntilIdle();

  const top_result = Array.from(top.event_results.values())[0];
  assert.equal(top_result.status, "error");
  assert.ok(top_result.error instanceof EventHandlerTimeoutError);

  assert.ok(queued_child);
  const queued_results = Array.from(queued_child!.event_results.values());
  assert.equal(queued_child_runs, 0);
  assert.ok(queued_results.length >= 2);
  for (const result of queued_results) {
    assert.equal(result.status, "error");
    assert.ok(result.error instanceof EventHandlerCancelledError);
    assert.ok(
      (result.error as EventHandlerCancelledError).parent_error instanceof EventHandlerTimeoutError
    );
  }

  assert.ok(awaited_child);
  const awaited_results = Array.from(awaited_child!.event_results.values());
  const awaited_completed = awaited_results.filter((result) => result.status === "completed");
  const awaited_timeouts = awaited_results.filter(
    (result) => result.error instanceof EventHandlerTimeoutError
  );
  assert.equal(awaited_completed.length, 1);
  assert.equal(awaited_timeouts.length, 1);

  assert.ok(immediate_grandchild);
  const immediate_results = Array.from(immediate_grandchild!.event_results.values());
  assert.equal(immediate_grandchild_runs, 2);
  const immediate_completed = immediate_results.filter((result) => result.status === "completed");
  assert.equal(immediate_completed.length, 2);

  assert.ok(queued_grandchild);
  const queued_grandchild_results = Array.from(queued_grandchild!.event_results.values());
  assert.equal(queued_grandchild_runs, 0);
  const queued_cancelled = queued_grandchild_results.filter(
    (result) => result.error instanceof EventHandlerCancelledError
  );
  assert.ok(queued_cancelled.length >= 2);
});
