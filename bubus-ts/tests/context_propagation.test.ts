import assert from "node:assert/strict";
import { test } from "node:test";

import { BaseEvent, EventBus } from "../src/index.js";
import { async_local_storage, hasAsyncLocalStorage } from "../src/async_context.js";

type ContextStore = {
  request_id?: string;
  user_id?: string;
  trace_id?: string;
};

const SimpleEvent = BaseEvent.extend("SimpleEvent", {});
const ChildEvent = BaseEvent.extend("ChildEvent", {});

const skip_if_no_async_local_storage = !hasAsyncLocalStorage();

const require_async_local_storage = () => {
  assert.ok(async_local_storage, "AsyncLocalStorage not available");
  return async_local_storage;
};

const get_store = (store: ContextStore | undefined | null): ContextStore => store ?? {};

test(
  "context propagates to handler",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("ContextTestBus");
    const captured_values: ContextStore = {};
    const storage = require_async_local_storage();

    bus.on(SimpleEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_values.request_id = store?.request_id;
      captured_values.user_id = store?.user_id;
    });

    await storage.run(
      { request_id: "req-12345", user_id: "user-abc" },
      async () => {
        const event = bus.dispatch(SimpleEvent({}));
        await event.done();
      }
    );

    assert.equal(captured_values.request_id, "req-12345");
    assert.equal(captured_values.user_id, "user-abc");
  }
);

test(
  "context propagates through nested handlers",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("NestedContextBus");
    const captured_parent: ContextStore = {};
    const captured_child: ContextStore = {};
    const storage = require_async_local_storage();

    bus.on(SimpleEvent, async (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_parent.request_id = store?.request_id;
      captured_parent.trace_id = store?.trace_id;

      const child = event.bus?.dispatch(ChildEvent({}));
      if (child) {
        await child.done();
      }
    });

    bus.on(ChildEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_child.request_id = store?.request_id;
      captured_child.trace_id = store?.trace_id;
    });

    await storage.run(
      { request_id: "req-nested-123", trace_id: "trace-xyz" },
      async () => {
        const event = bus.dispatch(SimpleEvent({}));
        await event.done();
      }
    );

    assert.equal(captured_parent.request_id, "req-nested-123");
    assert.equal(captured_parent.trace_id, "trace-xyz");
    assert.equal(captured_child.request_id, "req-nested-123");
    assert.equal(captured_child.trace_id, "trace-xyz");
  }
);

test(
  "context isolation between dispatches",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("IsolationTestBus");
    const captured_values: string[] = [];
    const storage = require_async_local_storage();

    bus.on(SimpleEvent, async () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_values.push(store?.request_id ?? "<unset>");
    });

    const event_a = storage.run({ request_id: "req-A" }, () => bus.dispatch(SimpleEvent({})));
    const event_b = storage.run({ request_id: "req-B" }, () => bus.dispatch(SimpleEvent({})));

    await event_a.done();
    await event_b.done();

    assert.ok(captured_values.includes("req-A"));
    assert.ok(captured_values.includes("req-B"));
  }
);

test(
  "context propagates to multiple handlers",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("ParallelContextBus");
    const captured_values: string[] = [];
    const storage = require_async_local_storage();

    bus.on(SimpleEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_values.push(`h1:${store?.request_id ?? "<unset>"}`);
    });

    bus.on(SimpleEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_values.push(`h2:${store?.request_id ?? "<unset>"}`);
    });

    await storage.run({ request_id: "req-parallel" }, async () => {
      const event = bus.dispatch(SimpleEvent({}));
      await event.done();
    });

    assert.ok(captured_values.includes("h1:req-parallel"));
    assert.ok(captured_values.includes("h2:req-parallel"));
  }
);

test(
  "context propagates through event forwarding",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus_a = new EventBus("BusA");
    const bus_b = new EventBus("BusB");
    const captured_bus_a: ContextStore = {};
    const captured_bus_b: ContextStore = {};
    const storage = require_async_local_storage();

    bus_a.on(SimpleEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_bus_a.request_id = store?.request_id;
    });

    bus_b.on(SimpleEvent, () => {
      const store = storage.getStore() as ContextStore | undefined;
      captured_bus_b.request_id = store?.request_id;
    });

    bus_a.on("*", bus_b.dispatch);

    await storage.run({ request_id: "req-forwarded" }, async () => {
      const event = bus_a.dispatch(SimpleEvent({}));
      await event.done();
      await bus_b.waitUntilIdle();
    });

    assert.equal(captured_bus_a.request_id, "req-forwarded");
    assert.equal(captured_bus_b.request_id, "req-forwarded");
  }
);

test(
  "handler can modify context without affecting parent",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("ModifyContextBus");
    const storage = require_async_local_storage();
    let parent_value_after_child = "";

    bus.on(SimpleEvent, async (event) => {
      if (!storage.enterWith) {
        throw new Error("AsyncLocalStorage.enterWith is required for this test");
      }
      storage.enterWith({ request_id: "parent-value" });
      const child = event.bus?.dispatch(ChildEvent({}));
      if (child) {
        await child.done();
      }
      const store = get_store(storage.getStore() as ContextStore | undefined);
      parent_value_after_child = store.request_id ?? "<unset>";
    });

    bus.on(ChildEvent, () => {
      if (!storage.enterWith) {
        throw new Error("AsyncLocalStorage.enterWith is required for this test");
      }
      storage.enterWith({ request_id: "child-modified" });
    });

    await storage.run({}, async () => {
      const event = bus.dispatch(SimpleEvent({}));
      await event.done();
    });

    assert.equal(parent_value_after_child, "parent-value");
  }
);

test(
  "event parent_id tracking still works with context propagation",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("ParentIdTrackingBus");
    const storage = require_async_local_storage();
    let parent_event_id: string | undefined;
    let child_event_parent_id: string | undefined;

    bus.on(SimpleEvent, async (event) => {
      parent_event_id = event.event_id;
      const child = event.bus?.dispatch(ChildEvent({}));
      if (child) {
        await child.done();
      }
    });

    bus.on(ChildEvent, (event) => {
      child_event_parent_id = event.event_parent_id;
    });

    await storage.run({ request_id: "req-parent-tracking" }, async () => {
      const event = bus.dispatch(SimpleEvent({}));
      await event.done();
    });

    assert.ok(parent_event_id);
    assert.ok(child_event_parent_id);
    assert.equal(child_event_parent_id, parent_event_id);
  }
);

test(
  "dispatch context and parent_id both work together",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("CombinedContextBus");
    const storage = require_async_local_storage();
    const results: Record<string, string | undefined> = {};

    bus.on(SimpleEvent, async (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      results.parent_request_id = store?.request_id;
      results.parent_event_id = event.event_id;
      const child = event.bus?.dispatch(ChildEvent({}));
      if (child) {
        await child.done();
      }
    });

    bus.on(ChildEvent, (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      results.child_request_id = store?.request_id;
      results.child_event_parent_id = event.event_parent_id;
    });

    await storage.run({ request_id: "req-combined-test" }, async () => {
      const event = bus.dispatch(SimpleEvent({}));
      await event.done();
    });

    assert.equal(results.parent_request_id, "req-combined-test");
    assert.equal(results.child_request_id, "req-combined-test");
    assert.equal(results.child_event_parent_id, results.parent_event_id);
  }
);

test(
  "deeply nested context and parent tracking",
  { skip: skip_if_no_async_local_storage },
  async () => {
    const bus = new EventBus("DeepNestingBus");
    const storage = require_async_local_storage();
    const results: Array<{
      level: number;
      request_id?: string;
      event_id: string;
      parent_id?: string;
    }> = [];

    const Level2Event = BaseEvent.extend("Level2Event", {});
    const Level3Event = BaseEvent.extend("Level3Event", {});

    bus.on(SimpleEvent, async (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      results.push({
        level: 1,
        request_id: store?.request_id,
        event_id: event.event_id,
        parent_id: event.event_parent_id
      });
      const child = event.bus?.dispatch(Level2Event({}));
      if (child) {
        await child.done();
      }
    });

    bus.on(Level2Event, async (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      results.push({
        level: 2,
        request_id: store?.request_id,
        event_id: event.event_id,
        parent_id: event.event_parent_id
      });
      const child = event.bus?.dispatch(Level3Event({}));
      if (child) {
        await child.done();
      }
    });

    bus.on(Level3Event, (event) => {
      const store = storage.getStore() as ContextStore | undefined;
      results.push({
        level: 3,
        request_id: store?.request_id,
        event_id: event.event_id,
        parent_id: event.event_parent_id
      });
    });

    await storage.run({ request_id: "req-deep-nesting" }, async () => {
      const event = bus.dispatch(SimpleEvent({}));
      await event.done();
    });

    assert.equal(results.length, 3);
    for (const result of results) {
      assert.equal(result.request_id, "req-deep-nesting");
    }
    assert.equal(results[0].parent_id, undefined);
    assert.equal(results[1].parent_id, results[0].event_id);
    assert.equal(results[2].parent_id, results[1].event_id);
  }
);
