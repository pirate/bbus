import assert from "node:assert/strict";
import { test } from "node:test";

import { BaseEvent, EventBus } from "../src/index.js";

const ParentEvent = BaseEvent.extend("ParentEvent", {});
const ImmediateChildEvent = BaseEvent.extend("ImmediateChildEvent", {});
const QueuedChildEvent = BaseEvent.extend("QueuedChildEvent", {});

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("comprehensive patterns: forwarding, async/sync dispatch, parent tracking", async () => {
  const bus_1 = new EventBus("bus1");
  const bus_2 = new EventBus("bus2");

  const results: Array<[number, string]> = [];
  const execution_counter = { count: 0 };

  const child_bus2_event_handler = (event: BaseEvent): string => {
    execution_counter.count += 1;
    const seq = execution_counter.count;
    const event_type_short = event.event_type.replace(/Event$/, "");
    results.push([seq, `bus2_handler_${event_type_short}`]);
    return "forwarded bus result";
  };

  bus_2.on("*", child_bus2_event_handler);
  bus_1.on("*", bus_2.dispatch);

  const parent_bus1_handler = async (event: BaseEvent): Promise<string> => {
    execution_counter.count += 1;
    const seq = execution_counter.count;
    results.push([seq, "parent_start"]);

    const child_event_async = event.bus?.emit(QueuedChildEvent({}))!;
    assert.notEqual(child_event_async.event_status, "completed");

    const child_event_sync = await event.bus?.emit(ImmediateChildEvent({})).done()!;
    assert.equal(child_event_sync.event_status, "completed");

    assert.ok(child_event_sync.event_path.includes("bus2"));
    assert.ok(
      Array.from(child_event_sync.event_results.values()).some((result) =>
        result.handler_name.includes("dispatch")
      )
    );

    assert.equal(child_event_async.event_parent_id, event.event_id);
    assert.equal(child_event_sync.event_parent_id, event.event_id);

    execution_counter.count += 1;
    const end_seq = execution_counter.count;
    results.push([end_seq, "parent_end"]);
    return "parent_done";
  };

  bus_1.on(ParentEvent, parent_bus1_handler);

  const parent_event = bus_1.dispatch(ParentEvent({}));
  await parent_event.done();
  await bus_1.waitUntilIdle();
  await bus_2.waitUntilIdle();

  const event_children = bus_1.event_history.filter(
    (event) =>
      event.event_type === "ImmediateChildEvent" || event.event_type === "QueuedChildEvent"
  );
  assert.ok(event_children.length > 0);
  assert.ok(
    event_children.every((event) => event.event_parent_id === parent_event.event_id)
  );

  const sorted_results = results.slice().sort((a, b) => a[0] - b[0]);
  const execution_order = sorted_results.map((item) => item[1]);

  assert.equal(execution_order[0], "parent_start");
  assert.ok(execution_order.includes("bus2_handler_ImmediateChild"));

  if (execution_order.includes("parent_end")) {
    const parent_end_idx = execution_order.indexOf("parent_end");
    assert.ok(parent_end_idx > 1);
  }

  assert.equal(
    execution_order.filter((value) => value === "bus2_handler_ImmediateChild").length,
    1
  );
  assert.equal(
    execution_order.filter((value) => value === "bus2_handler_QueuedChild").length,
    1
  );
  assert.equal(
    execution_order.filter((value) => value === "bus2_handler_Parent").length,
    1
  );
});

test("race condition stress", async () => {
  const bus_1 = new EventBus("bus1");
  const bus_2 = new EventBus("bus2");

  const results: string[] = [];

  const child_handler = async (event: BaseEvent): Promise<string> => {
    const bus_name = event.event_path[event.event_path.length - 1] ?? "unknown";
    results.push(`child_${bus_name}`);
    await delay(1);
    return `child_done_${bus_name}`;
  };

  const parent_handler = async (event: BaseEvent): Promise<string> => {
    const children: BaseEvent[] = [];

    for (let i = 0; i < 3; i += 1) {
      children.push(event.bus?.emit(QueuedChildEvent({}))!);
    }

    for (let i = 0; i < 3; i += 1) {
      const child = await event.bus?.emit(ImmediateChildEvent({})).done()!;
      assert.equal(child.event_status, "completed");
      children.push(child);
    }

    assert.ok(children.every((child) => child.event_parent_id === event.event_id));
    return "parent_done";
  };

  const bad_handler = (_bad: BaseEvent): void => {};

  bus_1.on("*", bus_2.dispatch);
  bus_1.on(QueuedChildEvent, child_handler);
  bus_1.on(ImmediateChildEvent, child_handler);
  bus_2.on(QueuedChildEvent, child_handler);
  bus_2.on(ImmediateChildEvent, child_handler);
  bus_1.on(BaseEvent, parent_handler);
  bus_1.on(BaseEvent, bad_handler);

  for (let run = 0; run < 5; run += 1) {
    results.length = 0;

    const event = bus_1.dispatch(new BaseEvent({}));
    await event.done();
    await bus_1.waitUntilIdle();
    await bus_2.waitUntilIdle();

    assert.equal(
      results.filter((value) => value === "child_bus1").length,
      6,
      `Run ${run}: Expected 6 child_bus1, got ${results.filter((value) => value === "child_bus1").length}`
    );
    assert.equal(
      results.filter((value) => value === "child_bus2").length,
      6,
      `Run ${run}: Expected 6 child_bus2, got ${results.filter((value) => value === "child_bus2").length}`
    );
  }
});

test("awaited child jumps queue without overshoot", async () => {
  const bus = new EventBus("TestBus", { max_history_size: 100 });
  const execution_order: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});
  const Event3 = BaseEvent.extend("Event3", {});
  const LocalChildEvent = BaseEvent.extend("ChildEvent", {});

  const event1_handler = async (_event: BaseEvent): Promise<string> => {
    execution_order.push("Event1_start");
    const child = _event.bus?.emit(LocalChildEvent({}))!;
    execution_order.push("Child_dispatched");
    await child.done();
    execution_order.push("Child_await_returned");
    execution_order.push("Event1_end");
    return "event1_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Event2_start");
    execution_order.push("Event2_end");
    return "event2_done";
  };

  const event3_handler = async (): Promise<string> => {
    execution_order.push("Event3_start");
    execution_order.push("Event3_end");
    return "event3_done";
  };

  const child_handler = async (): Promise<string> => {
    execution_order.push("Child_start");
    execution_order.push("Child_end");
    return "child_done";
  };

  bus.on(Event1, event1_handler);
  bus.on(Event2, event2_handler);
  bus.on(Event3, event3_handler);
  bus.on(LocalChildEvent, child_handler);

  const event_1 = bus.dispatch(Event1({}));
  const event_2 = bus.dispatch(Event2({}));
  const event_3 = bus.dispatch(Event3({}));

  await delay(0);

  await event_1.done();

  assert.ok(execution_order.includes("Child_start"));
  assert.ok(execution_order.includes("Child_end"));
  const child_start_idx = execution_order.indexOf("Child_start");
  const child_end_idx = execution_order.indexOf("Child_end");
  const event1_end_idx = execution_order.indexOf("Event1_end");
  assert.ok(child_start_idx < event1_end_idx);
  assert.ok(child_end_idx < event1_end_idx);

  assert.ok(!execution_order.includes("Event2_start"));
  assert.ok(!execution_order.includes("Event3_start"));

  assert.equal(event_2.event_status, "pending");
  assert.equal(event_3.event_status, "pending");

  await bus.waitUntilIdle();

  const event2_start_idx = execution_order.indexOf("Event2_start");
  const event3_start_idx = execution_order.indexOf("Event3_start");
  assert.ok(event2_start_idx < event3_start_idx);

  assert.equal(event_2.event_status, "completed");
  assert.equal(event_3.event_status, "completed");

  const history_list = bus.event_history;
  const child_event = history_list.find((event) => event.event_type === "ChildEvent");
  const event2_from_history = history_list.find((event) => event.event_type === "Event2");
  const event3_from_history = history_list.find((event) => event.event_type === "Event3");

  assert.ok(child_event?.event_started_at);
  assert.ok(event2_from_history?.event_started_at);
  assert.ok(event3_from_history?.event_started_at);

  assert.ok(child_event!.event_started_at! < event2_from_history!.event_started_at!);
  assert.ok(child_event!.event_started_at! < event3_from_history!.event_started_at!);
});

test("dispatch multiple, await one skips others until after handler completes", async () => {
  const bus = new EventBus("MultiDispatchBus", { max_history_size: 100 });
  const execution_order: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});
  const Event3 = BaseEvent.extend("Event3", {});
  const ChildA = BaseEvent.extend("ChildA", {});
  const ChildB = BaseEvent.extend("ChildB", {});
  const ChildC = BaseEvent.extend("ChildC", {});

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push("Event1_start");

    event.bus?.emit(ChildA({}));
    execution_order.push("ChildA_dispatched");

    const child_b = event.bus?.emit(ChildB({}))!;
    execution_order.push("ChildB_dispatched");

    event.bus?.emit(ChildC({}));
    execution_order.push("ChildC_dispatched");

    await child_b.done();
    execution_order.push("ChildB_await_returned");

    execution_order.push("Event1_end");
    return "event1_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Event2_start");
    execution_order.push("Event2_end");
    return "event2_done";
  };

  const event3_handler = async (): Promise<string> => {
    execution_order.push("Event3_start");
    execution_order.push("Event3_end");
    return "event3_done";
  };

  const child_a_handler = async (): Promise<string> => {
    execution_order.push("ChildA_start");
    execution_order.push("ChildA_end");
    return "child_a_done";
  };

  const child_b_handler = async (): Promise<string> => {
    execution_order.push("ChildB_start");
    execution_order.push("ChildB_end");
    return "child_b_done";
  };

  const child_c_handler = async (): Promise<string> => {
    execution_order.push("ChildC_start");
    execution_order.push("ChildC_end");
    return "child_c_done";
  };

  bus.on(Event1, event1_handler);
  bus.on(Event2, event2_handler);
  bus.on(Event3, event3_handler);
  bus.on(ChildA, child_a_handler);
  bus.on(ChildB, child_b_handler);
  bus.on(ChildC, child_c_handler);

  const event_1 = bus.dispatch(Event1({}));
  bus.dispatch(Event2({}));
  bus.dispatch(Event3({}));

  await event_1.done();

  assert.ok(execution_order.includes("ChildB_start"));
  assert.ok(execution_order.includes("ChildB_end"));

  const child_b_end_idx = execution_order.indexOf("ChildB_end");
  const event1_end_idx = execution_order.indexOf("Event1_end");
  assert.ok(child_b_end_idx < event1_end_idx);

  if (execution_order.includes("ChildA_start")) {
    const child_a_start_idx = execution_order.indexOf("ChildA_start");
    assert.ok(child_a_start_idx > event1_end_idx);
  }
  if (execution_order.includes("ChildC_start")) {
    const child_c_start_idx = execution_order.indexOf("ChildC_start");
    assert.ok(child_c_start_idx > event1_end_idx);
  }
  if (execution_order.includes("Event2_start")) {
    const event2_start_idx = execution_order.indexOf("Event2_start");
    assert.ok(event2_start_idx > event1_end_idx);
  }
  if (execution_order.includes("Event3_start")) {
    const event3_start_idx = execution_order.indexOf("Event3_start");
    assert.ok(event3_start_idx > event1_end_idx);
  }

  await bus.waitUntilIdle();

  const event2_start_idx = execution_order.indexOf("Event2_start");
  const event3_start_idx = execution_order.indexOf("Event3_start");
  const child_a_start_idx = execution_order.indexOf("ChildA_start");
  const child_c_start_idx = execution_order.indexOf("ChildC_start");

  assert.ok(event2_start_idx < event3_start_idx);
  assert.ok(event3_start_idx < child_a_start_idx);
  assert.ok(child_a_start_idx < child_c_start_idx);
});

test("multi-bus queues are independent when awaiting child", async () => {
  const bus_1 = new EventBus("Bus1", { max_history_size: 100 });
  const bus_2 = new EventBus("Bus2", { max_history_size: 100 });
  const execution_order: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});
  const Event3 = BaseEvent.extend("Event3", {});
  const Event4 = BaseEvent.extend("Event4", {});
  const LocalChildEvent = BaseEvent.extend("ChildEvent", {});

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push("Bus1_Event1_start");
    const child = event.bus?.emit(LocalChildEvent({}))!;
    execution_order.push("Child_dispatched_to_Bus1");
    await child.done();
    execution_order.push("Child_await_returned");
    execution_order.push("Bus1_Event1_end");
    return "event1_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Bus1_Event2_start");
    execution_order.push("Bus1_Event2_end");
    return "event2_done";
  };

  const event3_handler = async (): Promise<string> => {
    execution_order.push("Bus2_Event3_start");
    execution_order.push("Bus2_Event3_end");
    return "event3_done";
  };

  const event4_handler = async (): Promise<string> => {
    execution_order.push("Bus2_Event4_start");
    execution_order.push("Bus2_Event4_end");
    return "event4_done";
  };

  const child_handler = async (): Promise<string> => {
    execution_order.push("Child_start");
    execution_order.push("Child_end");
    return "child_done";
  };

  bus_1.on(Event1, event1_handler);
  bus_1.on(Event2, event2_handler);
  bus_1.on(LocalChildEvent, child_handler);

  bus_2.on(Event3, event3_handler);
  bus_2.on(Event4, event4_handler);

  const event_1 = bus_1.dispatch(Event1({}));
  bus_1.dispatch(Event2({}));
  bus_2.dispatch(Event3({}));
  bus_2.dispatch(Event4({}));

  await delay(0);

  await event_1.done();

  assert.ok(execution_order.includes("Child_start"));
  assert.ok(execution_order.includes("Child_end"));

  const child_end_idx = execution_order.indexOf("Child_end");
  const event1_end_idx = execution_order.indexOf("Bus1_Event1_end");
  assert.ok(child_end_idx < event1_end_idx);

  assert.ok(!execution_order.includes("Bus1_Event2_start"));
  assert.ok(!execution_order.includes("Bus2_Event3_start"));
  assert.ok(!execution_order.includes("Bus2_Event4_start"));

  await bus_1.waitUntilIdle();
  await bus_2.waitUntilIdle();

  assert.ok(execution_order.includes("Bus1_Event2_start"));
  assert.ok(execution_order.includes("Bus2_Event3_start"));
  assert.ok(execution_order.includes("Bus2_Event4_start"));
});

test("awaiting an already completed event is a no-op", async () => {
  const bus = new EventBus("AlreadyCompletedBus", { max_history_size: 100 });
  const execution_order: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});

  const event1_handler = async (): Promise<string> => {
    execution_order.push("Event1_start");
    execution_order.push("Event1_end");
    return "event1_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Event2_start");
    execution_order.push("Event2_end");
    return "event2_done";
  };

  bus.on(Event1, event1_handler);
  bus.on(Event2, event2_handler);

  const event_1 = await bus.dispatch(Event1({})).done();
  assert.equal(event_1.event_status, "completed");

  const event_2 = bus.dispatch(Event2({}));

  await event_1.done();

  assert.equal(event_2.event_status, "pending");

  await bus.waitUntilIdle();
});

test("multiple awaits on same event", async () => {
  const bus = new EventBus("MultiAwaitBus", { max_history_size: 100 });
  const execution_order: string[] = [];
  const await_results: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});
  const LocalChildEvent = BaseEvent.extend("ChildEvent", {});

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push("Event1_start");

    const child = event.bus?.emit(LocalChildEvent({}))!;

    const await_child = async (name: string): Promise<void> => {
      await child.done();
      await_results.push(`${name}_completed`);
    };

    await Promise.all([await_child("await1"), await_child("await2")]);
    execution_order.push("Both_awaits_completed");
    execution_order.push("Event1_end");
    return "event1_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Event2_start");
    execution_order.push("Event2_end");
    return "event2_done";
  };

  const child_handler = async (): Promise<string> => {
    execution_order.push("Child_start");
    await delay(10);
    execution_order.push("Child_end");
    return "child_done";
  };

  bus.on(Event1, event1_handler);
  bus.on(Event2, event2_handler);
  bus.on(LocalChildEvent, child_handler);

  const event_1 = bus.dispatch(Event1({}));
  bus.dispatch(Event2({}));

  await event_1.done();

  assert.equal(await_results.length, 2);
  assert.ok(await_results.includes("await1_completed"));
  assert.ok(await_results.includes("await2_completed"));

  assert.ok(execution_order.includes("Child_start"));
  assert.ok(execution_order.includes("Child_end"));
  const child_end_idx = execution_order.indexOf("Child_end");
  const event1_end_idx = execution_order.indexOf("Event1_end");
  assert.ok(child_end_idx < event1_end_idx);

  assert.ok(!execution_order.includes("Event2_start"));

  await bus.waitUntilIdle();
});

test("deeply nested awaited children", async () => {
  const bus = new EventBus("DeepNestedBus", { max_history_size: 100 });
  const execution_order: string[] = [];

  const Event1 = BaseEvent.extend("Event1", {});
  const Event2 = BaseEvent.extend("Event2", {});
  const Child1 = BaseEvent.extend("Child1", {});
  const Child2 = BaseEvent.extend("Child2", {});

  const event1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push("Event1_start");
    const child1 = event.bus?.emit(Child1({}))!;
    await child1.done();
    execution_order.push("Event1_end");
    return "event1_done";
  };

  const child1_handler = async (event: BaseEvent): Promise<string> => {
    execution_order.push("Child1_start");
    const child2 = event.bus?.emit(Child2({}))!;
    await child2.done();
    execution_order.push("Child1_end");
    return "child1_done";
  };

  const child2_handler = async (): Promise<string> => {
    execution_order.push("Child2_start");
    execution_order.push("Child2_end");
    return "child2_done";
  };

  const event2_handler = async (): Promise<string> => {
    execution_order.push("Event2_start");
    execution_order.push("Event2_end");
    return "event2_done";
  };

  bus.on(Event1, event1_handler);
  bus.on(Child1, child1_handler);
  bus.on(Child2, child2_handler);
  bus.on(Event2, event2_handler);

  const event_1 = bus.dispatch(Event1({}));
  bus.dispatch(Event2({}));

  await event_1.done();

  assert.ok(execution_order.includes("Child1_start"));
  assert.ok(execution_order.includes("Child1_end"));
  assert.ok(execution_order.includes("Child2_start"));
  assert.ok(execution_order.includes("Child2_end"));

  const child2_end_idx = execution_order.indexOf("Child2_end");
  const child1_end_idx = execution_order.indexOf("Child1_end");
  const event1_end_idx = execution_order.indexOf("Event1_end");
  assert.ok(child2_end_idx < child1_end_idx);
  assert.ok(child1_end_idx < event1_end_idx);

  assert.ok(!execution_order.includes("Event2_start"));

  await bus.waitUntilIdle();

  const event2_start_idx = execution_order.indexOf("Event2_start");
  assert.ok(event2_start_idx > event1_end_idx);
});
