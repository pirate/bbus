import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const ParentEvent = BaseEvent.extend("ParentEvent", {});
const ChildEvent = BaseEvent.extend("ChildEvent", {});
const GrandchildEvent = BaseEvent.extend("GrandchildEvent", {});
const UnrelatedEvent = BaseEvent.extend("UnrelatedEvent", {});
const ScreenshotEvent = BaseEvent.extend("ScreenshotEvent", { target_id: z.string() });
const NavigateEvent = BaseEvent.extend("NavigateEvent", { url: z.string() });
const TabCreatedEvent = BaseEvent.extend("TabCreatedEvent", { tab_id: z.string() });
const SystemEvent = BaseEvent.extend("SystemEvent", {});
const UserActionEvent = BaseEvent.extend("UserActionEvent", {
  action: z.string(),
  user_id: z.string()
});

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("find past returns most recent completed event", async () => {
  const bus = new EventBus("FindPastBus");

  const first_event = bus.dispatch(ParentEvent({}));
  await first_event.done();
  await delay(20);
  const second_event = bus.dispatch(ParentEvent({}));
  await second_event.done();

  const found_event = await bus.find(ParentEvent, { past: true, future: false });
  assert.ok(found_event);
  assert.equal(found_event.event_id, second_event.event_id);
});

test("find past returns null when no matching event exists", async () => {
  const bus = new EventBus("FindPastNoneBus");

  const start = Date.now();
  const found_event = await bus.find(ParentEvent, { past: true, future: false });
  const elapsed_ms = Date.now() - start;

  assert.equal(found_event, null);
  assert.ok(elapsed_ms < 100);
});

test("find past window filters by time", async () => {
  const bus = new EventBus("FindWindowBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);
  const new_event = bus.dispatch(ParentEvent({}));
  await new_event.done();

  const found_event = await bus.find(ParentEvent, { past: 0.1, future: false });
  assert.ok(found_event);
  assert.equal(found_event.event_id, new_event.event_id);
});

test("find past returns null when all events are too old", async () => {
  const bus = new EventBus("FindTooOldBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);

  const found_event = await bus.find(ParentEvent, { past: 0.05, future: false });
  assert.equal(found_event, null);
});

test("find future waits for event", async () => {
  const bus = new EventBus("FindFutureBus");

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(ParentEvent({}));
  }, 50);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.event_type, "ParentEvent");
});

test("find future works with string event keys", async () => {
  const bus = new EventBus("FindFutureStringBus");

  const find_promise = bus.find("ParentEvent", { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(ParentEvent({}));
  }, 30);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.event_type, "ParentEvent");
});

test("find future ignores past events", async () => {
  const bus = new EventBus("FindFutureIgnoresPastBus");

  const prior = bus.dispatch(ParentEvent({}));
  await prior.done();

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 });
  assert.equal(found_event, null);
});

test("find future times out when no event arrives", async () => {
  const bus = new EventBus("FindFutureTimeoutBus");

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 });
  assert.equal(found_event, null);
});

test("find past=false future=false returns null immediately", async () => {
  const bus = new EventBus("FindNeitherBus");

  const start = Date.now();
  const found_event = await bus.find(ParentEvent, { past: false, future: false });
  const elapsed_ms = Date.now() - start;

  assert.equal(found_event, null);
  assert.ok(elapsed_ms < 100);
});

test("find past+future returns past event immediately", async () => {
  const bus = new EventBus("FindPastFutureBus");

  const dispatched = bus.dispatch(ParentEvent({}));
  await dispatched.done();

  const start = Date.now();
  const found_event = await bus.find(ParentEvent, { past: true, future: 0.5 });
  const elapsed_ms = Date.now() - start;

  assert.ok(found_event);
  assert.equal(found_event.event_id, dispatched.event_id);
  assert.ok(elapsed_ms < 100);
});

test("find past+future waits for future when no past match", async () => {
  const bus = new EventBus("FindPastFutureWaitBus");

  const find_promise = bus.find(ChildEvent, { past: true, future: 0.3 });

  setTimeout(() => {
    bus.dispatch(ChildEvent({}));
  }, 50);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.event_type, "ChildEvent");
});

test("find past/future windows are independent", async () => {
  const bus = new EventBus("FindWindowIndependentBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);

  const start = Date.now();
  const found_event = await bus.find(ParentEvent, { past: 0.05, future: 0.05 });
  const elapsed_ms = Date.now() - start;

  assert.equal(found_event, null);
  assert.ok(elapsed_ms > 30);
});

test("find past true future float returns old event immediately", async () => {
  const bus = new EventBus("FindPastTrueFutureFloatBus");

  const dispatched = bus.dispatch(ParentEvent({}));
  await dispatched.done();
  await delay(120);

  const found_event = await bus.find(ParentEvent, { past: true, future: 0.1 });
  assert.ok(found_event);
  assert.equal(found_event.event_id, dispatched.event_id);
});

test("find past float future waits for new event", async () => {
  const bus = new EventBus("FindPastFloatFutureWaitBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);

  const find_promise = bus.find(ParentEvent, { past: 0.05, future: 0.2 });

  setTimeout(() => {
    bus.dispatch(ParentEvent({}));
  }, 50);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.notEqual(found_event.event_id, old_event.event_id);
});

test("find past true future true returns past event immediately", async () => {
  const bus = new EventBus("FindPastTrueFutureTrueBus");

  const dispatched = bus.dispatch(ParentEvent({}));
  await dispatched.done();

  const start = Date.now();
  const found_event = await bus.find(ParentEvent, { past: true, future: true });
  const elapsed_ms = Date.now() - start;

  assert.ok(found_event);
  assert.equal(found_event.event_id, dispatched.event_id);
  assert.ok(elapsed_ms < 100);
});

test("find respects where filter", async () => {
  const bus = new EventBus("FindWhereBus");

  const event_a = bus.dispatch(ScreenshotEvent({ target_id: "tab-a" }));
  const event_b = bus.dispatch(ScreenshotEvent({ target_id: "tab-b" }));
  await event_a.done();
  await event_b.done();

  const found_event = await bus.find(
    ScreenshotEvent,
    (event) => event.target_id === "tab-b",
    { past: true, future: false }
  );

  assert.ok(found_event);
  assert.equal(found_event.event_id, event_b.event_id);
});

test("find where filter works with future waiting", async () => {
  const bus = new EventBus("FindWhereFutureBus");

  const find_promise = bus.find(
    UserActionEvent,
    (event) => event.user_id === "user123",
    { past: false, future: 0.3 }
  );

  setTimeout(() => {
    bus.dispatch(UserActionEvent({ action: "logout", user_id: "user456" }));
    bus.dispatch(UserActionEvent({ action: "login", user_id: "user123" }));
  }, 50);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.user_id, "user123");
});

test("find with multiple concurrent waiters resolves correct events", async () => {
  const bus = new EventBus("FindConcurrentBus");

  const find_normal = bus.find(
    UserActionEvent,
    (event) => event.action === "normal",
    { past: false, future: 0.5 }
  );
  const find_special = bus.find(
    UserActionEvent,
    (event) => event.action === "special",
    { past: false, future: 0.5 }
  );
  const find_system = bus.find("SystemEvent", { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(UserActionEvent({ action: "normal", user_id: "u1" }));
    bus.dispatch(SystemEvent({}));
    bus.dispatch(UserActionEvent({ action: "special", user_id: "u2" }));
  }, 50);

  const [normal, system, special] = await Promise.all([
    find_normal,
    find_system,
    find_special
  ]);

  assert.ok(normal);
  assert.equal(normal.action, "normal");
  assert.ok(system);
  assert.equal(system.event_type, "SystemEvent");
  assert.ok(special);
  assert.equal(special.action, "special");
});

test("find child_of returns child event", async () => {
  const bus = new EventBus("FindChildBus");

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({}));
  });

  const parent_event = bus.dispatch(ParentEvent({}));
  await bus.waitUntilIdle();

  const child_event = await bus.find(ChildEvent, {
    past: true,
    future: false,
    child_of: parent_event
  });

  assert.ok(child_event);
  assert.equal(child_event.event_parent_id, parent_event.event_id);
});

test("find child_of returns null for non-child", async () => {
  const bus = new EventBus("FindNonChildBus");

  const parent_event = bus.dispatch(ParentEvent({}));
  const unrelated_event = bus.dispatch(UnrelatedEvent({}));
  await parent_event.done();
  await unrelated_event.done();

  const found_event = await bus.find(UnrelatedEvent, {
    past: true,
    future: false,
    child_of: parent_event
  });

  assert.equal(found_event, null);
});

test("find child_of returns grandchild event", async () => {
  const bus = new EventBus("FindGrandchildBus");

  let child_event_id: string | null = null;
  bus.on(ParentEvent, async (event) => {
    const child = await event.bus?.emit(ChildEvent({})).done();
    child_event_id = child?.event_id ?? null;
  });
  bus.on(ChildEvent, async (event) => {
    await event.bus?.emit(GrandchildEvent({})).done();
  });

  const parent_event = bus.dispatch(ParentEvent({}));
  await parent_event.done();
  await bus.waitUntilIdle();

  const grandchild_event = await bus.find(GrandchildEvent, {
    past: true,
    future: false,
    child_of: parent_event
  });

  assert.ok(grandchild_event);
  assert.equal(grandchild_event.event_parent_id, child_event_id);
});

test("find child_of works across forwarded buses", async () => {
  const main_bus = new EventBus("MainBus");
  const auth_bus = new EventBus("AuthBus");

  let child_event_id: string | null = null;

  main_bus.on(ParentEvent, auth_bus.dispatch);
  auth_bus.on(ParentEvent, async (event) => {
    const child = await event.bus?.emit(ChildEvent({})).done();
    child_event_id = child.event_id;
  });

  const parent_event = main_bus.dispatch(ParentEvent({}));
  await parent_event.done();
  await main_bus.waitUntilIdle();
  await auth_bus.waitUntilIdle();

  const found_child = await auth_bus.find(ChildEvent, {
    past: 5,
    future: 5,
    child_of: parent_event
  });

  assert.ok(found_child);
  assert.equal(found_child.event_id, child_event_id);
});

test("find child_of filters to correct parent among siblings", async () => {
  const bus = new EventBus("FindCorrectParentBus");

  bus.on(NavigateEvent, async (event) => {
    await event.bus?.emit(TabCreatedEvent({ tab_id: `tab_for_${event.url}` })).done();
  });
  bus.on(TabCreatedEvent, () => {});

  const nav_1 = bus.dispatch(NavigateEvent({ url: "site1" }));
  const nav_2 = bus.dispatch(NavigateEvent({ url: "site2" }));
  await nav_1.done();
  await nav_2.done();

  const tab_1 = await bus.find(TabCreatedEvent, {
    child_of: nav_1,
    past: true,
    future: false
  });
  const tab_2 = await bus.find(TabCreatedEvent, {
    child_of: nav_2,
    past: true,
    future: false
  });

  assert.ok(tab_1);
  assert.ok(tab_2);
  assert.equal(tab_1.tab_id, "tab_for_site1");
  assert.equal(tab_2.tab_id, "tab_for_site2");
});

test("find future with child_of waits for matching child", async () => {
  const bus = new EventBus("FindFutureChildBus");

  bus.on(ParentEvent, async (event) => {
    await delay(30);
    await event.bus?.emit(ChildEvent({})).done();
  });

  const parent_event = bus.dispatch(ParentEvent({}));

  const find_promise = bus.find(ChildEvent, {
    child_of: parent_event,
    past: false,
    future: 0.3
  });

  const child_event = await find_promise;
  assert.ok(child_event);
  assert.equal(child_event.event_parent_id, parent_event.event_id);
});

test("find with past float and where filter", async () => {
  const bus = new EventBus("FindWherePastFloatBus");

  const old_event = bus.dispatch(ScreenshotEvent({ target_id: "tab1" }));
  await old_event.done();
  await delay(120);
  const new_event = bus.dispatch(ScreenshotEvent({ target_id: "tab2" }));
  await new_event.done();

  const found_tab2 = await bus.find(
    ScreenshotEvent,
    (event) => event.target_id === "tab2",
    { past: 0.1, future: false }
  );

  assert.ok(found_tab2);
  assert.equal(found_tab2.event_id, new_event.event_id);

  const found_tab1 = await bus.find(
    ScreenshotEvent,
    (event) => event.target_id === "tab1",
    { past: 0.1, future: false }
  );
  assert.equal(found_tab1, null);
});

test("find with child_of and past float", async () => {
  const bus = new EventBus("FindChildPastFloatBus");

  let child_event_id: string | null = null;
  bus.on(ParentEvent, async (event) => {
    const child = await event.bus?.emit(ChildEvent({})).done();
    child_event_id = child?.event_id ?? null;
  });

  const parent_event = bus.dispatch(ParentEvent({}));
  await parent_event.done();
  await bus.waitUntilIdle();

  const found_child = await bus.find(ChildEvent, {
    child_of: parent_event,
    past: 5,
    future: false
  });

  assert.ok(found_child);
  assert.equal(found_child.event_id, child_event_id);
});

test("find with all parameters combined", async () => {
  const bus = new EventBus("FindAllParamsBus");

  let child_event_id: string | null = null;
  bus.on(ParentEvent, async (event) => {
    const child = await event.bus?.emit(ScreenshotEvent({ target_id: "child_tab" })).done();
    child_event_id = child?.event_id ?? null;
  });

  const parent_event = bus.dispatch(ParentEvent({}));
  await parent_event.done();
  await bus.waitUntilIdle();

  const found_child = await bus.find(
    ScreenshotEvent,
    (event) => event.target_id === "child_tab",
    {
      child_of: parent_event,
      past: 5,
      future: false
    }
  );

  assert.ok(found_child);
  assert.equal(found_child.event_id, child_event_id);
});

test("find past ignores in-progress events but returns after completion", async () => {
  const bus = new EventBus("FindCompletedOnlyBus");

  bus.on(ParentEvent, async () => {
    await delay(80);
  });

  const dispatched = bus.dispatch(ParentEvent({}));
  await delay(10);

  const early_find = await bus.find(ParentEvent, { past: true, future: false });
  assert.equal(early_find, null);

  await dispatched.done();

  const later_find = await bus.find(ParentEvent, { past: true, future: false });
  assert.ok(later_find);
  assert.equal(later_find.event_id, dispatched.event_id);
});

test("find future resolves before handlers complete", async () => {
  const bus = new EventBus("FindBeforeCompleteBus");

  bus.on(ParentEvent, async () => {
    await delay(80);
  });

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(ParentEvent({}));
  }, 20);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.event_status, "started");

  await found_event.done();
  assert.equal(found_event.event_status, "completed");
});

test("find catches child event that fired during parent handler", async () => {
  const bus = new EventBus("FindRaceConditionBus");

  let tab_event_id: string | null = null;
  bus.on(NavigateEvent, async (event) => {
    const tab_event = await event.bus?.emit(TabCreatedEvent({ tab_id: "new_tab" })).done();
    tab_event_id = tab_event?.event_id ?? null;
  });
  bus.on(TabCreatedEvent, () => {});

  const nav_event = bus.dispatch(NavigateEvent({ url: "https://example.com" }));
  await nav_event.done();

  const found_tab = await bus.find(TabCreatedEvent, {
    child_of: nav_event,
    past: true,
    future: false
  });

  assert.ok(found_tab);
  assert.equal(found_tab.event_id, tab_event_id);
});

test("find returns promise that can be awaited later", async () => {
  const bus = new EventBus("FindPromiseBus");

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 });
  assert.ok(find_promise instanceof Promise);

  bus.dispatch(ParentEvent({}));
  const found_event = await find_promise;
  assert.ok(found_event);
});
