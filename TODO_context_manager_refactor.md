# Context Manager Refactor Plan (Python + TypeScript)

This document replaces the previous draft with a behavior-accurate trace of both libraries (`bubus/`, `bubus-ts/`) and a parity plan that does not assume unimplemented APIs.

## Goals

1. Make lock/timeout/slow-monitor/error boundaries explicit and symmetric.
2. Decouple lock policy behind small interfaces/protocols.
3. Preserve queue-jumping semantics (including cross-bus forwarding).
4. Preserve existing concurrency/completion behavior unless explicitly changed.
5. Align Python and TypeScript designs where practical.
6. Enforce agreed cross-language semantics for timeouts, `first()`, queue-jump locking, and core error taxonomy.

## Verified Runtime Trace

### Python (`bubus/`)

1. Dispatch + context capture:
- `emit()` applies per-event defaults and captures dispatch context via `contextvars.copy_context()` (`bubus/event_bus.py:959`, `bubus/event_bus.py:991`).

2. Runloop path:
- `_run_loop_weak()` dequeues events and chooses parallel-vs-serial branch by querying `locks.get_lock_for_event(...)` (`bubus/event_bus.py:1587`, `bubus/event_bus.py:1639`).
- Serial path calls `step(event=...)`; parallel path spawns task that also calls `step(event=...)`.

3. Core execution path:
- `step()` acquires event lock via `locks.lock_for_event(...)` (`bubus/event_bus.py:1795`, `bubus/event_bus.py:1853`).
- `handle_event()` builds handler set, starts event slow-monitor task, runs `_execute_handlers(...)`, then completion propagation (`bubus/event_bus.py:1868`).
- `_execute_handlers()` handles pending-result creation + serial/parallel + `FIRST` cancellation semantics (`bubus/event_bus.py:2047`).
- `execute_handler()` acquires handler lock via `locks.lock_for_event_handler(...)`, then calls `EventResult.execute(...)` (`bubus/event_bus.py:2175`, `bubus/event_bus.py:2211`).
- `EventResult.execute()` enforces handler timeout via `asyncio.wait_for`, runs slow-handler monitor, and records errors (`bubus/base_event.py:458`).

4. Queue-jumping path:
- `await event` inside handler enters `_wait_for_completion_inside_handler()` -> `_process_self_on_all_buses()` (`bubus/base_event.py:888`, `bubus/base_event.py:779`).
- `_process_self_on_all_buses()` fast-path removes queued instance and calls `bus.step(event=self)`.
- Slow-path detects lock-mediated in-flight state (`bus.locks.get_lock_for_event(...)` + `_processing_event_ids`) and may still call `bus.step(event=self)` once per bus.

5. Lock/reentrancy details:
- `LockManager.get_lock_for_event()` picks exactly one lock: `None | global | bus` (`bubus/lock_manager.py:113`).
- `ReentrantLock` tracks depth in `ContextVar` map (`bubus/lock_manager.py:12`).
- `lock_context_for_current_handler()` mirrors event-lock ownership into copied dispatch context (`bubus/lock_manager.py:185`).

6. Timeout/error/context:
- Handler timeout composition is resolved in `_resolve_handler_timeout()` (`bubus/event_bus.py:657`).
- Event-level timeout is **not** currently a hard wrapper around `handle_event`; `event.event_timeout` currently influences per-handler effective timeout.
- Errors are normalized inside `EventResult.execute()` (cancelled, timeout, generic) and surfaced via `execute_handler()` result-change notifications.
- ContextVars are set/reset with `_enter_handler_execution_context()` / `_exit_handler_execution_context()` (`bubus/event_bus.py:2006`, `bubus/event_bus.py:2014`).

### TypeScript (`bubus-ts/`)

1. Dispatch + ALS capture:
- `dispatch()` captures AsyncLocalStorage store via `captureAsyncContext()` (`bubus-ts/src/event_bus.ts:459`, `bubus-ts/src/async_context.ts:39`).

2. Runloop path:
- `runloop()` dequeues, acquires event semaphore from `LockManager.getSemaphoreForEvent()`, and calls `processEvent(...)` (`bubus-ts/src/event_bus.ts:865`, `bubus-ts/src/lock_manager.ts:288`).

3. Core execution path:
- `processEvent()` starts event, starts event slow-warning timer, builds pending handler results, and delegates to `BaseEvent.processEvent(...)` (`bubus-ts/src/event_bus.ts:687`).
- `BaseEvent.processEvent()` runs `EventResult.runHandler()` for each pending handler and applies `'all'`/`'first'` completion logic (`bubus-ts/src/base_event.ts:394`).
- `EventResult.runHandler()` handles handler semaphore, timeout race, slow-warning timer, error normalization, and completion/error marking (`bubus-ts/src/event_result.ts:245`).

4. Queue-jumping path:
- `event.done()/immediate()` routes to `processEventImmediately()` (`bubus-ts/src/base_event.ts:655`, `bubus-ts/src/event_bus.ts:738`).
- If inside handler, it uses `HandlerLock.runQueueJump(...)` and per-bus runloop pause/release (`bubus-ts/src/lock_manager.ts:131`, `bubus-ts/src/lock_manager.ts:190`).
- Cross-bus queue-jump is handled by `processEventImmediatelyAcrossBuses()` with semaphore bypass rules for shared global semaphore (`bubus-ts/src/event_bus.ts:787`).

5. Timeout/error/context:
- Effective handler timeout is min(handler, event, bus default) in `EventResult.handler_timeout` (`bubus-ts/src/event_result.ts:161`).
- Event-level hard timeout wrapper around all handlers does not currently exist.
- Running async handlers cannot be force-cancelled in JS; timeouts/aborts mark result error and signal abort, but handler code may continue in background.
- ALS restore occurs in `runWithAsyncContext(event._event_dispatch_context, ...)` (`bubus-ts/src/event_result.ts:279`, `bubus-ts/src/async_context.ts:46`).

## Inconsistencies Fixed from Previous Draft

1. The draft was Python-only; this plan now covers both libraries.
2. `_run_loop_weak()` lock decision is currently based on `locks.get_lock_for_event(...)`, not direct enum checks.
3. `_process_self_on_all_buses()` currently relies on lock-object presence and `_processing_event_ids`, not only `event_concurrency != parallel`.
4. Event-level hard timeout (`handle_event` wrapper) is a proposed semantic change, not current behavior.
5. Python currently uses `_enter/_exit_handler_execution_context` token pair; no `handler_execution_context` helper yet.
6. TS currently has no context-manager abstraction; it uses semaphores + `try/finally` + timers.
7. Verification commands in prior draft referenced missing tests (`tests/test_comprehensive_patterns.py`, `tests/test_timeout_scope.py` in Python).

## Proposed Refactor (No Code Changes Yet)

### Phase 0: Locked-in semantics (decided)

1. `event_timeout` is a hard cap across all handlers (both languages).
2. `event_handler_timeout` defaults to `event_timeout` when unset.
3. Queue-jump path keeps lock-object checks and preserves correct re-entrancy/release behavior for both `event_concurrency` and `event_handler_concurrency`.
4. `first()` winner criteria:
- Python: first non-`None`, non-error, non-`BaseEvent` result.
- TypeScript: first non-`undefined`, non-error, non-`BaseEvent` result (`null` remains a valid winning value).
5. Core error taxonomy parity across both languages must include:
- `EventHandlerCancelledError`
- `EventHandlerResultSchemaError`
- `EventHandlerTimeoutError`
- `EventHandlerAbortedError`
6. Lock manager should be refactored behind an interface seam that is backend-friendly later, but no full backend-swappable implementation is required in this change.
7. TypeScript hard-timeout behavior must mirror Python intent as closely as possible:
- On event-level timeout, do not wait for in-flight handler promises.
- Mark in-flight handlers as `EventHandlerAbortedError`.
- Mark pending downstream/child handlers as `EventHandlerCancelledError`.
- Ignore late promise fulfillments/rejections after timeout state is finalized.

### Phase 1: LockManager interface decoupling

Python:
- Introduce `LockManagerProtocol` with three public context managers:
  - `event_lock(...)`
  - `handler_lock(...)`
  - `handler_dispatch_context(...)`
- Keep existing names as compatibility aliases during migration:
  - `lock_for_event` -> `event_lock`
  - `lock_for_event_handler` -> `handler_lock`
  - `lock_context_for_current_handler` -> `handler_dispatch_context`
- Keep `get_lock_for_event(...)` style lock-object query path available for queue-jump slow-path correctness checks.

TypeScript:
- Introduce a small lock-policy interface using functional scopes:
  - `withEventLock(event, fn)`
  - `withHandlerLock(event, result, fn)`
  - `withHandlerDispatchContext(event, fn)` (if needed for parity hooks)
- Default implementation should wrap existing `AsyncSemaphore` + `HandlerLock`.
- Ensure interface still exposes enough lock identity/state for queue-jump lock checks and re-entrancy-safe release/reacquire behavior.

### Phase 2: Context helpers

Python:
- Replace `_enter/_exit_handler_execution_context` with one context manager `handler_execution_context(event, handler_id)`.
- Keep ContextVar behavior unchanged; include lock dispatch context mark inside this manager.

TypeScript:
- Keep ALS restoration in `runWithAsyncContext`, but move wrapper composition into one helper so timeout/slow/error wrappers can be stacked consistently.

### Phase 3: Timeout/slow/error wrapper extraction

Python:
- Add reusable helpers:
  - `_timeout_scope(timeout)`
  - `_slow_monitor(coro | None)`
  - `_save_handler_errors(...)`
- Use them in `EventResult.execute()` and also wrap `handle_event()` in event-level `_timeout_scope(event.event_timeout)` to enforce hard event cap.
- Keep handler timeout resolution rule as: handler timeout defaults to event timeout when unset.

TypeScript:
- Add wrapper utilities mirroring Python intent:
  - `withTimeout(timeoutSecs, fn)`
  - `withSlowMonitor(timerFactory, fn)`
  - `withSavedHandlerErrors(eventResult, fn)`
- Apply event-level hard timeout wrapper around event processing (`processEvent` / per-bus handler run scope), not just per-handler timeout races.
- Keep handler timeout resolution rule as: handler timeout defaults to event timeout when unset.
- Normalize handler failures to the agreed taxonomy in `EventResult.runHandler()`.
- On event-level timeout, immediately finalize timeout state without awaiting in-flight handlers; aborted/cancelled marking must prevent late async completions from mutating finalized results.

#### TypeScript abort/cancel implementation details (hard event timeout)

Current state (already present):
- Started handlers can already be interrupted via `signalAbort(...)` and lock release (`bubus-ts/src/event_result.ts:364`, `bubus-ts/src/event_result.ts:245`).
- Pending/started handler cancellation paths already exist for first-mode and parent cancellation (`bubus-ts/src/base_event.ts:538`, `bubus-ts/src/base_event.ts:574`).
- Late completions are already guarded because `markCompleted`/`markError` are terminal no-ops (`bubus-ts/src/event_result.ts:385`, `bubus-ts/src/event_result.ts:394`).

Gap (must be added):
- There is no true event-level hard timeout wrapper over all handlers yet; `BaseEvent.processEvent()` still awaits handler promises (`bubus-ts/src/base_event.ts:394`).

Required changes:
1. Add an event-level timeout race in `EventBus.processEvent(...)` (`bubus-ts/src/event_bus.ts:687`) for the bus-local handler execution scope.
2. On event-timeout, immediately run a bus-scoped timeout finalizer that sweeps this event’s handler results on that bus:
- `started` -> `EventHandlerAbortedError`
- `pending` -> `EventHandlerCancelledError`
3. In the same timeout finalizer, ensure active execution state is released:
- call `result._lock?.exitHandlerRun()`
- call `result.releaseQueueJumpPauses()`
- call `result.signalAbort(aborted_error)` for started handlers
4. Do not await in-flight handler promises after timeout; detach and suppress with `catch` so event finalization is not blocked.
5. Reuse descendant cancellation logic (`cancelPendingDescendants`/`markCancelled`) so child events queued after timeout are marked cancelled consistently.
6. Keep terminal-state guards so any late promise resolution/rejection cannot overwrite aborted/cancelled outcomes.
7. Update `first()` winner filtering in TS to: first non-`undefined`, non-error, non-`BaseEvent` result (where `null` remains valid).

### Phase 4: Queue-jump safety after abstraction

Python:
- Ensure `_process_self_on_all_buses()` still preserves current slow-path behavior that depends on in-flight lock state.
- Do not regress `ReentrantLock` depth behavior across copied context.
- Keep lock-object checks in queue-jump path (do not replace with enum-only checks).

TypeScript:
- Preserve `HandlerLock.runQueueJump()` behavior and cross-bus pause/release lifecycle.
- Ensure any lock-policy abstraction still supports bypass logic for shared global semaphore instances.
- Ensure queue-jump correctness remains lock-driven, including proper release/reacquire in serial handler mode.

### Phase 5: Verification matrix

Python:
```bash
python -m pytest tests/test_event_handler_concurrency.py -xvs
python -m pytest tests/test_event_handler_completion.py -xvs
python -m pytest tests/test_handler_timeout.py -xvs
python -m pytest tests/test_event_timeout_defaults.py -xvs
python -m pytest tests/test_context_propagation.py -xvs
python -m pytest tests/test_parent_event_tracking.py -xvs
python -m pytest tests/test_stress_20k_events.py -xvs
```

TypeScript:
```bash
cd bubus-ts
pnpm test tests/locking.test.ts
pnpm test tests/timeout.test.ts
pnpm test tests/first.test.ts
pnpm test tests/context_propagation.test.ts
pnpm test tests/forwarding.test.ts
```

## Semantics Decisions (Resolved)

1. `event_timeout` is a hard cap across all handlers in both languages.
2. `event_handler_timeout` defaults to `event_timeout` when unset.
3. Queue-jump logic keeps lock checks and must remain re-entrancy safe.
4. `first()` winner criteria are now explicitly language-aligned with TS `undefined` vs Python `None` semantics.
5. Core handler error taxonomy is aligned across both libraries for cancelled/result-schema/timeout/aborted.
6. Lock-manager refactor targets an interface seam now, full backend swapping later.
7. TypeScript event-level timeout finalizes immediately: in-flight -> `EventHandlerAbortedError`, pending child/downstream -> `EventHandlerCancelledError`, and late async completions are ignored.

## Implementation Readiness

This plan is aligned with the current code paths and now has explicit cross-language semantic decisions. Implementation can proceed in both libraries without further semantic ambiguity.
