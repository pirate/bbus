# TODO: Python Unified Runtime Refactor (Middleware + Context-Manager Style)

This plan merges:
- current middleware runtime behavior
- context-manager refactor goals from `TODO_context_manager_refactor.md`

into one ordered Python implementation plan.
This file supersedes `TODO_context_manager_refactor.md` for Python implementation planning.

## Semantic Decisions (Already Fixed)

1. `event_timeout` is a hard cap across all handlers for an event.
2. `event_handler_timeout` defaults to `event_timeout` when unset.
3. Queue-jump path keeps lock-object checks and re-entrant lock correctness.
4. `first()` winner in Python is first non-`None`, non-error, non-`BaseEvent` result.
5. Core handler error taxonomy must include:
- `EventHandlerCancelledError`
- `EventHandlerResultSchemaError`
- `EventHandlerTimeoutError`
- `EventHandlerAbortedError`

## Constraints (Locked In)

1. Middleware throw behavior is undefined (no suppression/guarding required).
2. Hook re-fires are acceptable (simple implementation preferred).
3. Keep queue-jump semantics and lock correctness.
4. Preserve current behavior unless explicitly listed as a semantic change.
5. Docs updates are out of scope for this pass.

## Target Architecture

### Context-manager style

Explicit scope boundaries for lock/context/timeout/slow-monitor/error handling:

- `event_lock(...)`
- `handler_lock(...)`
- `handler_dispatch_context(...)`
- `handler_execution_context(event, handler_id)` (single context manager replacing enter/exit token helpers)
- timeout/slow/error wrapper helpers used consistently by both event and handler execution paths

### Middleware orchestration

- Middleware execution remains centralized in `EventBus` internal notifier methods.
- Hook calls remain sequential in middleware registration order.
- No middleware exception handling contract is added.

## Verified Runtime Trace (Implementation Anchors)

Use these concrete call paths to constrain refactor changes:

1. Dispatch + context capture:
- `EventBus.emit(...)` applies defaults and captures dispatch context with `contextvars.copy_context()`.
- This is the correct anchor for enqueue-time lifecycle behavior (`event: pending` emission remains bus-owned).

2. Runloop + event processing:
- `_run_loop_weak(...)` dequeues and branches serial/parallel based on `locks.get_lock_for_event(...)`.
- `step(event=...)` acquires event lock via `locks.lock_for_event(...)` then calls `handle_event(...)`.
- `handle_event(...)` is the event-level orchestration seam for slow monitor, handler execution, and completion propagation.

3. Handler execution stack:
- `_execute_handlers(...)` creates pending results and applies first/all completion policy.
- `execute_handler(...)` acquires `locks.lock_for_event_handler(...)` then delegates to `EventResult.execute(...)`.
- `EventResult.execute(...)` is already the core timeout/slow/error normalization path.

4. Queue-jump path:
- Await-inside-handler flows through `_wait_for_completion_inside_handler()` to `_process_self_on_all_buses()`.
- Slow-path logic depends on lock-object/in-flight checks (`get_lock_for_event(...)` + processing id tracking), so refactor must preserve lock-aware checks instead of enum-only shortcuts.

5. Context boundary anchors:
- ContextVar state currently enters/exits through `_enter_handler_execution_context(...)` and `_exit_handler_execution_context(...)`.
- Lock-manager dispatch context helper (`lock_context_for_current_handler(...)`) must remain coupled to handler execution context behavior after consolidation.

### Compact Line-Anchored Call Graph

- `EventBus.emit(...)` defaults/context capture/queue insert: `bubus/event_bus.py:930`.
- `_run_loop_weak(...)` dequeue + serial/parallel branch via lock query: `bubus/event_bus.py:1587`.
- `step(...)` event lock + handle dispatch seam: `bubus/event_bus.py:1795`.
- `handle_event(...)` event-level orchestration seam: `bubus/event_bus.py:1887`.
- `_execute_handlers(...)` pending result creation + first/all logic: `bubus/event_bus.py:2066`.
- `execute_handler(...)` handler lock + result execute seam: `bubus/event_bus.py:2194`.
- `EventResult.execute(...)` handler timeout/slow/error core: `bubus/base_event.py:458`.
- Queue-jump path: `_process_self_on_all_buses(...)`: `bubus/base_event.py:779`.
- Current ContextVar enter/exit helpers:
  - `_enter_handler_execution_context(...)`: `bubus/event_bus.py:2025`.
  - `_exit_handler_execution_context(...)`: `bubus/event_bus.py:2033`.

### Scope-Nesting Rationale

- Preferred layering for event/handler scopes is:
  - lock -> error save/normalization -> hard timeout -> slow monitor -> work.
- Timeout should wrap slow monitor so timeout cancellation tears down monitor task cleanly.
- Error wrapper should remain outermost around timeout/monitor/work so timeout, cancellation, and generic handler failures are normalized consistently.

## Sequencing Rationale (Why This Order)

1. Middleware constructor normalization lands first to fix API/runtime mismatch without touching lock/timeout semantics.
2. Lock-manager protocol seam lands before behavior changes, with compatibility aliases kept, so call sites can migrate incrementally.
3. Handler execution context consolidation happens before timeout hard-cap work to avoid changing context and timeout behavior in one step.
4. Event-level hard timeout is introduced only after wrappers/seams exist, reducing risk around queue-jump and cancellation propagation.
5. Queue-jump safety verification is a dedicated phase after seam extraction to catch regressions in re-entrancy and release/reacquire behavior.

## Ordered Implementation Plan

## Phase 0: Baseline + invariants

1. Snapshot current behavior with existing tests:
- `tests/test_event_handler.py`
- `tests/test_eventbus_timeout.py`
- `tests/test_eventbus_dispatch_contextvars.py`
- `tests/test_eventbus_dispatch_parent_tracking.py`
- `tests/test_eventbus.py` (middleware sections)

2. Capture explicit invariants before refactor:
- queue-jump still respects lock/re-entrancy behavior
- handler timeout resolution precedence remains unchanged unless explicitly changed below
- first-winner semantics and handler error taxonomy remain stable across refactor phases

## Phase 1: Middleware normalization + constructor parity cleanup

Files:
- `bubus/event_bus.py`
- `bubus/middlewares.py` (typing only if needed)

Changes:
1. Normalize `middlewares` input in constructor:
- instance entries pass through
- class entries auto-instantiate (`MiddlewareClass()`)
2. Keep internal storage as `list[EventBusMiddleware]`.
3. Internal hook loop order remains registration order.
4. No middleware exception suppression wrappers are added.

Note:
- This resolves existing doc/runtime mismatch where docs mention class-or-instance inputs.

## Phase 2: LockManager protocol seam (compat first)

Files:
- `bubus/lock_manager.py`
- `bubus/event_bus.py`
- `bubus/base_event.py` (call-site rename updates)

Changes:
1. Introduce protocol/interface seam (`LockManagerProtocol`) exposing:
- `event_lock(...)`
- `handler_lock(...)`
- `handler_dispatch_context(...)`
2. Keep compatibility aliases:
- `lock_for_event` -> `event_lock`
- `lock_for_event_handler` -> `handler_lock`
- `lock_context_for_current_handler` -> `handler_dispatch_context`
3. Keep lock identity query path available (`get_lock_for_event(...)`) for queue-jump slow-path checks.
4. Keep compatibility aliases until both runloop path and queue-jump path are migrated and tested.

## Phase 3: Handler execution context manager

Files:
- `bubus/event_bus.py`

Changes:
1. Replace `_enter_handler_execution_context(...)` and `_exit_handler_execution_context(...)`
with one context manager:
- `handler_execution_context(event, handler_id)`
2. Keep exact ContextVar semantics.
3. Include lock dispatch context mark in this manager so lock/context behavior remains coupled and explicit.
4. Keep behavior identical for both async and sync handler invocation paths.

## Phase 4: Timeout/slow/error wrapper extraction

Files:
- `bubus/event_bus.py`
- `bubus/base_event.py`
- `bubus/helpers.py`
- `bubus/event_handler.py` (define shared handler error classes here)

Changes:
1. Extract reusable wrappers/helpers:
- `_timeout_scope(timeout)`
- `_slow_monitor(...)`
- `_save_handler_errors(...)` (or equivalent)
2. Apply wrappers in `EventResult.execute()` without changing observed handler-level behavior first.
3. Then apply same wrapper style to event-level execution path.
4. Introduce/normalize shared handler error classes for parity:
- `EventHandlerCancelledError`
- `EventHandlerResultSchemaError`
- `EventHandlerTimeoutError`
- `EventHandlerAbortedError`
5. Ensure terminal handler paths map to these classes consistently.

## Phase 5: Event-level hard timeout semantic change

Files:
- `bubus/event_bus.py`
- `bubus/base_event.py`

Semantic decision implemented here:
1. `event_timeout` becomes a hard cap across all handlers for an event.
2. `event_handler_timeout` defaults to `event_timeout` when unset (retain this resolution rule).

Implementation steps:
1. Wrap event processing scope (`handle_event` path) in event-level timeout.
2. Ensure timeout finalization preserves existing cancellation cascade expectations.
3. Keep middleware lifecycle notifications monotonic through timeout transitions.
4. Ensure `event_handler_timeout` fallback remains `event_timeout` after refactor.
5. Keep no-wait cancellation semantics for timed-out in-flight child processing consistent.

## Phase 6: Queue-jump lock safety after seam extraction

Files:
- `bubus/base_event.py`
- `bubus/event_bus.py`
- `bubus/lock_manager.py`

Checks/changes:
1. Keep lock-object checks in queue-jump slow-path (do not reduce to enum-only checks).
2. Preserve `ReentrantLock` depth semantics across copied context.
3. Keep cross-bus queue-jump behavior unchanged.
4. Ensure no regression in release/reacquire behavior during nested execution.

## Phase 7: `first()` and taxonomy alignment pass

Files:
- `bubus/event_bus.py`
- `bubus/base_event.py`
- `tests/test_event_handler.py`
- `tests/test_eventbus.py`

Changes:
1. Re-assert/lock winner criteria:
- non-error
- non-`BaseEvent`
- non-`None`
2. Ensure cancellation of non-winners remains consistent with queue-jump and timeout behaviors.
3. Add/adjust tests explicitly for `None` non-winner behavior.
4. Add/adjust tests for taxonomy classes on timeout/cancel/abort/result-schema paths.

## Phase 8: Middleware lifecycle alignment pass

Files:
- `bubus/event_bus.py`
- `bubus/middlewares.py` (only if signatures/typing need sync)
- `tests/test_eventbus.py`

Changes:
1. Confirm event + result + handler-change hooks are emitted from centralized notifiers.
2. Confirm ordering remains:
- event: `pending -> started -> completed`
- result: `pending -> started -> completed`
3. Keep simple behavior:
- re-fires allowed
- middleware throws undefined behavior

## Phase 9: Verification matrix

Run:
```bash
python -m pytest tests/test_eventbus.py -xvs
python -m pytest tests/test_event_handler.py -xvs
python -m pytest tests/test_eventbus_timeout.py -xvs
python -m pytest tests/test_events_suck.py -xvs
python -m pytest tests/test_eventbus_dispatch_contextvars.py -xvs
python -m pytest tests/test_eventbus_dispatch_parent_tracking.py -xvs
python -m pytest tests/test_eventbus_performance.py -xvs
```

Add/adjust tests for:
1. middleware constructor auto-init from class entries
2. sequential hook order
3. queue-jump correctness under new context-manager seams
4. event hard-timeout behavior
5. taxonomy assertions for cancelled/aborted/timeout/result-schema
6. `first()` winner filtering behavior (`None` is non-winner)

## Done Criteria

1. Context-manager architecture is explicit and unified.
2. Lock manager has a stable interface seam with compatibility aliases.
3. Middleware path is normalized and centralized.
4. Event hard-timeout semantics are implemented and tested.
5. Queue-jump/lock correctness is preserved.
6. `first()` and taxonomy semantics match locked decisions.
