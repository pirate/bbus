"""
Tests for the unified find() method and tree traversal helpers.

Addresses GitHub Issues #10 (debouncing) and #15 (expect past + child_of).
"""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownLambdaType=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnusedVariable=false

import asyncio
from datetime import UTC, datetime

import pytest

from bubus import BaseEvent, EventBus


# Test event types
class ParentEvent(BaseEvent[str]):
    pass


class ChildEvent(BaseEvent[str]):
    pass


class GrandchildEvent(BaseEvent[str]):
    pass


class UnrelatedEvent(BaseEvent[str]):
    pass


class ScreenshotEvent(BaseEvent[str]):
    """Example event for debouncing tests."""

    target_id: str = ''
    full_page: bool = False


class NavigateEvent(BaseEvent[str]):
    """Example event for race condition tests."""

    url: str = ''


class TabCreatedEvent(BaseEvent[str]):
    """Example event that fires as result of navigation."""

    tab_id: str = ''


# =============================================================================
# Tree Traversal Helper Tests
# =============================================================================


class TestEventIsChildOf:
    """Tests for event_is_child_of() method."""

    async def test_direct_child_returns_true(self):
        """event_is_child_of returns True for direct parent-child relationship."""
        bus = EventBus()

        try:
            # Create parent-child relationship via dispatch inside handler
            child_event_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_event_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            child = child_event_ref[0]

            # Verify the relationship
            assert bus.event_is_child_of(child, parent) is True

        finally:
            await bus.stop(clear=True)

    async def test_grandchild_returns_true(self):
        """event_is_child_of returns True for grandparent relationship."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await bus.dispatch(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await bus.dispatch(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            grandchild = grandchild_ref[0]

            # Grandchild should be descendant of parent
            assert bus.event_is_child_of(grandchild, parent) is True

        finally:
            await bus.stop(clear=True)

    async def test_unrelated_events_returns_false(self):
        """event_is_child_of returns False for unrelated events."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'parent_done')
            bus.on(UnrelatedEvent, lambda e: 'unrelated_done')

            parent = await bus.dispatch(ParentEvent())
            unrelated = await bus.dispatch(UnrelatedEvent())

            assert bus.event_is_child_of(unrelated, parent) is False

        finally:
            await bus.stop(clear=True)

    async def test_same_event_returns_false(self):
        """event_is_child_of returns False when checking event against itself."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            event = await bus.dispatch(ParentEvent())

            assert bus.event_is_child_of(event, event) is False

        finally:
            await bus.stop(clear=True)

    async def test_reversed_relationship_returns_false(self):
        """event_is_child_of returns False when parent/child are reversed."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            child = child_ref[0]

            # Parent is NOT a child of child
            assert bus.event_is_child_of(parent, child) is False

        finally:
            await bus.stop(clear=True)


class TestEventIsParentOf:
    """Tests for event_is_parent_of() method."""

    async def test_direct_parent_returns_true(self):
        """event_is_parent_of returns True for direct parent-child relationship."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            child = child_ref[0]

            # Parent IS parent of child
            assert bus.event_is_parent_of(parent, child) is True

        finally:
            await bus.stop(clear=True)

    async def test_grandparent_returns_true(self):
        """event_is_parent_of returns True for grandparent relationship."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await bus.dispatch(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await bus.dispatch(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            grandchild = grandchild_ref[0]

            # Parent IS ancestor of grandchild
            assert bus.event_is_parent_of(parent, grandchild) is True

        finally:
            await bus.stop(clear=True)


# =============================================================================
# find() Basic Functionality Tests
# =============================================================================


class TestFindPastOnly:
    """Tests for find(past=True, future=False) - equivalent to query()."""

    async def test_max_history_zero_disables_past_but_future_still_works(self):
        """With max_history_size=0, future find resolves on dispatch but completed events are not searchable in past."""
        bus = EventBus(max_history_size=0)

        try:
            bus.on(ParentEvent, lambda e: 'done')

            find_future_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            await asyncio.sleep(0)

            dispatched = bus.dispatch(ParentEvent())
            found_future = await find_future_task
            assert found_future is not None
            assert found_future.event_id == dispatched.event_id

            await dispatched
            assert dispatched.event_id not in bus.event_history

            found_past = await bus.find(ParentEvent, past=True, future=False)
            assert found_past is None
        finally:
            await bus.stop(clear=True)

    async def test_returns_matching_event_from_history(self):
        """find(past=True, future=False) returns event from history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.dispatch(ParentEvent())

            # Find it in history (past=True = search all history)
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_float_filters_by_time_window(self):
        """find(past=0.1) only returns events from last 0.1 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an event
            _old_event = await bus.dispatch(ParentEvent())

            # Wait a bit
            await asyncio.sleep(0.15)

            # Dispatch another event
            new_event = await bus.dispatch(ParentEvent())

            # With a very short past window, should only find the new event
            found = await bus.find(ParentEvent, past=0.1, future=False)
            assert found is not None
            assert found.event_id == new_event.event_id

            # With a longer past window, should still find new event (most recent first)
            found = await bus.find(ParentEvent, past=1.0, future=False)
            assert found is not None
            assert found.event_id == new_event.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_float_returns_none_when_all_events_too_old(self):
        """find(past=0.05) returns None if all events are older than 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an event
            await bus.dispatch(ParentEvent())

            # Wait longer than our window
            await asyncio.sleep(0.15)

            # With very short past window, should find nothing
            found = await bus.find(ParentEvent, past=0.05, future=False)
            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_returns_none_when_no_match(self):
        """find(past=True, future=False) returns None when no matching event."""
        bus = EventBus()

        try:
            # No events dispatched
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_respects_where_filter(self):
        """find() applies where filter correctly."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch two events with different target_ids
            await bus.dispatch(ScreenshotEvent(target_id='tab1'))
            event2 = await bus.dispatch(ScreenshotEvent(target_id='tab2'))

            # Find only the one with target_id='tab2'
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab2',
                past=True,
                future=False,
            )

            assert found is not None
            assert found.event_id == event2.event_id

        finally:
            await bus.stop(clear=True)

    async def test_returns_most_recent_match(self):
        """find() returns most recent matching event from history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch multiple events
            await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.01)  # Ensure different timestamps
            event2 = await bus.dispatch(ParentEvent())

            # Should return the most recent
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is not None
            assert found.event_id == event2.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_ignores_in_progress_until_event_completes(self):
        """History search should only return completed events, never in-progress ones."""
        bus = EventBus()

        try:
            release_handler = asyncio.Event()

            async def slow_handler(event: ParentEvent) -> str:
                await release_handler.wait()
                return 'done'

            bus.on(ParentEvent, slow_handler)

            dispatched = bus.dispatch(ParentEvent())
            await asyncio.sleep(0.02)  # Let handler start.

            # In-progress event should not be returned by history search.
            found_while_running = await bus.find(ParentEvent, past=True, future=False)
            assert found_while_running is None

            release_handler.set()
            await dispatched
            await bus.wait_until_idle()

            found_after_completion = await bus.find(ParentEvent, past=True, future=False)
            assert found_after_completion is not None
            assert found_after_completion.event_id == dispatched.event_id
        finally:
            await bus.stop(clear=True)


class TestFindFutureOnly:
    """Tests for find(past=False, future=...) - equivalent to expect()."""

    async def test_waits_for_future_event(self):
        """find(past=False, future=1) waits for event to be dispatched."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Start waiting for event
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.dispatch(ParentEvent())

            find_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_future_float_timeout(self):
        """find(future=0.01) times out quickly when no event."""
        bus = EventBus()

        try:
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=0.01)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert elapsed < 0.1  # Should timeout quickly

        finally:
            await bus.stop(clear=True)

    async def test_ignores_past_events(self):
        """find(past=False, future=...) ignores events already in history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            await bus.dispatch(ParentEvent())

            # Should NOT find it (past=False), and timeout quickly
            found = await bus.find(ParentEvent, past=False, future=0.01)

            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_future_works_with_string_event_type(self):
        """find('EventName', ...) resolves using string keys, not just model classes."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.dispatch(ParentEvent())

            find_task = asyncio.create_task(bus.find('ParentEvent', past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert found.event_type == 'ParentEvent'
        finally:
            await bus.stop(clear=True)

    async def test_multiple_concurrent_find_waiters_resolve_correct_events(self):
        """Concurrent find() waiters should each resolve to the correct event."""
        bus = EventBus()

        try:
            # Keep one permanent handler so we can assert temporary find handlers are cleaned up.
            bus.on(ScreenshotEvent, lambda e: 'done')
            baseline_handler_count = len(bus.handlers.get('ScreenshotEvent', []))

            wait_for_a = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == 'tab-a',
                    past=False,
                    future=1,
                )
            )
            wait_for_b = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == 'tab-b',
                    past=False,
                    future=1,
                )
            )

            await asyncio.sleep(0.02)
            event_a = await bus.dispatch(ScreenshotEvent(target_id='tab-a'))
            event_b = await bus.dispatch(ScreenshotEvent(target_id='tab-b'))

            found_a, found_b = await asyncio.gather(wait_for_a, wait_for_b)

            assert found_a is not None
            assert found_b is not None
            assert found_a.event_id == event_a.event_id
            assert found_b.event_id == event_b.event_id

            # All temporary find handlers should be removed.
            assert len(bus.handlers.get('ScreenshotEvent', [])) == baseline_handler_count
        finally:
            await bus.stop(clear=True)

    async def test_find_future_resolves_before_handlers_complete(self):
        """find(future=...) resolves on dispatch, before slow handlers complete."""
        bus = EventBus()

        try:
            processing_complete = False

            async def slow_handler(event: ParentEvent) -> str:
                nonlocal processing_complete
                await asyncio.sleep(0.1)
                processing_complete = True
                return 'done'

            bus.on(ParentEvent, slow_handler)

            find_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            await asyncio.sleep(0.01)

            dispatched = bus.dispatch(ParentEvent())
            found = await find_task

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert processing_complete is False
            assert found.event_status in ('pending', 'started')

            await bus.wait_until_idle()
            assert processing_complete is True
        finally:
            await bus.stop(clear=True)


class TestFindNeitherPastNorFuture:
    """Tests for find(past=False, future=False) - should return None."""

    async def test_returns_none_immediately(self):
        """find(past=False, future=False) returns None immediately."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event
            await bus.dispatch(ParentEvent())

            # With both past and future disabled, should return None
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert elapsed < 0.1  # Should be instant

        finally:
            await bus.stop(clear=True)


class TestFindPastAndFuture:
    """Tests for find(past=..., future=...) - combined search."""

    async def test_returns_past_event_immediately(self):
        """find(past=True, future=5) returns past event without waiting."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.dispatch(ParentEvent())

            # Should find it immediately from history
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=True, future=5)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert elapsed < 0.1  # Should be nearly instant

        finally:
            await bus.stop(clear=True)

    async def test_waits_for_future_when_no_past_match(self):
        """find(past=True, future=1) waits for future if no past match."""
        bus = EventBus()

        try:
            bus.on(ChildEvent, lambda e: 'done')

            # Different event type in history
            bus.on(ParentEvent, lambda e: 'done')
            await bus.dispatch(ParentEvent())

            # Start waiting for ChildEvent (not in history)
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.dispatch(ChildEvent())

            find_task = asyncio.create_task(bus.find(ChildEvent, past=True, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_and_future_independent_control(self):
        """past=0.05, future=0.05 uses different windows for each."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.15)

            # With short past window (0.05s), old event won't be found
            # With short future window (0.05s), will timeout
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=0.05, future=0.05)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            # Should have waited ~0.05s for future
            assert 0.04 < elapsed < 0.15

        finally:
            await bus.stop(clear=True)

    async def test_past_true_future_float(self):
        """past=True searches all history, future=0.1 waits up to 0.1s."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            dispatched = await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.15)

            # past=True should find the old event (no time window)
            found = await bus.find(ParentEvent, past=True, future=0.1)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_float_future_true_would_wait_forever(self):
        """past=0.05 with old events + future=True - verify past window works."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.15)

            # past=0.05 won't find old event, but we dispatch a new one
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.dispatch(ParentEvent())

            find_task = asyncio.create_task(bus.find(ParentEvent, past=0.05, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            # Should find the new event from future wait
            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)


# =============================================================================
# find() with child_of Tests
# =============================================================================


class TestFindWithChildOf:
    """Tests for find() with child_of parameter."""

    async def test_returns_child_of_specified_parent(self):
        """find(child_of=parent) returns event that is child of parent."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            # Find child of parent
            found = await bus.find(ChildEvent, child_of=parent, past=True, future=False)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.stop(clear=True)

    async def test_returns_none_for_non_child(self):
        """find(child_of=parent) returns None if event is not a child."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'parent_done')
            bus.on(UnrelatedEvent, lambda e: 'unrelated_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.dispatch(UnrelatedEvent())

            # Should not find UnrelatedEvent as child of parent
            found = await bus.find(UnrelatedEvent, child_of=parent, past=True, future=False)

            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_finds_grandchild(self):
        """find(child_of=grandparent) returns grandchild event."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await bus.dispatch(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await bus.dispatch(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            # Find grandchild of parent
            found = await bus.find(GrandchildEvent, child_of=parent, past=True, future=False)

            assert found is not None
            assert found.event_id == grandchild_ref[0].event_id

        finally:
            await bus.stop(clear=True)

    async def test_child_of_works_across_forwarded_buses(self):
        """find(child_of=parent) works when events are forwarded across buses."""
        main_bus = EventBus(name='MainBus')
        auth_bus = EventBus(name='AuthBus')

        try:
            child_ref: list[BaseEvent] = []

            # Forward ParentEvent from main_bus to auth_bus
            main_bus.on(ParentEvent, auth_bus.dispatch)

            # auth_bus handles ParentEvent and dispatches a ChildEvent
            async def auth_handler(event: ParentEvent) -> str:
                child = await auth_bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'auth_done'

            auth_bus.on(ParentEvent, auth_handler)
            auth_bus.on(ChildEvent, lambda e: 'child_done')

            # Dispatch on main_bus, which forwards to auth_bus
            parent = await main_bus.dispatch(ParentEvent())
            await main_bus.wait_until_idle()
            await auth_bus.wait_until_idle()

            # Find child event on auth_bus using parent from main_bus
            found = await auth_bus.find(ChildEvent, child_of=parent, past=5, future=5)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await main_bus.stop(clear=True)
            await auth_bus.stop(clear=True)


# =============================================================================
# expect() Backwards Compatibility Tests
# =============================================================================


class TestExpectBackwardsCompatibility:
    """Tests to ensure expect() still works with old API."""

    async def test_expect_waits_for_future_event(self):
        """expect() still waits for future events (existing behavior)."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.dispatch(ParentEvent())

            expect_task = asyncio.create_task(bus.expect(ParentEvent, timeout=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(expect_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_expect_with_include_filter(self):
        """expect() with include parameter still works."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            async def dispatch_events():
                await asyncio.sleep(0.02)
                await bus.dispatch(ScreenshotEvent(target_id='wrong'))
                await asyncio.sleep(0.02)
                return await bus.dispatch(ScreenshotEvent(target_id='correct'))

            expect_task = asyncio.create_task(
                bus.expect(
                    ScreenshotEvent,
                    include=lambda e: e.target_id == 'correct',
                    timeout=1,
                )
            )
            dispatch_task = asyncio.create_task(dispatch_events())

            found, dispatched = await asyncio.gather(expect_task, dispatch_task)

            assert found is not None
            assert found.target_id == 'correct'

        finally:
            await bus.stop(clear=True)

    async def test_expect_with_exclude_filter(self):
        """expect() with exclude parameter still works."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            async def dispatch_events():
                await asyncio.sleep(0.02)
                await bus.dispatch(ScreenshotEvent(target_id='excluded'))
                await asyncio.sleep(0.02)
                return await bus.dispatch(ScreenshotEvent(target_id='included'))

            expect_task = asyncio.create_task(
                bus.expect(
                    ScreenshotEvent,
                    exclude=lambda e: e.target_id == 'excluded',
                    timeout=1,
                )
            )
            dispatch_task = asyncio.create_task(dispatch_events())

            found, dispatched = await asyncio.gather(expect_task, dispatch_task)

            assert found is not None
            assert found.target_id == 'included'

        finally:
            await bus.stop(clear=True)

    async def test_expect_with_past_true(self):
        """expect(past=True) finds already-dispatched events."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.dispatch(ParentEvent())

            # expect with past=True should find it
            found = await bus.expect(ParentEvent, past=True, timeout=5)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_expect_with_past_float(self):
        """expect(past=5.0) searches last 5 seconds of history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.dispatch(ParentEvent())

            # expect with past=5.0 should find recent event
            found = await bus.expect(ParentEvent, past=5.0, timeout=1)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_expect_with_child_of(self):
        """expect(child_of=parent) filters by parent relationship."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            # expect with child_of and past=True
            found = await bus.expect(ChildEvent, child_of=parent, past=True, timeout=5)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.stop(clear=True)


# =============================================================================
# Debouncing Pattern Tests (Issue #10)
# =============================================================================


class TestDebouncingPattern:
    """Tests for the debouncing pattern: find() or dispatch()."""

    async def test_returns_existing_fresh_event(self):
        """Pattern returns existing event when fresh."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch a screenshot
            original = await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Use debouncing pattern - should return the existing event
            is_fresh = lambda e: (datetime.now(UTC) - e.event_completed_at).seconds < 5
            result = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1' and is_fresh(e),
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            assert result.event_id == original.event_id

        finally:
            await bus.stop(clear=True)

    async def test_dispatches_new_when_no_match(self):
        """Pattern dispatches new event when no matching event in history."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # No existing events - should dispatch new
            result = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            assert result is not None
            assert result.target_id == 'tab1'
            assert result.event_status == 'completed'

        finally:
            await bus.stop(clear=True)

    async def test_dispatches_new_when_stale(self):
        """Pattern dispatches new event when existing is stale."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch an event
            await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Filter that marks all events as stale
            is_fresh = lambda e: False  # Nothing is fresh

            result = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1' and is_fresh(e),
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Should be a new event (different ID)
            assert result is not None
            # Both events should be in history now
            screenshots = [e for e in bus.event_history.values() if isinstance(e, ScreenshotEvent)]
            assert len(screenshots) == 2

        finally:
            await bus.stop(clear=True)

    async def test_find_past_only_returns_immediately_without_waiting(self):
        """find(past=True, future=False) returns immediately, never waits."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # No events in history - find should return None instantly
            start = datetime.now(UTC)
            result = await bus.find(ParentEvent, past=True, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result is None
            assert elapsed < 0.05  # Should be nearly instant (< 50ms)

        finally:
            await bus.stop(clear=True)

    async def test_find_past_float_returns_immediately_without_waiting(self):
        """find(past=5, future=False) returns immediately, never waits."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # No events in history - find should return None instantly
            start = datetime.now(UTC)
            result = await bus.find(ParentEvent, past=5, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result is None
            assert elapsed < 0.05  # Should be nearly instant (< 50ms)

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_without_waiting_finds_existing(self):
        """Or-chain pattern finds existing events without blocking."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch first event
            original = await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Or-chain should find existing event instantly
            start = datetime.now(UTC)
            result = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))
            elapsed = (datetime.now(UTC) - start).total_seconds()

            # Should return existing event
            assert result.event_id == original.event_id
            # Should be fast (no waiting)
            assert elapsed < 0.1

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_without_waiting_dispatches_when_no_match(self):
        """Or-chain pattern dispatches new event when no match, still fast."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # No matching events - should dispatch new one
            start = datetime.now(UTC)
            result = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))
            elapsed = (datetime.now(UTC) - start).total_seconds()

            # Should have dispatched new event
            assert result is not None
            assert result.target_id == 'tab1'
            # Should be fast (find returned None immediately, then dispatch ran)
            assert elapsed < 0.1

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_multiple_sequential_lookups(self):
        """Multiple or-chain lookups work without blocking."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Multiple sequential debouncing calls
            start = datetime.now(UTC)

            # First call - dispatches new
            result1 = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Second call - finds existing
            result2 = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab1'))

            # Third call - dispatches new (different target)
            result3 = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab2',
                past=True,
                future=False,
            ) or await bus.dispatch(ScreenshotEvent(target_id='tab2'))

            elapsed = (datetime.now(UTC) - start).total_seconds()

            # First two should be same event
            assert result1.event_id == result2.event_id
            # Third should be different
            assert result3.event_id != result1.event_id
            assert result3.target_id == 'tab2'
            # All operations should be fast
            assert elapsed < 0.2

        finally:
            await bus.stop(clear=True)

    async def test_find_without_await_is_a_coroutine(self):
        """find() without await returns a coroutine that can be awaited."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Call find without await - should return a coroutine
            coro = bus.find(ParentEvent, past=True, future=False)

            # Verify it's a coroutine
            import inspect

            assert inspect.iscoroutine(coro)

            # Now await it
            result = await coro

            assert result is None

        finally:
            await bus.stop(clear=True)


# =============================================================================
# Race Condition Fix Tests (Issue #15)
# =============================================================================


class TestRaceConditionFix:
    """Tests for the race condition fix where event fires before expect()."""

    async def test_find_catches_already_fired_event(self):
        """find(past=True) catches event that fired before the call."""
        bus = EventBus()

        try:
            tab_ref: list[BaseEvent] = []

            async def navigate_handler(event: NavigateEvent) -> str:
                # This synchronously creates the tab event
                tab = await bus.dispatch(TabCreatedEvent(tab_id='new_tab'))
                tab_ref.append(tab)
                return 'navigate_done'

            bus.on(NavigateEvent, navigate_handler)
            bus.on(TabCreatedEvent, lambda e: 'tab_created')

            # Dispatch navigation - tab event fires during handler
            nav_event = await bus.dispatch(NavigateEvent(url='https://example.com'))

            # By now TabCreatedEvent has already fired
            # Using find(past=True) should catch it
            found = await bus.find(TabCreatedEvent, child_of=nav_event, past=True, future=False)

            assert found is not None
            assert found.event_id == tab_ref[0].event_id

        finally:
            await bus.stop(clear=True)

    async def test_child_of_filters_to_correct_parent(self):
        """child_of correctly filters to events from the right parent."""
        bus = EventBus()

        try:

            async def navigate_handler(event: NavigateEvent) -> str:
                await bus.dispatch(TabCreatedEvent(tab_id=f'tab_for_{event.url}'))
                return 'navigate_done'

            bus.on(NavigateEvent, navigate_handler)
            bus.on(TabCreatedEvent, lambda e: 'tab_created')

            # Two navigations, each creates a tab
            nav1 = await bus.dispatch(NavigateEvent(url='site1'))
            nav2 = await bus.dispatch(NavigateEvent(url='site2'))

            # Find tab created by nav1 specifically
            tab1 = await bus.find(TabCreatedEvent, child_of=nav1, past=True, future=False)

            # Find tab created by nav2 specifically
            tab2 = await bus.find(TabCreatedEvent, child_of=nav2, past=True, future=False)

            assert tab1 is not None
            assert tab2 is not None
            assert tab1.tab_id == 'tab_for_site1'
            assert tab2.tab_id == 'tab_for_site2'

        finally:
            await bus.stop(clear=True)


# =============================================================================
# New Parameter Combination Tests
# =============================================================================


class TestNewParameterCombinations:
    """Tests for the new bool | float parameter combinations."""

    async def test_past_true_future_false_searches_all_history(self):
        """past=True, future=False searches all history instantly."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event and wait
            dispatched = await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.1)

            # Should find old event with past=True
            found = await bus.find(ParentEvent, past=True, future=False)
            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.stop(clear=True)

    async def test_past_float_future_false_filters_by_age(self):
        """past=0.05, future=False only searches last 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event
            await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.1)  # Make it old

            # past=0.05 means "events in last 0.05 seconds" = nothing old
            found = await bus.find(ParentEvent, past=0.05, future=False)
            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_past_false_future_float_waits_for_timeout(self):
        """past=False, future=0.05 waits up to 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=0.05)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert 0.04 < elapsed < 0.15  # Should wait ~0.05s

        finally:
            await bus.stop(clear=True)

    async def test_past_true_future_true_searches_all_and_waits_forever(self):
        """past=True, future=True searches all history, would wait forever."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            dispatched = await bus.dispatch(ParentEvent())
            await asyncio.sleep(0.1)

            # past=True should find the old event immediately
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=True, future=True)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert elapsed < 0.1  # Should be instant (found in past)

        finally:
            await bus.stop(clear=True)

    async def test_find_with_where_and_past_float(self):
        """where filter combined with past=float works correctly."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch events with different target_ids
            await bus.dispatch(ScreenshotEvent(target_id='tab1'))
            await asyncio.sleep(0.15)
            event2 = await bus.dispatch(ScreenshotEvent(target_id='tab2'))

            # Find with both where filter and past window
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab2',
                past=0.1,  # Only search last 0.1 seconds
                future=False,
            )
            assert found is not None
            assert found.event_id == event2.event_id

            # tab1 is too old for the past window
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'tab1',
                past=0.1,
                future=False,
            )
            assert found is None

        finally:
            await bus.stop(clear=True)

    async def test_find_with_child_of_and_past_float(self):
        """child_of filter combined with past=float works correctly."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ChildEvent())
                child_ref.append(child)
                return 'done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            # Find child with past window - should work since event is fresh
            found = await bus.find(
                ChildEvent,
                child_of=parent,
                past=5,  # 5 second window
                future=False,
            )
            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.stop(clear=True)

    async def test_find_with_all_parameters(self):
        """All parameters combined work correctly."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.dispatch(ScreenshotEvent(target_id='child_tab'))
                child_ref.append(child)
                return 'done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ScreenshotEvent, lambda e: 'done')

            parent = await bus.dispatch(ParentEvent())
            await bus.wait_until_idle()

            # Find with all parameters
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == 'child_tab',
                child_of=parent,
                past=5,
                future=False,
            )
            assert found is not None
            assert found.event_id == child_ref[0].event_id
            assert found.target_id == 'child_tab'

        finally:
            await bus.stop(clear=True)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
