# pyright: basic
"""
Comprehensive tests for the EventBus implementation.

Tests cover:
- Basic event enqueueing and processing
- Sync and async contexts
- Handler registration and execution
- FIFO ordering
- Parallel handler execution
- Error handling
- Write-ahead logging
- Serialization
- Batch operations
"""

import asyncio
import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import Field

from bubus import BaseEvent, EventBus, SQLiteHistoryMirrorMiddleware
from bubus.middlewares import (
    AutoErrorEventMiddleware,
    AutoHandlerChangeEventMiddleware,
    AutoReturnEventMiddleware,
    BusHandlerRegisteredEvent,
    BusHandlerUnregisteredEvent,
    EventBusMiddleware,
    LoggerEventBusMiddleware,
    OtelTracingMiddleware,
    WALEventBusMiddleware,
)


class CreateAgentTaskEvent(BaseEvent):
    """Test event model for creating an agent task"""

    user_id: str
    agent_session_id: str
    llm_model: str
    task: str


# Test event models
class UserActionEvent(BaseEvent):
    """Test event model for user actions"""

    action: str
    user_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class SystemEventModel(BaseEvent):
    """Test event model for system events"""

    event_name: str
    severity: str = 'info'
    details: dict[str, Any] = Field(default_factory=dict)


class MockAgent:
    """Mock agent for testing"""

    def __init__(self, name: str = 'TestAgent'):
        self.name = name
        self.events_received = []


@pytest.fixture
async def eventbus():
    """Create an event bus for testing"""
    bus = EventBus(max_history_size=10000)  # Increase history limit for tests
    yield bus
    await bus.stop()


@pytest.fixture
async def parallel_eventbus():
    """Create an event bus with parallel handler execution"""
    bus = EventBus(event_handler_concurrency='parallel')
    yield bus
    await bus.stop()


@pytest.fixture
def mock_agent():
    """Create a mock agent"""
    return MockAgent()


class TestEventBusBasics:
    """Test basic EventBus functionality"""

    async def test_eventbus_initialization(self, mock_agent: MockAgent):
        """Test that EventBus initializes correctly"""
        bus = EventBus()

        assert bus._is_running is False
        assert bus._runloop_task is None
        assert len(bus.event_history) == 0
        assert len(bus.handlers_by_key.get('*', [])) == 0  # No default logger anymore
        assert bus.max_history_drop is False

    def test_eventbus_accepts_custom_id(self):
        """EventBus constructor accepts id=... to set bus UUID."""
        custom_id = '018f8e40-1234-7000-8000-000000001234'
        bus = EventBus(id=custom_id)

        assert bus.id == custom_id
        assert bus.label.endswith('#1234')

    async def test_auto_start_and_stop(self, mock_agent):
        """Test auto-start functionality and stopping the event bus"""
        bus = EventBus()

        # Should not be running initially
        assert bus._is_running is False
        assert bus._runloop_task is None

        # Auto-start by emitting an event
        bus.dispatch(UserActionEvent(action='test', user_id='user123'))
        await bus.wait_until_idle()

        # Should be running after auto-start
        assert bus._is_running is True
        assert bus._runloop_task is not None

        # Stop the bus
        await bus.stop()
        assert bus._is_running is False

        # Stop again should be idempotent
        await bus.stop()
        assert bus._is_running is False


class TestEventEnqueueing:
    """Test event enqueueing functionality"""

    async def test_emit_and_result(self, eventbus):
        """Test event emission in async and sync contexts, and result() pattern"""
        # Test async emission
        event = UserActionEvent(action='login', user_id='user123', event_timeout=1)
        queued = eventbus.dispatch(event)

        # Check immediate result
        assert isinstance(queued, UserActionEvent)
        assert queued.event_type == 'UserActionEvent'
        assert queued.action == 'login'
        assert queued.user_id == 'user123'
        assert queued.event_id is not None
        assert queued.event_created_at is not None
        assert queued.event_started_at is None  # Not started yet
        assert queued.event_completed_at is None  # Not completed yet
        assert queued.event_status == 'pending'

        # Test result() pattern
        processed = await queued
        assert processed.event_started_at is not None
        assert processed.event_completed_at is not None
        assert processed.event_status == 'completed'
        # Check that we have no results (no default handler anymore)
        assert len(processed.event_results) == 0

        # Check event history
        assert len(eventbus.event_history) == 1

    def test_emit_sync(self, mock_agent):
        """Test sync event emission"""
        bus = EventBus()
        event = SystemEventModel(event_name='startup', severity='info')

        with pytest.raises(RuntimeError) as e:
            bus.dispatch(event)

        assert 'no event loop is running' in str(e.value)
        assert len(bus.event_history) == 0

    async def test_emit_alias_dispatches_event(self, eventbus):
        """Test EventBus.emit() alias dispatches and processes events."""
        handled_event_ids: list[str] = []

        async def user_handler(event: UserActionEvent) -> str:
            handled_event_ids.append(event.event_id)
            return 'handled'

        eventbus.on(UserActionEvent, user_handler)

        event = UserActionEvent(action='alias', user_id='user123')
        queued = eventbus.emit(event)

        assert queued is event
        completed = await queued
        assert completed.event_status == 'completed'
        assert handled_event_ids == [event.event_id]
        assert eventbus.label in completed.event_path

    async def test_unbounded_history_disables_history_rejection(self):
        """When max_history_size=None, dispatch should not reject on history size."""
        bus = EventBus(name='NoLimitBus', max_history_size=None)

        processed = 0

        async def slow_handler(event: BaseEvent) -> None:
            nonlocal processed
            await asyncio.sleep(0.01)
            processed += 1

        bus.on('SlowEvent', slow_handler)

        events: list[BaseEvent] = []

        try:
            for _ in range(150):
                events.append(bus.dispatch(BaseEvent(event_type='SlowEvent')))

            await asyncio.gather(*events)
            await bus.wait_until_idle()
            assert processed == 150
        finally:
            await bus.stop(clear=True)

    async def test_zero_history_size_keeps_inflight_and_drops_on_completion(self):
        """max_history_size=0 keeps in-flight events but removes them as soon as they complete."""
        bus = EventBus(name='ZeroHistoryBus', max_history_size=0, max_history_drop=False)

        first_handler_started = asyncio.Event()
        release_handlers = asyncio.Event()

        async def slow_handler(_event: BaseEvent[Any]) -> None:
            first_handler_started.set()
            await release_handlers.wait()

        bus.on('SlowEvent', slow_handler)

        try:
            first = bus.dispatch(BaseEvent(event_type='SlowEvent'))
            await asyncio.wait_for(first_handler_started.wait(), timeout=1.0)
            second = bus.dispatch(BaseEvent(event_type='SlowEvent'))

            assert first.event_id in bus.event_history
            assert second.event_id in bus.event_history

            release_handlers.set()
            await asyncio.gather(first, second)
            await bus.wait_until_idle()

            assert len(bus.event_history) == 0
        finally:
            await bus.stop(clear=True)


class TestHandlerRegistration:
    """Test handler registration and execution"""

    async def test_handler_registration(self, eventbus):
        """Test handler registration via string, model class, and wildcard"""
        results = {'specific': [], 'model': [], 'universal': []}

        # Handler for specific event type by string
        async def user_handler(event: UserActionEvent) -> str:
            results['specific'].append(event.action)
            return 'user_handled'

        # Handler for event type by model class
        async def system_handler(event: SystemEventModel) -> str:
            results['model'].append(event.event_name)
            return 'system_handled'

        # Universal handler
        async def universal_handler(event: BaseEvent) -> str:
            results['universal'].append(event.event_type)
            return 'universal'

        # Register handlers
        eventbus.on('UserActionEvent', user_handler)
        eventbus.on(SystemEventModel, system_handler)
        eventbus.on('*', universal_handler)

        # Emit events
        eventbus.dispatch(UserActionEvent(action='login', user_id='u1'))
        eventbus.dispatch(SystemEventModel(event_name='startup'))
        await eventbus.wait_until_idle()

        # Verify all handlers were called correctly
        assert results['specific'] == ['login']
        assert results['model'] == ['startup']
        assert set(results['universal']) == {'UserActionEvent', 'SystemEventModel'}

    async def test_class_matcher_matches_generic_base_event_by_event_type(self, eventbus):
        """Class listeners should still match generic BaseEvent payloads by event_type string."""

        class DifferentNameFromClass(BaseEvent):
            pass

        seen: list[str] = []

        async def class_handler(event: BaseEvent) -> None:
            seen.append(f'class:{event.event_type}')

        async def string_handler(event: BaseEvent) -> None:
            seen.append(f'string:{event.event_type}')

        async def wildcard_handler(event: BaseEvent) -> None:
            seen.append(f'wildcard:{event.event_type}')

        eventbus.on(DifferentNameFromClass, class_handler)
        eventbus.on('DifferentNameFromClass', string_handler)
        eventbus.on('*', wildcard_handler)

        eventbus.dispatch(BaseEvent(event_type='DifferentNameFromClass'))
        await eventbus.wait_until_idle()

        assert seen == [
            'class:DifferentNameFromClass',
            'string:DifferentNameFromClass',
            'wildcard:DifferentNameFromClass',
        ]
        assert len(eventbus.handlers_by_key.get('DifferentNameFromClass', [])) == 2

    async def test_multiple_handlers_parallel(self, parallel_eventbus):
        """Test that multiple handlers run in parallel"""
        eventbus = parallel_eventbus
        start_times = []
        end_times = []

        async def slow_handler_1(event: BaseEvent) -> str:
            start_times.append(('h1', time.time()))
            await asyncio.sleep(0.1)
            end_times.append(('h1', time.time()))
            return 'handler1'

        async def slow_handler_2(event: BaseEvent) -> str:
            start_times.append(('h2', time.time()))
            await asyncio.sleep(0.1)
            end_times.append(('h2', time.time()))
            return 'handler2'

        # Subscribe both handlers
        eventbus.on('UserActionEvent', slow_handler_1)
        eventbus.on('UserActionEvent', slow_handler_2)

        # Emit event and wait
        start = time.time()
        event = await eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))
        duration = time.time() - start

        # Check handlers ran in parallel (should take ~0.1s, not 0.2s)
        assert duration < 0.15
        assert len(start_times) == 2
        assert len(end_times) == 2

        # Check results
        handler1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler_1')), None)
        handler2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler_2')), None)
        assert handler1_result is not None and handler1_result.result == 'handler1'
        assert handler2_result is not None and handler2_result.result == 'handler2'

    def test_handler_can_be_sync_or_async(self, mock_agent):
        """Test that both sync and async handlers are accepted"""
        bus = EventBus()

        def sync_handler(event: BaseEvent) -> str:
            return 'sync'

        async def async_handler(event: BaseEvent) -> str:
            return 'async'

        # Both should work
        bus.on('TestEvent', sync_handler)
        bus.on('TestEvent', async_handler)

        # Check both were registered
        assert len(bus.handlers_by_key.get('TestEvent', [])) == 2

    async def test_class_and_instance_method_handlers(self, eventbus):
        """Test using class and instance methods as handlers"""
        results = []

        class EventProcessor:
            def __init__(self, name: str, value: int):
                self.name = name
                self.value = value

            def sync_method_handler(self, event: UserActionEvent) -> dict:
                """Sync instance method handler"""
                results.append(f'{self.name}_sync')
                return {'processor': self.name, 'value': self.value, 'action': event.action}

            async def async_method_handler(self, event: UserActionEvent) -> dict:
                """Async instance method handler"""
                await asyncio.sleep(0.01)  # Simulate some async work
                results.append(f'{self.name}_async')
                return {'processor': self.name, 'value': self.value * 2, 'action': event.action}

            @classmethod
            def class_method_handler(cls, event: UserActionEvent) -> str:
                """Class method handler"""
                results.append('classmethod')
                return f'Handled by {cls.__name__}'

            @staticmethod
            def static_method_handler(event: UserActionEvent) -> str:
                """Static method handler"""
                results.append('staticmethod')
                return 'Handled by static method'

        # Create instances
        processor1 = EventProcessor('Processor1', 10)
        processor2 = EventProcessor('Processor2', 20)

        # Register instance methods (suppress warning about same-named handlers from different instances)
        import warnings

        eventbus.on(UserActionEvent, processor1.sync_method_handler)
        eventbus.on(UserActionEvent, processor1.async_method_handler)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            eventbus.on(UserActionEvent, processor2.sync_method_handler)

        # Register class and static methods
        eventbus.on('UserActionEvent', EventProcessor.class_method_handler)
        eventbus.on('UserActionEvent', EventProcessor.static_method_handler)

        # Dispatch event
        event = UserActionEvent(action='test_methods', user_id='u123')
        completed_event = await eventbus.dispatch(event)

        # Verify all handlers were called
        assert len(results) == 5
        assert 'Processor1_sync' in results
        assert 'Processor1_async' in results
        assert 'Processor2_sync' in results
        assert 'classmethod' in results
        assert 'staticmethod' in results

        # Verify results contain expected data
        results_list = await completed_event.event_results_list()

        # Find processor1 sync result
        p1_sync_result = next(
            r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor1' and r.get('value') == 10
        )
        assert p1_sync_result['action'] == 'test_methods'

        # Find processor1 async result (value doubled)
        p1_async_result = next(
            r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor1' and r.get('value') == 20
        )
        assert p1_async_result['action'] == 'test_methods'

        # Find processor2 sync result
        p2_sync_result = next(r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor2')
        assert p2_sync_result['value'] == 20
        assert p2_sync_result['action'] == 'test_methods'

        # Verify class and static method results
        assert 'Handled by EventProcessor' in results_list
        assert 'Handled by static method' in results_list


class TestEventForwarding:
    """Tests for event forwarding between buses."""

    @pytest.mark.asyncio
    async def test_forwarding_loop_prevention(self):
        bus_a = EventBus(name='ForwardBusA')
        bus_b = EventBus(name='ForwardBusB')
        bus_c = EventBus(name='ForwardBusC')

        class LoopEvent(BaseEvent[str]):
            pass

        seen: dict[str, int] = {'A': 0, 'B': 0, 'C': 0}

        async def handler_a(event: LoopEvent) -> str:
            seen['A'] += 1
            return 'handled-a'

        async def handler_b(event: LoopEvent) -> str:
            seen['B'] += 1
            return 'handled-b'

        async def handler_c(event: LoopEvent) -> str:
            seen['C'] += 1
            return 'handled-c'

        bus_a.on(LoopEvent, handler_a)
        bus_b.on(LoopEvent, handler_b)
        bus_c.on(LoopEvent, handler_c)

        # Create a forwarding cycle A -> B -> C -> A, which should be broken automatically.
        bus_a.on('*', bus_b.dispatch)
        bus_b.on('*', bus_c.dispatch)
        bus_c.on('*', bus_a.dispatch)

        try:
            event = await bus_a.dispatch(LoopEvent())

            await bus_a.wait_until_idle()
            await bus_b.wait_until_idle()
            await bus_c.wait_until_idle()

            assert seen == {'A': 1, 'B': 1, 'C': 1}
            assert event.event_path == [bus_a.label, bus_b.label, bus_c.label]
        finally:
            await bus_a.stop(clear=True)
            await bus_b.stop(clear=True)
            await bus_c.stop(clear=True)


class TestFIFOOrdering:
    """Test FIFO event processing"""

    async def test_fifo_with_varying_handler_delays(self, eventbus):
        """Test FIFO order is maintained with varying handler processing times"""
        processed_order = []
        handler_start_times = []

        async def handler(event: UserActionEvent) -> int:
            order = event.metadata.get('order', -1)
            handler_start_times.append((order, asyncio.get_event_loop().time()))
            # Variable delays to test ordering
            if order % 2 == 0:
                await asyncio.sleep(0.05)  # Even events take longer
            else:
                await asyncio.sleep(0.01)  # Odd events are quick
            processed_order.append(order)
            return order

        eventbus.on('UserActionEvent', handler)

        # Emit 20 events rapidly
        for i in range(20):
            eventbus.dispatch(UserActionEvent(action=f'test_{i}', user_id='u1', metadata={'order': i}))

        await eventbus.wait_until_idle()

        # Verify FIFO order maintained
        assert processed_order == list(range(20))
        # Verify handler start times are in order
        for i in range(1, len(handler_start_times)):
            assert handler_start_times[i][1] >= handler_start_times[i - 1][1]


class TestErrorHandling:
    """Test error handling in handlers"""

    async def test_error_handling(self, eventbus):
        """Test handler error capture and isolation"""
        results = []

        async def failing_handler(event: BaseEvent) -> str:
            raise ValueError('Expected to fail - testing error handling in event handlers')

        async def working_handler(event: BaseEvent) -> str:
            results.append('success')
            return 'worked'

        # Register both handlers
        eventbus.on('UserActionEvent', failing_handler)
        eventbus.on('UserActionEvent', working_handler)

        # Emit and wait for result
        event = await eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))

        # Verify error capture and isolation
        failing_result = next((r for r in event.event_results.values() if r.handler_name.endswith('failing_handler')), None)
        assert failing_result is not None
        assert failing_result.status == 'error'
        assert 'Expected to fail' in str(failing_result.error)
        working_result = next((r for r in event.event_results.values() if r.handler_name.endswith('working_handler')), None)
        assert working_result is not None
        assert working_result.result == 'worked'
        assert results == ['success']

    async def test_event_result_raises_exception_group_when_multiple_handlers_fail(self, eventbus):
        """event_result() should raise ExceptionGroup when multiple handler failures exist."""

        async def failing_handler_one(event: BaseEvent) -> str:
            raise ValueError('first failure')

        async def failing_handler_two(event: BaseEvent) -> str:
            raise RuntimeError('second failure')

        eventbus.on('UserActionEvent', failing_handler_one)
        eventbus.on('UserActionEvent', failing_handler_two)

        event = await eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))

        with pytest.raises(ExceptionGroup) as exc_info:
            await event.event_result()

        grouped_errors = exc_info.value.exceptions
        assert len(grouped_errors) == 2
        assert {type(err) for err in grouped_errors} == {ValueError, RuntimeError}

    async def test_event_result_single_handler_error_raises_original_exception(self, eventbus):
        """event_result() should preserve original exception type when only one handler fails."""

        async def failing_handler(event: BaseEvent) -> str:
            raise ValueError('single failure')

        eventbus.on('UserActionEvent', failing_handler)

        event = await eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))

        with pytest.raises(ValueError, match='single failure'):
            await event.event_result()


class TestBatchOperations:
    """Test batch event operations"""

    async def test_batch_emit_with_gather(self, eventbus):
        """Test batch event emission with asyncio.gather"""
        events = [
            UserActionEvent(action='login', user_id='u1'),
            SystemEventModel(event_name='startup'),
            UserActionEvent(action='logout', user_id='u1'),
        ]

        # Enqueue batch
        emitted_events = [eventbus.dispatch(event) for event in events]
        results = await asyncio.gather(*emitted_events)

        # Check all processed
        assert len(results) == 3
        for result in results:
            assert result.event_completed_at is not None


class TestWriteAheadLog:
    """Test write-ahead logging functionality"""

    async def test_write_ahead_log_captures_all_events(self, eventbus):
        """Test that all events are captured in write-ahead log"""
        # Emit several events
        events = []
        for i in range(5):
            event = UserActionEvent(action=f'action_{i}', user_id='u1')
            events.append(eventbus.dispatch(event))

        await eventbus.wait_until_idle()

        # Check write-ahead log
        log = eventbus.event_history.copy()
        assert len(log) == 5
        for i, event in enumerate(log.values()):
            assert event.action == f'action_{i}'

        # Check event state properties
        completed = eventbus.events_completed
        pending = eventbus.events_pending
        processing = eventbus.events_started
        assert len(completed) + len(pending) + len(processing) == len(log)
        assert len(completed) == 5  # All events should be completed
        assert len(pending) == 0  # No events should be pending
        assert len(processing) == 0  # No events should be processing


class TestEventCompletion:
    """Test event completion tracking"""

    async def test_wait_for_result(self, eventbus):
        """Test waiting for event completion"""
        completion_order = []

        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(0.1)
            completion_order.append('handler_done')
            return 'done'

        eventbus.on('UserActionEvent', slow_handler)

        # Enqueue without waiting
        event = eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))
        completion_order.append('enqueue_done')

        # Wait for completion
        event = await event
        completion_order.append('wait_done')

        # Check order
        assert completion_order == ['enqueue_done', 'handler_done', 'wait_done']
        assert event.event_completed_at is not None


class TestEdgeCases:
    """Test edge cases and special scenarios"""

    async def test_stop_with_pending_events(self, mock_agent):
        """Test stopping event bus with events still in queue"""
        bus = EventBus()

        # Add a slow handler
        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(1)
            return 'done'

        bus.on('*', slow_handler)

        # Enqueue events but don't wait
        for i in range(5):
            bus.dispatch(UserActionEvent(action=f'action_{i}', user_id='u1'))

        # Stop immediately
        await bus.stop()

        # Bus should stop even with pending events
        assert not bus._is_running

    async def test_event_with_complex_data(self, eventbus):
        """Test events with complex nested data"""
        complex_data = {
            'nested': {
                'list': [1, 2, {'inner': 'value'}],
                'datetime': datetime.now(timezone.utc),
                'none': None,
            }
        }

        event = SystemEventModel(event_name='complex', details=complex_data)

        result = await eventbus.dispatch(event)

        # Check data preserved
        assert result.details['nested']['list'][2]['inner'] == 'value'

    async def test_concurrent_emit_calls(self, eventbus):
        """Test multiple concurrent emit calls"""
        # Create many events concurrently in batches to keep this test deterministic.
        total_events = 100
        batch_size = 50
        all_tasks = []

        for batch_start in range(0, total_events, batch_size):
            batch_end = min(batch_start + batch_size, total_events)
            batch_tasks = []

            for i in range(batch_start, batch_end):
                event = UserActionEvent(action=f'concurrent_{i}', user_id='u1')
                # Emit returns the event syncresultsonously, but we need to wait for completion
                emitted_event = eventbus.dispatch(event)
                batch_tasks.append(emitted_event)

            # Wait for this batch to complete before starting the next
            await asyncio.gather(*batch_tasks)
            all_tasks.extend(batch_tasks)

        # Wait for processing
        await eventbus.wait_until_idle()

        # Check all events in log
        log = eventbus.event_history.copy()
        assert len(log) == 100

    async def test_mixed_delay_handlers_maintain_order(self, eventbus):
        """Test that events with different handler delays still maintain FIFO order"""
        collected_orders = []
        handler_start_times = []

        async def handler(event: UserActionEvent):
            order = event.metadata.get('order', -1)
            handler_start_times.append((order, asyncio.get_event_loop().time()))
            # Simulate varying processing times
            if order % 2 == 0:
                await asyncio.sleep(0.05)  # Even events take longer
            else:
                await asyncio.sleep(0.01)  # Odd events are quick
            collected_orders.append(order)
            return f'handled_{order}'

        eventbus.on('UserActionEvent', handler)

        # Emit events
        num_events = 20
        for i in range(num_events):
            event = UserActionEvent(action=f'mixed_{i}', user_id='u1', metadata={'order': i})
            eventbus.dispatch(event)

        # Wait for all events to process
        await eventbus.wait_until_idle()

        # Verify exact FIFO order despite different processing times
        assert collected_orders == list(range(num_events)), f'Events processed out of order: {collected_orders}'

        # Verify handler start times are in order (events are dequeued in FIFO order)
        for i in range(1, len(handler_start_times)):
            prev_order, prev_time = handler_start_times[i - 1]
            curr_order, curr_time = handler_start_times[i]
            assert curr_time >= prev_time, f'Event {curr_order} started before event {prev_order}'


class TestEventTypeOverride:
    """Test that Event subclasses properly override event_type"""

    async def test_event_subclass_type(self, eventbus):
        """Test that event subclasses maintain their type"""

        # Create a specific event type
        event = CreateAgentTaskEvent(
            user_id='test_user', agent_session_id='12345678-1234-5678-1234-567812345678', llm_model='test-model', task='test task'
        )

        # Enqueue it
        result = eventbus.dispatch(event)

        # Check type is preserved - should be class name
        assert result.event_type == 'CreateAgentTaskEvent'
        assert isinstance(result, BaseEvent)

    async def test_event_type_and_version_identity_fields(self, eventbus):
        """event_type + event_version identify payload shape"""
        base_event = BaseEvent(event_type='TestEvent')
        assert base_event.event_type == 'TestEvent'
        assert base_event.event_version == '0.0.1'

        task_event = CreateAgentTaskEvent(
            user_id='test_user', agent_session_id='12345678-1234-5678-1234-567812345678', llm_model='test-model', task='test task'
        )
        assert task_event.event_type == 'CreateAgentTaskEvent'
        assert task_event.event_version == '0.0.1'

        # Check identity fields are preserved after emit
        result = eventbus.dispatch(task_event)
        assert result.event_type == task_event.event_type
        assert result.event_version == task_event.event_version

    async def test_event_version_defaults_and_overrides(self, eventbus):
        """event_version supports class defaults, runtime override, and JSON roundtrip."""

        base_event = BaseEvent(event_type='TestVersionEvent')
        assert base_event.event_version == '0.0.1'

        class VersionedEvent(BaseEvent):
            event_version = '1.2.3'
            data: str

        class_default = VersionedEvent(data='x')
        assert class_default.event_version == '1.2.3'

        runtime_override = VersionedEvent(data='x', event_version='9.9.9')
        assert runtime_override.event_version == '9.9.9'

        dispatched = eventbus.dispatch(VersionedEvent(data='queued'))
        assert dispatched.event_version == '1.2.3'

        restored = BaseEvent.model_validate(dispatched.model_dump(mode='json'))
        assert restored.event_version == '1.2.3'

    async def test_automatic_event_type_derivation(self, eventbus):
        """Test that event_type is automatically derived from class name when not specified"""

        # Test automatic derivation
        event = UserActionEvent(action='test', user_id='u1')
        assert event.event_type == 'UserActionEvent'

        event2 = SystemEventModel(event_name='startup')
        assert event2.event_type == 'SystemEventModel'

        # Create inline event class without explicit event_type
        class InlineTestEvent(BaseEvent):
            data: str

        inline_event = InlineTestEvent(data='test')
        assert inline_event.event_type == 'InlineTestEvent'

        # Test with EventBus
        received = []

        async def handler(event):
            received.append(event)

        eventbus.on('UserActionEvent', handler)
        eventbus.on('InlineTestEvent', handler)

        await eventbus.dispatch(event)
        await eventbus.dispatch(inline_event)
        await eventbus.wait_until_idle()

        assert len(received) == 2
        assert received[0].event_type == 'UserActionEvent'
        assert received[1].event_type == 'InlineTestEvent'

    async def test_explicit_event_type_override(self, eventbus):
        """Test that explicit event_type can still override the automatic derivation"""

        # Create event with explicit event_type override
        class OverrideEvent(BaseEvent):
            event_type: str = Field(default='CustomEventType', frozen=True)
            data: str

        event = OverrideEvent(data='test')
        assert event.event_type == 'CustomEventType'  # Not 'OverrideEvent'

        # Test with EventBus
        received = []

        async def handler(event):
            received.append(event)

        eventbus.on('CustomEventType', handler)
        eventbus.on('OverrideEvent', handler)  # This won't match

        await eventbus.dispatch(event)
        await eventbus.wait_until_idle()

        assert len(received) == 1
        assert received[0].event_type == 'CustomEventType'


class TestWALPersistence:
    """Test automatic WAL persistence functionality"""

    async def test_wal_persistence_handler(self, tmp_path):
        """Test that events are automatically persisted to WAL file"""
        # Create event bus with WAL path
        wal_path = tmp_path / 'test_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit some events
            events = []
            for i in range(3):
                event = UserActionEvent(action=f'action_{i}', user_id=f'user_{i}')
                emitted_event = bus.dispatch(event)
                completed_event = await emitted_event
                events.append(completed_event)

            # Wait for processing
            await bus.wait_until_idle()

            # Check WAL file exists
            assert wal_path.exists()

            # Read and verify JSONL content
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 3

            # Parse each line as JSON
            for i, line in enumerate(lines):
                data = json.loads(line)
                assert data['action'] == f'action_{i}'
                assert data['user_id'] == f'user_{i}'
                assert data['event_type'] == 'UserActionEvent'
                assert isinstance(data['event_created_at'], str)
                datetime.fromisoformat(data['event_created_at'])

        finally:
            await bus.stop()

    async def test_wal_persistence_creates_parent_dir(self, tmp_path):
        """Test that WAL persistence creates parent directories"""
        # Use a nested path that doesn't exist
        wal_path = tmp_path / 'nested' / 'dirs' / 'events.jsonl'
        assert not wal_path.parent.exists()

        # Create event bus
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit an event
            event = bus.dispatch(UserActionEvent(action='test', user_id='u1'))
            await event

            # Wait for WAL persistence to complete
            await bus.wait_until_idle()

            # Parent directory should be created after event is processed
            assert wal_path.parent.exists()

            # Check file was created
            assert wal_path.exists()
        finally:
            await bus.stop()

    async def test_wal_persistence_skips_incomplete_events(self, tmp_path):
        """Test that WAL persistence only writes completed events"""
        wal_path = tmp_path / 'incomplete_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Add a slow handler that will delay completion
            async def slow_handler(event: BaseEvent) -> str:
                await asyncio.sleep(0.1)
                return 'slow'

            bus.on('UserActionEvent', slow_handler)

            # Emit event without waiting
            event = bus.dispatch(UserActionEvent(action='test', user_id='u1'))

            # Check file doesn't exist yet (event not completed)
            assert not wal_path.exists()

            # Wait for completion
            event = await event
            await bus.wait_until_idle()

            # Now file should exist with completed event
            assert wal_path.exists()
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data['event_type'] == 'UserActionEvent'
            # The WAL should have been written after the event completed
            assert data['action'] == 'test'
            assert data['user_id'] == 'u1'

        finally:
            await bus.stop()


class TestHandlerMiddleware:
    """Tests for the handler middleware pipeline."""

    async def test_middleware_wraps_successful_handler(self):
        calls: list[tuple[str, str]] = []

        class TrackingMiddleware(EventBusMiddleware):
            def __init__(self, call_log: list[tuple[str, str]]):
                self.call_log = call_log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.call_log.append(('before', event_result.status))
                elif status == 'completed':
                    self.call_log.append(('after', event_result.status))

        bus = EventBus(middlewares=[TrackingMiddleware(calls)])
        bus.on('UserActionEvent', lambda event: 'ok')

        try:
            completed = await bus.dispatch(UserActionEvent(action='test', user_id='user1'))
            await bus.wait_until_idle()

            assert completed.event_results
            result = next(iter(completed.event_results.values()))
            assert result.status == 'completed'
            assert result.result == 'ok'
            assert calls == [('before', 'started'), ('after', 'completed')]
        finally:
            await bus.stop()

    async def test_middleware_observes_handler_errors(self):
        observations: list[tuple[str, str]] = []

        class ErrorMiddleware(EventBusMiddleware):
            def __init__(self, log: list[tuple[str, str]]):
                self.log = log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.log.append(('before', event_result.status))
                elif status == 'completed' and event_result.error:
                    self.log.append(('error', type(event_result.error).__name__))

        async def failing_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        bus = EventBus(middlewares=[ErrorMiddleware(observations)])
        bus.on('UserActionEvent', failing_handler)

        try:
            event = await bus.dispatch(UserActionEvent(action='fail', user_id='user2'))
            await bus.wait_until_idle()

            result = next(iter(event.event_results.values()))
            assert result.status == 'error'
            assert isinstance(result.error, ValueError)
            assert observations == [('before', 'started'), ('error', 'ValueError')]
        finally:
            await bus.stop()

    async def test_auto_error_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, str]] = []
        bus = EventBus(middlewares=[AutoErrorEventMiddleware()])

        async def fail_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        async def fail_auto(event: BaseEvent) -> None:
            raise RuntimeError('nested')

        bus.on(UserActionEvent, fail_handler)
        bus.on('UserActionEventErrorEvent', lambda event: seen.append((event.event_type, event.error_type)))
        bus.on('UserActionEventErrorEvent', fail_auto)

        try:
            await bus.dispatch(UserActionEvent(action='fail', user_id='u1'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventErrorEvent', 'ValueError')]
            assert await bus.find('UserActionEventErrorEventErrorEvent', past=True, future=False) is None
        finally:
            await bus.stop()

    async def test_auto_return_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        async def ok_handler(event: BaseEvent) -> int:
            return 123

        async def non_none_auto(event: BaseEvent) -> str:
            return 'nested'

        bus.on(UserActionEvent, ok_handler)
        bus.on('UserActionEventResultEvent', lambda event: seen.append((event.event_type, event.data)))
        bus.on('UserActionEventResultEvent', non_none_auto)

        try:
            await bus.dispatch(UserActionEvent(action='ok', user_id='u2'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventResultEvent', 123)]
            assert await bus.find('UserActionEventResultEventResultEvent', past=True, future=False) is None
        finally:
            await bus.stop()

    async def test_auto_return_event_middleware_skips_baseevent_returns(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        class ReturnedEvent(BaseEvent):
            value: int

        async def returns_event(event: BaseEvent) -> ReturnedEvent:
            return ReturnedEvent(value=7)

        bus.on(UserActionEvent, returns_event)
        bus.on('UserActionEventResultEvent', lambda event: seen.append((event.event_type, event.data)))

        try:
            parent = await bus.dispatch(UserActionEvent(action='ok', user_id='u3'))
            await bus.wait_until_idle()
            assert len(parent.event_results) == 1
            only_result = next(iter(parent.event_results.values()))
            assert isinstance(only_result.result, ReturnedEvent)
            assert seen == []
            assert await bus.find('UserActionEventResultEvent', past=True, future=False) is None
        finally:
            await bus.stop()

    async def test_auto_handler_change_event_middleware_emits_registered_and_unregistered(self):
        registered: list[BusHandlerRegisteredEvent] = []
        unregistered: list[BusHandlerUnregisteredEvent] = []
        bus = EventBus(middlewares=[AutoHandlerChangeEventMiddleware()])

        bus.on(BusHandlerRegisteredEvent, lambda event: registered.append(event))
        bus.on(BusHandlerUnregisteredEvent, lambda event: unregistered.append(event))

        async def target_handler(event: UserActionEvent) -> None:
            return None

        try:
            handler_entry = bus.on(UserActionEvent, target_handler)
            await bus.wait_until_idle()

            bus.off(UserActionEvent, handler_entry)
            await bus.wait_until_idle()

            matching_registered = [event for event in registered if event.handler.id == handler_entry.id]
            matching_unregistered = [event for event in unregistered if event.handler.id == handler_entry.id]
            assert matching_registered
            assert matching_unregistered
            assert matching_registered[-1].handler.eventbus_id == bus.id
            assert matching_registered[-1].handler.eventbus_name == bus.name
            assert matching_registered[-1].handler.event_pattern == 'UserActionEvent'
            assert matching_unregistered[-1].handler.event_pattern == 'UserActionEvent'
        finally:
            await bus.stop()

    async def test_otel_tracing_middleware_tracks_parent_event_and_handler_spans(self):
        class RootEvent(BaseEvent):
            pass

        class ChildEvent(BaseEvent):
            pass

        class FakeSpan:
            def __init__(self, name: str, context: Any = None):
                self.name = name
                self.context = context
                self.attrs: dict[str, Any] = {}
                self.errors: list[str] = []
                self.ended = False

            def set_attribute(self, key: str, value: Any):
                self.attrs[key] = value

            def record_exception(self, error: BaseException):
                self.errors.append(type(error).__name__)

            def end(self):
                self.ended = True

        class FakeTracer:
            def __init__(self):
                self.spans: list[FakeSpan] = []

            def start_span(self, name: str, context: Any = None):
                span = FakeSpan(name, context=context)
                self.spans.append(span)
                return span

        class FakeTraceAPI:
            @staticmethod
            def set_span_in_context(span: FakeSpan):
                return {'parent': span}

        tracer = FakeTracer()
        bus = EventBus(middlewares=[OtelTracingMiddleware(tracer=tracer, trace_api=FakeTraceAPI())], name='TraceBus')

        async def child_handler(event: ChildEvent) -> None:
            return None

        async def root_handler(event: RootEvent) -> None:
            child = event.event_bus.dispatch(ChildEvent())
            await child

        bus.on(RootEvent, root_handler)
        bus.on(ChildEvent, child_handler)

        try:
            await bus.dispatch(RootEvent())
            await bus.wait_until_idle()

            root_event_span = next(span for span in tracer.spans if span.attrs.get('bubus.event_type') == 'RootEvent')
            root_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('bubus.handler_name', '')).endswith('root_handler')
            )
            child_event_span = next(span for span in tracer.spans if span.attrs.get('bubus.event_type') == 'ChildEvent')
            child_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('bubus.handler_name', '')).endswith('child_handler')
            )

            assert root_handler_span.context['parent'] is root_event_span
            assert child_event_span.context['parent'] is root_handler_span
            assert child_handler_span.context['parent'] is child_event_span
            assert root_event_span.attrs.get('bubus.bus_name') == bus.label
            assert root_handler_span.attrs.get('bubus.bus_name') == bus.label
            assert child_event_span.attrs.get('bubus.bus_name') == bus.label
            assert child_handler_span.attrs.get('bubus.bus_name') == bus.label
            assert all(span.ended for span in tracer.spans)
        finally:
            await bus.stop()


class TestSQLiteHistoryMirror:
    async def test_sqlite_history_persists_events_and_results(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def handler(event: BaseEvent) -> str:
            return 'ok'

        bus.on('UserActionEvent', handler)

        try:
            await bus.dispatch(UserActionEvent(action='ping', user_id='u-1'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']

            result_rows = conn.execute(
                'SELECT phase, status, result_repr, error_repr FROM event_results_log ORDER BY id'
            ).fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'completed']
            assert result_rows[-1][2] == "'ok'"
            assert result_rows[-1][3] is None
        finally:
            await bus.stop()


class TestLoggerMiddleware:
    async def test_logger_middleware_writes_file(self, tmp_path):
        log_path = tmp_path / 'events.log'
        bus = EventBus(middlewares=[LoggerEventBusMiddleware(log_path)])

        async def handler(event: BaseEvent) -> str:
            return 'logged'

        bus.on('UserActionEvent', handler)

        try:
            await bus.dispatch(UserActionEvent(action='log', user_id='user'))
            await bus.wait_until_idle()

            assert log_path.exists()
            contents = log_path.read_text().strip().splitlines()
            assert contents
            assert 'UserActionEvent' in contents[-1]
        finally:
            await bus.stop()

    async def test_logger_middleware_stdout_only(self, capsys):
        bus = EventBus(middlewares=[LoggerEventBusMiddleware()])

        async def handler(event: BaseEvent) -> str:
            return 'stdout'

        bus.on('UserActionEvent', handler)

        try:
            await bus.dispatch(UserActionEvent(action='log', user_id='user'))
            await bus.wait_until_idle()

            captured = capsys.readouterr()
            assert 'UserActionEvent' in captured.out
            assert 'stdout' not in captured.err
        finally:
            await bus.stop()

    async def test_sqlite_history_records_errors(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def failing_handler(event: BaseEvent) -> None:
            raise RuntimeError('handler boom')

        bus.on('UserActionEvent', failing_handler)

        try:
            await bus.dispatch(UserActionEvent(action='boom', user_id='u-2'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            result_rows = conn.execute('SELECT phase, status, error_repr FROM event_results_log ORDER BY id').fetchall()
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'error']
            assert 'RuntimeError' in result_rows[-1][2]
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']
        finally:
            await bus.stop()


class TestEventBusHierarchy:
    """Test hierarchical EventBus subscription patterns"""

    async def test_tresultsee_level_hierarchy_bubbling(self):
        """Test that events bubble up tresultsough a 3-level hierarchy and event_path is correct"""
        # Create tresultsee EventBus instances in a hierarchy
        parent_bus = EventBus(name='ParentBus')
        child_bus = EventBus(name='ChildBus')
        subchild_bus = EventBus(name='SubchildBus')

        # Track events received at each level
        events_at_parent = []
        events_at_child = []
        events_at_subchild = []

        async def parent_handler(event: BaseEvent) -> str:
            events_at_parent.append(event)
            return 'parent_received'

        async def child_handler(event: BaseEvent) -> str:
            events_at_child.append(event)
            return 'child_received'

        async def subchild_handler(event: BaseEvent) -> str:
            events_at_subchild.append(event)
            return 'subchild_received'

        # Register handlers
        parent_bus.on('*', parent_handler)
        child_bus.on('*', child_handler)
        subchild_bus.on('*', subchild_handler)

        # Subscribe buses to each other: parent <- child <- subchild
        # Child forwards events to parent
        child_bus.on('*', parent_bus.dispatch)
        # Subchild forwards events to child
        subchild_bus.on('*', child_bus.dispatch)

        try:
            # Emit event from the bottom of hierarchy
            event = UserActionEvent(action='bubble_test', user_id='test_user')
            emitted = subchild_bus.dispatch(event)

            # Wait for event to bubble up
            await subchild_bus.wait_until_idle()
            await child_bus.wait_until_idle()
            await parent_bus.wait_until_idle()

            # Verify event was received at all levels
            assert len(events_at_subchild) == 1
            assert len(events_at_child) == 1
            assert len(events_at_parent) == 1

            # Verify event_path shows the complete journey
            final_event = events_at_parent[0]
            assert final_event.event_path == [subchild_bus.label, child_bus.label, parent_bus.label]

            # Verify it's the same event content
            assert final_event.action == 'bubble_test'
            assert final_event.user_id == 'test_user'
            assert final_event.event_id == emitted.event_id

            # Test event emitted at middle level
            events_at_parent.clear()
            events_at_child.clear()
            events_at_subchild.clear()

            middle_event = SystemEventModel(event_name='middle_test')
            child_bus.dispatch(middle_event)

            await child_bus.wait_until_idle()
            await parent_bus.wait_until_idle()

            # Should only reach child and parent, not subchild
            assert len(events_at_subchild) == 0
            assert len(events_at_child) == 1
            assert len(events_at_parent) == 1
            assert events_at_parent[0].event_path == [child_bus.label, parent_bus.label]

        finally:
            await parent_bus.stop()
            await child_bus.stop()
            await subchild_bus.stop()

    async def test_circular_subscription_prevention(self):
        """Test that circular EventBus subscriptions don't create infinite loops"""
        # Create tresultsee peer EventBus instances
        peer1 = EventBus(name='Peer1')
        peer2 = EventBus(name='Peer2')
        peer3 = EventBus(name='Peer3')

        # Track events at each peer
        events_at_peer1 = []
        events_at_peer2 = []
        events_at_peer3 = []

        async def peer1_handler(event: BaseEvent) -> str:
            events_at_peer1.append(event)
            return 'peer1_received'

        async def peer2_handler(event: BaseEvent) -> str:
            events_at_peer2.append(event)
            return 'peer2_received'

        async def peer3_handler(event: BaseEvent) -> str:
            events_at_peer3.append(event)
            return 'peer3_received'

        # Register handlers
        peer1.on('*', peer1_handler)
        peer2.on('*', peer2_handler)
        peer3.on('*', peer3_handler)

        # Create circular subscription: peer1 -> peer2 -> peer3 -> peer1
        peer1.on('*', peer2.dispatch)
        peer2.on('*', peer3.dispatch)
        peer3.on('*', peer1.dispatch)  # This completes the circle

        def dump_bus_state() -> str:
            buses = [peer1, peer2, peer3]
            lines: list[str] = []
            for bus in buses:
                queue_size = bus.pending_event_queue.qsize() if bus.pending_event_queue else 0
                lines.append(
                    f'{bus.label} queue={queue_size} active={len(bus._active_event_ids)} processing={len(bus._processing_event_ids)} history={len(bus.event_history)}'
                )
            lines.append('--- peer1.log_tree() ---')
            lines.append(peer1.log_tree())
            lines.append('--- peer2.log_tree() ---')
            lines.append(peer2.log_tree())
            lines.append('--- peer3.log_tree() ---')
            lines.append(peer3.log_tree())
            return '\n'.join(lines)

        try:
            # Emit event from peer1
            event = UserActionEvent(action='circular_test', user_id='test_user')
            emitted = peer1.dispatch(event)

            # Wait for all processing to complete
            await asyncio.sleep(0.2)  # Give time for any potential loops
            try:
                await asyncio.wait_for(peer1.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer2.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer3.wait_until_idle(), timeout=5)
            except TimeoutError:
                pytest.fail(f'Circular test stalled during first propagation.\n{dump_bus_state()}')

            # Each peer should receive the event exactly once
            assert len(events_at_peer1) == 1
            assert len(events_at_peer2) == 1
            assert len(events_at_peer3) == 1

            # Check event paths show the propagation but no loops
            assert events_at_peer1[0].event_path == [peer1.label, peer2.label, peer3.label]
            assert events_at_peer2[0].event_path == [peer1.label, peer2.label, peer3.label]
            assert events_at_peer3[0].event_path == [peer1.label, peer2.label, peer3.label]

            # The event should NOT come back to peer1 from peer3
            # because peer3's emit handler will detect peer1 is already in the path

            # Verify all events have the same ID (same event, not duplicates)
            assert all(e.event_id == emitted.event_id for e in [events_at_peer1[0], events_at_peer2[0], events_at_peer3[0]])

            # Test starting from a different peer
            events_at_peer1.clear()
            events_at_peer2.clear()
            events_at_peer3.clear()

            event2 = SystemEventModel(event_name='circular_test_2')
            peer2.dispatch(event2)

            await asyncio.sleep(0.2)
            try:
                await asyncio.wait_for(peer1.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer2.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer3.wait_until_idle(), timeout=5)
            except TimeoutError:
                pytest.fail(f'Circular test stalled during second propagation.\n{dump_bus_state()}')

            # Should visit peer2 -> peer3 -> peer1, then stop
            assert len(events_at_peer1) == 1
            assert len(events_at_peer2) == 1
            assert len(events_at_peer3) == 1

            assert events_at_peer2[0].event_path == [peer2.label, peer3.label, peer1.label]
            assert events_at_peer3[0].event_path == [peer2.label, peer3.label, peer1.label]
            assert events_at_peer1[0].event_path == [peer2.label, peer3.label, peer1.label]

        finally:
            await peer1.stop()
            await peer2.stop()
            await peer3.stop()


class TestFindMethod:
    """Test find() behavior for future waits and filtering."""

    async def test_find_future_basic(self, eventbus):
        """Test basic future find functionality."""
        # Start waiting for an event that hasn't been dispatched yet
        find_task = asyncio.create_task(eventbus.find('UserActionEvent', past=False, future=1.0))

        # Give find time to register waiter
        await asyncio.sleep(0.01)

        # Dispatch the event
        dispatched = eventbus.dispatch(UserActionEvent(action='login', user_id='user123'))

        # Wait for find to resolve
        received = await find_task

        # Verify we got the right event
        assert received.event_type == 'UserActionEvent'
        assert received.action == 'login'
        assert received.user_id == 'user123'
        assert received.event_id == dispatched.event_id

    async def test_find_future_with_predicate(self, eventbus):
        """Test future find with where predicate filtering."""
        # Dispatch some events that don't match
        eventbus.dispatch(UserActionEvent(action='logout', user_id='user456'))
        eventbus.dispatch(UserActionEvent(action='login', user_id='user789'))

        find_task = asyncio.create_task(
            eventbus.find('UserActionEvent', where=lambda e: e.user_id == 'user123', past=False, future=1.0)
        )

        # Give find time to register
        await asyncio.sleep(0.01)

        # Dispatch more events
        eventbus.dispatch(UserActionEvent(action='update', user_id='user456'))
        target_event = eventbus.dispatch(UserActionEvent(action='login', user_id='user123'))
        eventbus.dispatch(UserActionEvent(action='delete', user_id='user789'))

        # Wait for the matching event
        received = await find_task

        # Should get the event matching the predicate
        assert received.user_id == 'user123'
        assert received.event_id == target_event.event_id

    async def test_find_future_timeout(self, eventbus):
        """Test future find timeout behavior."""
        result = await eventbus.find('NonExistentEvent', past=False, future=0.1)
        assert result is None

    async def test_find_future_with_model_class(self, eventbus):
        """Test future find with model class instead of string."""
        find_task = asyncio.create_task(eventbus.find(SystemEventModel, past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch different event types
        eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))
        target = eventbus.dispatch(SystemEventModel(event_name='startup', severity='info'))

        # Should receive the SystemEventModel
        received = await find_task
        assert isinstance(received, SystemEventModel)
        assert received.event_name == 'startup'
        assert received.event_id == target.event_id

    async def test_multiple_concurrent_future_finds(self, eventbus):
        """Test multiple concurrent future find calls."""
        find1 = asyncio.create_task(
            eventbus.find('UserActionEvent', where=lambda e: e.action == 'normal', past=False, future=2.0)
        )
        find2 = asyncio.create_task(eventbus.find('SystemEventModel', past=False, future=2.0))
        find3 = asyncio.create_task(
            eventbus.find('UserActionEvent', where=lambda e: e.action == 'special', past=False, future=2.0)
        )

        await asyncio.sleep(0.1)  # Give more time for handlers to register

        # Dispatch events
        e1 = eventbus.dispatch(UserActionEvent(action='normal', user_id='u1'))
        e2 = eventbus.dispatch(SystemEventModel(event_name='test'))
        e3 = eventbus.dispatch(UserActionEvent(action='special', user_id='u2'))

        # Wait for all events to be processed
        await eventbus.wait_until_idle()

        # Wait for all find tasks
        r1, r2, r3 = await asyncio.gather(find1, find2, find3)

        # Verify results
        assert r1.event_id == e1.event_id  # Normal UserActionEvent
        assert r2.event_id == e2.event_id  # SystemEventModel
        assert r3.event_id == e3.event_id  # Special UserActionEvent

    async def test_find_waiter_cleanup(self, eventbus):
        """Test that temporary find waiters are properly cleaned up."""
        initial_waiters = len(eventbus._find_waiters)
        result = await eventbus.find('TestEvent', past=False, future=0.1)
        assert result is None
        assert len(eventbus._find_waiters) == initial_waiters

        find_task = asyncio.create_task(eventbus.find('TestEvent2', past=False, future=1.0))
        await asyncio.sleep(0.01)
        eventbus.dispatch(BaseEvent(event_type='TestEvent2'))
        await find_task
        assert len(eventbus._find_waiters) == initial_waiters

    async def test_find_future_receives_dispatched_event_before_completion(self, eventbus):
        """Test that future find resolves before slow handlers complete."""
        processing_complete = False

        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(0.1)
            nonlocal processing_complete
            processing_complete = True
            return 'done'

        # Register a slow handler
        eventbus.on('SlowEvent', slow_handler)

        # Start future find
        find_task = asyncio.create_task(eventbus.find('SlowEvent', past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch event
        eventbus.dispatch(BaseEvent(event_type='SlowEvent'))

        # Wait for find
        received = await find_task

        assert received.event_type == 'SlowEvent'
        assert processing_complete is False

        # Find resolves on dispatch; handler result entries may or may not exist yet.
        slow_result = next(
            (res for res in received.event_results.values() if res.handler_name.endswith('slow_handler')),
            None,
        )
        if slow_result is not None:
            assert slow_result.status != 'completed'

        await eventbus.wait_until_idle()
        assert processing_complete is True


class TestFindPastMethod:
    """Tests for history-only find behavior."""

    async def test_find_past_returns_most_recent(self, eventbus):
        # Dispatch two events and ensure the newest is returned
        eventbus.dispatch(UserActionEvent(action='first', user_id='u1'))
        latest = eventbus.dispatch(UserActionEvent(action='second', user_id='u2'))
        await eventbus.wait_until_idle()

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is not None
        assert match.event_id == latest.event_id

    async def test_find_past_respects_time_window(self, eventbus):
        event = eventbus.dispatch(UserActionEvent(action='old', user_id='u1'))
        await eventbus.wait_until_idle()
        event.event_created_at -= timedelta(seconds=30)

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is None

    async def test_find_past_can_match_incomplete_events(self, eventbus):
        processing = asyncio.Event()

        async def slow_handler(evt: UserActionEvent) -> None:
            await asyncio.sleep(0.05)
            processing.set()

        eventbus.on('UserActionEvent', slow_handler)

        pending_event = eventbus.dispatch(UserActionEvent(action='slow', user_id='u1'))

        # While handler is running, past find can still match in-flight events
        in_flight = await eventbus.find('UserActionEvent', past=10, future=False)
        assert in_flight is not None
        assert in_flight.event_id == pending_event.event_id

        await pending_event
        await processing.wait()

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is not None
        assert match.event_id == pending_event.event_id


class TestDebouncePatterns:
    """End-to-end scenarios for debounce-style flows."""

    class DebounceEvent(BaseEvent):
        user_id: int

    async def test_debounce_prefers_recent_history(self, eventbus):
        # First event completes
        initial = await eventbus.dispatch(self.DebounceEvent(user_id=123))
        await eventbus.wait_until_idle()

        # Compose the debounce pattern: find(past) -> find(future) -> dispatch
        resolved = (
            await eventbus.find(self.DebounceEvent, past=10, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.05)
            or await eventbus.dispatch(self.DebounceEvent(user_id=123))
        )

        assert resolved is not None
        assert resolved.event_id == initial.event_id

        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_debounce_dispatches_when_recent_missing(self, eventbus):
        resolved = (
            await eventbus.find(self.DebounceEvent, past=1, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.05)
            or await eventbus.dispatch(self.DebounceEvent(user_id=999))
        )

        assert resolved is not None
        assert isinstance(resolved, self.DebounceEvent)
        assert resolved.user_id == 999

        await eventbus.wait_until_idle()

        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_debounce_uses_future_match_before_dispatch_fallback(self, eventbus):
        async def dispatch_after_delay() -> BaseEvent:
            await asyncio.sleep(0.02)
            return eventbus.dispatch(self.DebounceEvent(user_id=555))

        dispatch_task = asyncio.create_task(dispatch_after_delay())

        resolved = (
            await eventbus.find(self.DebounceEvent, past=1, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.1)
            or await eventbus.dispatch(self.DebounceEvent(user_id=999))
        )

        dispatched = await dispatch_task
        assert resolved is not None
        assert isinstance(resolved, self.DebounceEvent)
        assert resolved.event_id == dispatched.event_id
        assert resolved.user_id == 555

        await eventbus.wait_until_idle()
        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_find_with_complex_predicate(self, eventbus):
        """Test future find with complex predicate logic."""
        events_seen = []

        def complex_predicate(event: UserActionEvent) -> bool:
            if hasattr(event, 'action'):
                # Only match after seeing at least 3 events and action is 'target'
                result = len(events_seen) >= 3 and event.action == 'target'
                events_seen.append(event.action)
                return result
            return False

        find_task = asyncio.create_task(eventbus.find('UserActionEvent', where=complex_predicate, past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch events
        eventbus.dispatch(UserActionEvent(action='first', user_id='u1'))
        eventbus.dispatch(UserActionEvent(action='second', user_id='u2'))
        eventbus.dispatch(UserActionEvent(action='target', user_id='u3'))  # Won't match yet
        eventbus.dispatch(UserActionEvent(action='target', user_id='u4'))  # This should match

        received = await find_task

        assert received.user_id == 'u4'
        assert len(events_seen) == 4


class TestEventResults:
    """Test the event results functionality on BaseEvent"""

    async def test_dispatch_returns_event_results(self, eventbus):
        """Test that dispatch returns BaseEvent with result methods"""

        # Register a specific handler
        async def test_handler(event):
            return {'result': 'test_result'}

        eventbus.on('UserActionEvent', test_handler)

        result = eventbus.dispatch(UserActionEvent(action='test', user_id='u1'))
        assert isinstance(result, BaseEvent)

        # Wait for completion
        await result
        # Get results by handler ID
        all_results = await result.event_results_flat_dict()
        assert isinstance(all_results, dict)
        # Should contain only test_handler result
        assert len(all_results) == 1
        assert all_results['result'] == 'test_result'

        # Test with no specific handlers
        result_no_handlers = eventbus.dispatch(BaseEvent(event_type='NoHandlersEvent'))
        await result_no_handlers
        # Should have no handlers
        assert len(result_no_handlers.event_results) == 0

    async def test_event_results_indexing(self, eventbus):
        """Test indexing by handler name and ID"""
        order = []

        async def handler1(event):
            order.append(1)
            return 'first'

        async def handler2(event):
            order.append(2)
            return 'second'

        async def handler3(event):
            order.append(3)
            return 'third'

        eventbus.on('TestEvent', handler1)
        eventbus.on('TestEvent', handler2)
        eventbus.on('TestEvent', handler3)

        # Test indexing
        event = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Get results by handler name
        handler1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler1')), None)
        handler2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler2')), None)
        handler3_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler3')), None)

        assert handler1_result is not None and handler1_result.result == 'first'
        assert handler2_result is not None and handler2_result.result == 'second'
        assert handler3_result is not None and handler3_result.result == 'third'

    async def test_event_results_access(self, eventbus):
        """Test accessing event results"""

        async def early_handler(event):
            return 'early'

        async def late_handler(event):
            await asyncio.sleep(0.01)
            return 'late'

        eventbus.on('TestEvent', early_handler)
        eventbus.on('TestEvent', late_handler)

        result = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Check both handlers ran
        assert len(result.event_results) == 2
        early_result = next((r for r in result.event_results.values() if r.handler_name.endswith('early_handler')), None)
        late_result = next((r for r in result.event_results.values() if r.handler_name.endswith('late_handler')), None)
        assert early_result is not None and early_result.result == 'early'
        assert late_result is not None and late_result.result == 'late'

        # With empty handlers
        eventbus.handlers_by_key['EmptyEvent'] = []
        results_empty = eventbus.dispatch(BaseEvent(event_type='EmptyEvent'))
        await results_empty
        # Should have no handlers
        assert len(results_empty.event_results) == 0

    async def test_by_handler_name(self, eventbus):
        """Test handler results with duplicate names"""

        async def process_data(event):
            return 'version1'

        async def process_data2(event):  # Different function, same __name__
            return 'version2'

        process_data2.__name__ = 'process_data'  # Same name!

        async def unique_handler(event):
            return 'unique'

        # Should get warning about duplicate name
        with pytest.warns(UserWarning, match='already registered'):
            eventbus.on('TestEvent', process_data)
            eventbus.on('TestEvent', process_data2)
        eventbus.on('TestEvent', unique_handler)

        event = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Check results - with duplicate names, both handlers run
        process_results = [r for r in event.event_results.values() if r.handler_name.endswith('process_data')]
        assert len(process_results) == 2
        assert {r.result for r in process_results} == {'version1', 'version2'}

        unique_result = next((r for r in event.event_results.values() if r.handler_name.endswith('unique_handler')), None)
        assert unique_result is not None and unique_result.result == 'unique'

    async def test_by_handler_id(self, eventbus):
        """Test that all handlers run with unique IDs even with same name"""

        async def handler1(event):
            return 'v1'

        async def handler2(event):
            return 'v2'

        # Give them the same name for the test
        handler1.__name__ = 'handler'
        handler2.__name__ = 'handler'

        with pytest.warns(UserWarning, match='already registered'):
            eventbus.on('TestEvent', handler1)
            eventbus.on('TestEvent', handler2)

        event = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Get results by handler ID using the method that exists
        results = await event.event_results_by_handler_id()

        # All handlers present with unique IDs even with same name
        # Should have 2 results: handler1, handler2
        assert len(results) == 2
        assert 'v1' in results.values()
        assert 'v2' in results.values()

    async def test_flat_dict(self, eventbus):
        """Test event_results_flat_dict() merging"""

        async def config_base(event):
            return {'debug': False, 'port': 8080, 'name': 'base'}

        async def config_override(event):
            return {'debug': True, 'timeout': 30, 'name': 'override'}

        eventbus.on('GetConfig', config_base)
        eventbus.on('GetConfig', config_override)

        event = await eventbus.dispatch(BaseEvent(event_type='GetConfig'))
        merged = await event.event_results_flat_dict(raise_if_conflicts=False)

        # Later handlers override earlier ones
        assert merged == {
            'debug': True,  # Overridden
            'port': 8080,  # From base
            'timeout': 30,  # From override
            'name': 'override',  # Overridden
        }

        # Test non-dict handler (should be skipped)
        async def bad_handler(event):
            return 'not a dict'

        eventbus.on('BadConfig', bad_handler)
        event_bad = await eventbus.dispatch(BaseEvent(event_type='BadConfig'))

        # Non-dict results should be skipped, not raise error
        merged_bad = await event_bad.event_results_flat_dict()
        assert merged_bad == {}  # Empty dict since no dict results

    async def test_flat_dict_conflict_raises(self, eventbus):
        """event_results_flat_dict() raises by default when handlers conflict."""

        async def handler_one(event):
            return {'shared': 1, 'unique1': 'a'}

        async def handler_two(event):
            return {'shared': 2, 'unique2': 'b'}

        eventbus.on('ConflictEvent', handler_one)
        eventbus.on('ConflictEvent', handler_two)

        event = await eventbus.dispatch(BaseEvent(event_type='ConflictEvent'))

        with pytest.raises(ValueError) as exc_info:
            await event.event_results_flat_dict()

        assert 'overwrite values from previous handlers' in str(exc_info.value)

    async def test_flat_list(self, eventbus):
        """Test event_results_flat_list() concatenation"""

        async def errors1(event):
            return ['error1', 'error2']

        async def errors2(event):
            return ['error3']

        async def errors3(event):
            return ['error4', 'error5']

        eventbus.on('GetErrors', errors1)
        eventbus.on('GetErrors', errors2)
        eventbus.on('GetErrors', errors3)

        event = await eventbus.dispatch(BaseEvent(event_type='GetErrors'))
        all_errors = await event.event_results_flat_list()

        # Check that all errors are collected (order may vary due to handler execution)
        assert all_errors == ['error1', 'error2', 'error3', 'error4', 'error5']

        # Test with non-list handler
        async def single_value(event):
            return 'single'

        eventbus.on('GetSingle', single_value)
        event_single = await eventbus.dispatch(BaseEvent(event_type='GetSingle'))

        result = await event_single.event_results_flat_list(raise_if_none=False)
        assert 'single' not in result  # Single values should be skipped, as they are not lists
        assert len(result) == 0

    async def test_by_handler_name_access(self, eventbus):
        """Test accessing results by handler name"""

        async def handler_a(event):
            return 'result_a'

        async def handler_b(event):
            return 'result_b'

        eventbus.on('TestEvent', handler_a)
        eventbus.on('TestEvent', handler_b)

        event = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Access results by handler name
        handler_a_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler_a')), None)
        handler_b_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler_b')), None)

        assert handler_a_result is not None and handler_a_result.result == 'result_a'
        assert handler_b_result is not None and handler_b_result.result == 'result_b'

    async def test_string_indexing(self, eventbus):
        """Test accessing handler results"""

        async def my_handler(event):
            return 'my_result'

        eventbus.on('TestEvent', my_handler)
        event = await eventbus.dispatch(BaseEvent(event_type='TestEvent'))

        # Access result by handler name
        my_handler_result = next((r for r in event.event_results.values() if r.handler_name.endswith('my_handler')), None)
        assert my_handler_result is not None and my_handler_result.result == 'my_result'

        # Check missing handler returns None
        missing_result = next((r for r in event.event_results.values() if r.handler_name.endswith('missing')), None)
        assert missing_result is None


class TestEventBusForwarding:
    """Test event forwarding between buses with new EventResults"""

    async def test_forwarding_flattens_results(self):
        """Test that forwarding events between buses flattens all results"""
        bus1 = EventBus(name='Bus1')
        bus2 = EventBus(name='Bus2')
        bus3 = EventBus(name='Bus3')

        results = []

        async def bus1_handler(event):
            results.append('bus1')
            return 'from_bus1'

        async def bus2_handler(event):
            results.append('bus2')
            return 'from_bus2'

        async def bus3_handler(event):
            results.append('bus3')
            return 'from_bus3'

        # Register handlers
        bus1.on('TestEvent', bus1_handler)
        bus2.on('TestEvent', bus2_handler)
        bus3.on('TestEvent', bus3_handler)

        # Set up forwarding chain
        bus1.on('*', bus2.dispatch)
        bus2.on('*', bus3.dispatch)

        try:
            # Dispatch from bus1
            event = bus1.dispatch(BaseEvent(event_type='TestEvent'))

            # Wait for all buses to complete processing
            await bus1.wait_until_idle()
            await bus2.wait_until_idle()
            await bus3.wait_until_idle()

            # Wait for event completion
            event = await event

            # All handlers from all buses should be visible
            bus1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus1_handler')), None)
            bus2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus2_handler')), None)
            bus3_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus3_handler')), None)

            assert bus1_result is not None and bus1_result.result == 'from_bus1'
            assert bus2_result is not None and bus2_result.result == 'from_bus2'
            assert bus3_result is not None and bus3_result.result == 'from_bus3'

            # Check execution order
            assert results == ['bus1', 'bus2', 'bus3']

        finally:
            await bus1.stop()
            await bus2.stop()
            await bus3.stop()

    async def test_by_eventbus_id_and_path(self):
        """Test by_eventbus_id() and by_path() with forwarding"""
        bus1 = EventBus(name='MainBus')
        bus2 = EventBus(name='PluginBus')

        async def main_handler(event):
            return 'main_result'

        async def plugin_handler1(event):
            return 'plugin_result1'

        async def plugin_handler2(event):
            return 'plugin_result2'

        bus1.on('DataEvent', main_handler)
        bus2.on('DataEvent', plugin_handler1)
        bus2.on('DataEvent', plugin_handler2)

        # Forward from bus1 to bus2
        bus1.on('*', bus2.dispatch)

        try:
            event = bus1.dispatch(BaseEvent(event_type='DataEvent'))

            # Wait for processing
            await bus1.wait_until_idle()
            await bus2.wait_until_idle()
            event = await event

            # Check results from both buses
            main_result = next((r for r in event.event_results.values() if r.handler_name.endswith('main_handler')), None)
            plugin1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('plugin_handler1')), None)
            plugin2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('plugin_handler2')), None)

            assert main_result is not None and main_result.result == 'main_result'
            assert plugin1_result is not None and plugin1_result.result == 'plugin_result1'
            assert plugin2_result is not None and plugin2_result.result == 'plugin_result2'

            # Check event path shows forwarding
            assert event.event_path == [bus1.label, bus2.label]

        finally:
            await bus1.stop()
            await bus2.stop()


class TestComplexIntegration:
    """Complex integration test with all features"""

    async def test_complex_multi_bus_scenario(self, caplog):
        """Test complex scenario with multiple buses, duplicate names, and lookup flows"""
        # Create a hierarchy of buses
        app_bus = EventBus(name='AppBus')
        auth_bus = EventBus(name='AuthBus')
        data_bus = EventBus(name='DataBus')

        # Handlers with conflicting names
        async def app_validate(event):
            """App validation"""
            return {'app_valid': True, 'timestamp': 1000}

        app_validate.__name__ = 'validate'

        async def auth_validate(event):
            """Auth validation"""
            return {'auth_valid': True, 'user': 'alice'}

        auth_validate.__name__ = 'validate'

        async def data_validate(event):
            """Data validation"""
            return {'data_valid': True, 'schema': 'v2'}

        data_validate.__name__ = 'validate'

        async def auth_process(event):
            """Auth processing"""
            return ['auth_log_1', 'auth_log_2']

        auth_process.__name__ = 'process'

        async def data_process(event):
            """Data processing"""
            return ['data_log_1', 'data_log_2', 'data_log_3']

        data_process.__name__ = 'process'

        # Register handlers with same names on different buses
        app_bus.on('ValidationRequest', app_validate)
        auth_bus.on('ValidationRequest', auth_validate)
        auth_bus.on('ValidationRequest', auth_process)  # Different return type!
        data_bus.on('ValidationRequest', data_validate)
        data_bus.on('ValidationRequest', data_process)

        # Set up forwarding
        app_bus.on('*', auth_bus.dispatch)
        auth_bus.on('*', data_bus.dispatch)

        try:
            # Dispatch event
            event = app_bus.dispatch(BaseEvent(event_type='ValidationRequest'))

            # Wait for all processing
            await app_bus.wait_until_idle()
            await auth_bus.wait_until_idle()
            await data_bus.wait_until_idle()
            event = await event

            # Test that all handlers ran
            # Count handlers by name
            validate_results = [r for r in event.event_results.values() if r.handler_name.endswith('validate')]
            process_results = [r for r in event.event_results.values() if r.handler_name.endswith('process')]

            # Should have multiple validate and process handlers from different buses
            assert len(validate_results) >= 3  # One per bus
            assert len(process_results) >= 2  # Auth and Data buses

            # Check event path shows forwarding through all buses
            assert app_bus.label in event.event_path
            assert auth_bus.label in event.event_path
            assert data_bus.label in event.event_path

            # Test flat dict merging
            dict_result = await event.event_results_flat_dict()
            # Should have merged all dict returns
            assert 'app_valid' in dict_result and 'auth_valid' in dict_result and 'data_valid' in dict_result

            # Test flat list
            list_result = await event.event_results_flat_list()
            # Should include all list items
            assert any('log' in str(item) for item in list_result)

        finally:
            await app_bus.stop(timeout=0, clear=True)
            await auth_bus.stop(timeout=0, clear=True)
            await data_bus.stop(timeout=0, clear=True)

    async def test_event_result_type_enforcement_with_dict(self):
        """Test that handlers returning wrong types get errors when event expects dict result."""
        bus = EventBus(name='TestBus')

        # Create an event that expects dict results
        class DictResultEvent(BaseEvent[dict]):
            pass

        # Create handlers with different return types
        async def dict_handler1(event):
            return {'key1': 'value1'}

        async def dict_handler2(event):
            return {'key2': 'value2'}

        async def string_handler(event):
            return 'this is a string, not a dict'

        async def int_handler(event):
            return 42

        async def list_handler(event):
            return [1, 2, 3]

        # Register all handlers
        bus.on('DictResultEvent', dict_handler1)
        bus.on('DictResultEvent', dict_handler2)
        bus.on('DictResultEvent', string_handler)
        bus.on('DictResultEvent', int_handler)
        bus.on('DictResultEvent', list_handler)

        try:
            # Dispatch event
            event = bus.dispatch(DictResultEvent())
            await bus.wait_until_idle()
            event = await event

            # Check that handlers returning dicts succeeded
            dict_results = [r for r in event.event_results.values() if r.handler_name in ['dict_handler1', 'dict_handler2']]
            assert all(r.status == 'completed' for r in dict_results)
            assert all(isinstance(r.result, dict) for r in dict_results)

            # Check that handlers returning wrong types have errors
            wrong_type_results = [
                r for r in event.event_results.values() if r.handler_name in ['string_handler', 'int_handler', 'list_handler']
            ]
            assert all(r.status == 'error' for r in wrong_type_results)
            assert all(r.error is not None for r in wrong_type_results)

            # Check error messages mention type mismatch
            for result in wrong_type_results:
                error_msg = str(result.error)
                assert 'did not match expected event_result_type' in error_msg
                assert 'dict' in error_msg

            # event_results_flat_dict should still work when raise_if_any=False, only including valid dict results
            dict_result = await event.event_results_flat_dict(raise_if_any=False)
            assert 'key1' in dict_result and 'key2' in dict_result
            assert len(dict_result) == 2  # Only the two dict results

        finally:
            await bus.stop(timeout=0, clear=True)

    async def test_event_result_type_enforcement_with_list(self):
        """Test that handlers returning wrong types get errors when event expects list result."""
        bus = EventBus(name='TestBus')

        # Create an event that expects list results
        class ListResultEvent(BaseEvent[list]):
            pass

        # Create handlers with different return types
        async def list_handler1(event):
            return [1, 2, 3]

        async def list_handler2(event):
            return ['a', 'b', 'c']

        async def dict_handler(event):
            return {'key': 'value'}

        async def string_handler(event):
            return 'not a list'

        async def int_handler(event):
            return 99

        # Register all handlers
        bus.on('ListResultEvent', list_handler1)
        bus.on('ListResultEvent', list_handler2)
        bus.on('ListResultEvent', dict_handler)
        bus.on('ListResultEvent', string_handler)
        bus.on('ListResultEvent', int_handler)

        try:
            # Dispatch event
            event = bus.dispatch(ListResultEvent())
            await bus.wait_until_idle()
            event = await event

            # Check that handlers returning lists succeeded
            list_results = [r for r in event.event_results.values() if r.handler_name in ['list_handler1', 'list_handler2']]
            assert all(r.status == 'completed' for r in list_results)
            assert all(isinstance(r.result, list) for r in list_results)

            # Check that handlers returning wrong types have errors
            wrong_type_results = [
                r for r in event.event_results.values() if r.handler_name in ['dict_handler', 'string_handler', 'int_handler']
            ]
            assert all(r.status == 'error' for r in wrong_type_results)
            assert all(r.error is not None for r in wrong_type_results)

            # Check error messages mention type mismatch
            for result in wrong_type_results:
                error_msg = str(result.error)
                assert 'did not match expected event_result_type' in error_msg
                assert 'list' in error_msg

            # event_results_flat_list should still work when raise_if_any=False, only including valid list results
            list_result = await event.event_results_flat_list(raise_if_any=False)
            assert list_result == [1, 2, 3, 'a', 'b', 'c']  # Flattened from both list handlers

        finally:
            await bus.stop(timeout=0, clear=True)
