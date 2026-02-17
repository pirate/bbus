import asyncio
import time

from bubus import BaseEvent, EventBus
from bubus.retry import retry


class TestRetryWithEventBus:
    """Test @retry decorator with EventBus handlers."""

    async def test_retry_decorator_on_eventbus_handler(self):
        """Test that @retry decorator works correctly when applied to EventBus handlers."""
        handler_calls: list[tuple[str, float]] = []

        class TestEvent(BaseEvent[str]):
            """Simple test event."""

            message: str

        bus = EventBus(name='test_retry_bus')

        @retry(
            max_attempts=3,
            retry_after=0.1,
            timeout=1.0,
            semaphore_limit=1,
            semaphore_scope='global',
        )
        async def retrying_handler(event: TestEvent) -> str:
            call_time = time.time()
            handler_calls.append(('called', call_time))

            if len(handler_calls) < 3:
                raise ValueError(f'Attempt {len(handler_calls)} failed')

            return f'Success: {event.message}'

        bus.on('TestEvent', retrying_handler)

        event = TestEvent(message='Hello retry!')
        completed_event = await bus.emit(event)
        await bus.wait_until_idle(timeout=5)

        assert len(handler_calls) == 3, f'Expected 3 attempts, got {len(handler_calls)}'
        for i in range(1, len(handler_calls)):
            delay = handler_calls[i][1] - handler_calls[i - 1][1]
            assert delay >= 0.08, f'Retry delay {i} was {delay:.3f}s, expected >= 0.08s'

        assert completed_event.event_status == 'completed'
        handler_result = await completed_event.event_result()
        assert handler_result == 'Success: Hello retry!'

        await bus.stop()

    async def test_retry_with_semaphore_on_multiple_handlers(self):
        """Test @retry decorator with semaphore limiting concurrent handler executions."""
        active_handlers: list[int] = []
        max_concurrent = 0
        handler_results: dict[int, list[tuple[str, float]]] = {1: [], 2: [], 3: [], 4: []}

        class WorkEvent(BaseEvent[str]):
            """Event that triggers work."""

            work_id: int

        bus = EventBus(name='test_concurrent_bus', event_handler_concurrency='parallel')

        async def create_handler(handler_id: int):
            @retry(
                max_attempts=1,
                timeout=5.0,
                semaphore_limit=2,
                semaphore_name='test_handler_sem',
                semaphore_scope='global',
            )
            async def limited_handler(event: WorkEvent) -> str:
                nonlocal max_concurrent
                active_handlers.append(handler_id)
                handler_results[handler_id].append(('started', time.time()))

                current_concurrent = len(active_handlers)
                max_concurrent = max(max_concurrent, current_concurrent)
                await asyncio.sleep(0.2)

                active_handlers.remove(handler_id)
                handler_results[handler_id].append(('completed', time.time()))
                return f'Handler {handler_id} processed work {event.work_id}'

            limited_handler.__name__ = f'limited_handler_{handler_id}'
            return limited_handler

        for i in range(1, 5):
            handler = await create_handler(i)
            bus.on('WorkEvent', handler)

        event = WorkEvent(work_id=1)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=3)

        assert max_concurrent <= 2, f'Max concurrent was {max_concurrent}, expected <= 2'
        for handler_id in range(1, 5):
            assert len(handler_results[handler_id]) == 2, f'Handler {handler_id} should have started and completed'

        all_starts = [r[1] for results in handler_results.values() for r in results if r[0] == 'started']
        all_ends = [r[1] for results in handler_results.values() for r in results if r[0] == 'completed']
        total_time = max(all_ends) - min(all_starts)
        assert 0.35 < total_time < 0.6, f'Total execution time was {total_time:.3f}s, expected ~0.4s'

        await bus.stop()

    async def test_retry_timeout_with_eventbus_handler(self):
        """Test that retry timeout works correctly with EventBus handlers."""

        class TimeoutEvent(BaseEvent[str]):
            """Event for timeout testing."""

            test_id: str
            event_timeout: float | None = 1

        bus = EventBus(name='test_timeout_bus')
        handler_started = False

        @retry(
            max_attempts=1,
            timeout=0.2,
        )
        async def wrapped_handler(event: TimeoutEvent) -> str:
            nonlocal handler_started
            handler_started = True
            await asyncio.sleep(5)
            return 'Should not reach here'

        bus.on(TimeoutEvent, wrapped_handler)

        event = TimeoutEvent(test_id='7ebbd9f4-755a-7f13-828a-183dfe2d4302')
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert handler_started, 'Handler should have started'
        assert len(event.event_results) == 1
        result = next(iter(event.event_results.values()))
        assert result.status == 'error'
        assert result.error is not None
        assert isinstance(result.error, TimeoutError)

        await bus.stop()

    async def test_retry_with_event_type_filter(self):
        """Test retry decorator with specific exception types."""

        class RetryTestEvent(BaseEvent[str]):
            """Event for testing retry on specific exceptions."""

            attempt_limit: int

        bus = EventBus(name='test_exception_filter_bus')
        attempt_count = 0

        @retry(
            max_attempts=4,
            retry_after=0.05,
            timeout=1.0,
            retry_on_errors=[ValueError, RuntimeError],
        )
        async def selective_retry_handler(event: RetryTestEvent) -> str:
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count == 1:
                raise ValueError('This should be retried')
            if attempt_count == 2:
                raise RuntimeError('This should also be retried')
            if attempt_count == 3:
                raise TypeError('This should NOT be retried')

            return 'Success'

        bus.on('RetryTestEvent', selective_retry_handler)

        event = RetryTestEvent(attempt_limit=3)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert attempt_count == 3, f'Expected 3 attempts, got {attempt_count}'
        handler_id = list(event.event_results.keys())[0]
        result = event.event_results[handler_id]
        assert result.status == 'error'
        assert isinstance(result.error, TypeError)
        assert 'This should NOT be retried' in str(result.error)

        await bus.stop()
