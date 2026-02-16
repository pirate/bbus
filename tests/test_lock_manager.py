import asyncio
import contextvars
from typing import cast

import pytest

from bubus import BaseEvent, EventBus
from bubus.lock_manager import LockManager


class LockEvent(BaseEvent[str]):
    pass


def test_lock_manager_uses_context_manager_api_only() -> None:
    assert hasattr(LockManager, 'with_event_lock')
    assert hasattr(LockManager, 'with_handler_lock')
    assert hasattr(LockManager, 'with_handler_dispatch_context')

    # Legacy alias methods were intentionally removed in the refactor.
    assert not hasattr(LockManager, 'lock_for_event')
    assert not hasattr(LockManager, 'lock_for_event_handler')
    assert not hasattr(LockManager, 'lock_context_for_current_handler')


async def test_handler_dispatch_context_preserves_reentrant_locking_in_dispatch_context() -> None:
    bus = EventBus(name='LockDispatchContextBus')
    lock_manager = cast(LockManager, bus.locks)
    event = LockEvent()

    # Simulate a captured dispatch context from outside event processing.
    dispatch_context = contextvars.Context()

    async with lock_manager.with_event_lock(bus, event):

        async def lock_without_dispatch_mark() -> str:
            async with lock_manager.with_event_lock(bus, event):
                return 'acquired'

        blocked_task = asyncio.create_task(lock_without_dispatch_mark(), context=dispatch_context)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(blocked_task, timeout=0.05)
        blocked_task.cancel()
        await asyncio.gather(blocked_task, return_exceptions=True)

        async def lock_with_dispatch_mark() -> str:
            with lock_manager.with_handler_dispatch_context(bus, event):
                async with lock_manager.with_event_lock(bus, event):
                    return 'acquired'

        marked_task = asyncio.create_task(lock_with_dispatch_mark(), context=dispatch_context)
        assert await asyncio.wait_for(marked_task, timeout=0.05) == 'acquired'
