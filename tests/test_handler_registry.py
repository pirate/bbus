from typing import Any

import pytest

from bubus.models import BaseEvent, EventHandler
from bubus.service import EventBus


@pytest.mark.asyncio
async def test_on_stores_eventhandler_entry_and_index() -> None:
    bus = EventBus(name='RegistryBus')

    def handler(event: BaseEvent[Any]) -> str:
        return event.event_type

    entry = bus.on('RegistryEvent', handler)

    assert isinstance(entry, EventHandler)
    assert entry.id is not None
    assert entry.id in bus.handlers
    assert bus.handlers[entry.id] is entry
    assert 'RegistryEvent' in bus.handlers_by_key
    assert entry.id in bus.handlers_by_key['RegistryEvent']

    dispatched = bus.dispatch(BaseEvent(event_type='RegistryEvent'))
    completed = await dispatched
    assert entry.id in completed.event_results
    assert completed.event_results[entry.id].handler.id == entry.id

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_off_removes_by_callable_id_entry_or_all() -> None:
    bus = EventBus(name='RegistryOffBus')

    def handler_a(event: BaseEvent[Any]) -> None:
        return None

    def handler_b(event: BaseEvent[Any]) -> None:
        return None

    def handler_c(event: BaseEvent[Any]) -> None:
        return None

    entry_a = bus.on('RegistryEvent', handler_a)
    entry_b = bus.on('RegistryEvent', handler_b)
    entry_c = bus.on('RegistryEvent', handler_c)
    assert entry_a.id and entry_b.id and entry_c.id

    bus.off('RegistryEvent', handler_a)
    assert entry_a.id not in bus.handlers
    assert entry_a.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_b.id in bus.handlers

    bus.off('RegistryEvent', entry_b.id)
    assert entry_b.id not in bus.handlers
    assert entry_b.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_c.id in bus.handlers

    bus.off('RegistryEvent', entry_c)
    assert entry_c.id not in bus.handlers
    assert 'RegistryEvent' not in bus.handlers_by_key

    bus.on('RegistryEvent', handler_a)
    bus.on('RegistryEvent', handler_b)
    bus.off('RegistryEvent')
    assert 'RegistryEvent' not in bus.handlers_by_key
    assert all(entry.event_pattern != 'RegistryEvent' for entry in bus.handlers.values())

    await bus.stop(clear=True)
