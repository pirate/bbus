from typing import Any, cast
from uuid import uuid4

import pytest

from bubus.models import BaseEvent, EventHandler, EventHandlerCallable, EventResult, get_handler_id
from bubus.service import EventBus


class _StubEvent:
    """Minimal event-like object used to verify EventResult independence."""

    def __init__(self):
        self.event_id = 'stub-event'
        self.event_children: list[BaseEvent | _StubEvent] = []
        self.event_result_type = str
        self.event_timeout = 0.5
        self.event_completed_at = None
        self.event_results: dict[str, EventResult] = {}
        self._cancelled_due_to_error: BaseException | None = None

    def event_cancel_pending_child_processing(self, error: BaseException) -> None:
        self._cancelled_due_to_error = error


@pytest.mark.asyncio
async def test_event_result_execute_without_base_event() -> None:
    """EventResult should execute without requiring a real BaseEvent or EventBus."""

    stub_event = _StubEvent()

    async def handler(event: _StubEvent) -> str:
        return 'ok'

    handler_entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StubEvent',
        eventbus_name='Standalone',
        eventbus_id='standalone-1',
    )

    event_result = EventResult(
        event_id=str(uuid4()),
        handler=handler_entry,
        timeout=stub_event.event_timeout,
        result_schema=str,
    )

    test_bus = EventBus(name='StandaloneTest1')
    result_value = await event_result.execute(
        cast(BaseEvent[Any], stub_event),
        eventbus=test_bus,
        timeout=stub_event.event_timeout,
    )

    assert result_value == 'ok'
    assert event_result.status == 'completed'
    assert event_result.result == 'ok'
    assert stub_event.__dict__.get('_cancelled_due_to_error') is None
    await test_bus.stop()


class StandaloneEvent(BaseEvent[str]):
    data: str


@pytest.mark.asyncio
async def test_event_and_result_without_eventbus() -> None:
    """Verify BaseEvent + EventResult work without instantiating an EventBus."""

    event = StandaloneEvent(data='message')

    def handler(evt: StandaloneEvent) -> str:
        return evt.data.upper()

    handler_id = get_handler_id(cast(EventHandlerCallable, handler), None)
    pending_results = event.event_create_pending_results({handler_id: cast(EventHandlerCallable, handler)})
    event_result = pending_results[handler_id]

    test_bus = EventBus(name='StandaloneTest2')
    value = await event_result.execute(
        event,
        eventbus=test_bus,
        timeout=event.event_timeout,
    )

    assert value == 'MESSAGE'
    assert event_result.status == 'completed'
    assert event.event_results[handler_id] is event_result

    event.event_mark_complete_if_all_handlers_completed()
    assert event.event_completed_at is not None
    await test_bus.stop()


def test_event_handler_model_is_serializable() -> None:
    """EventHandler is a Pydantic model and can round-trip serialized metadata."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    dumped = entry.model_dump(mode='json')
    assert dumped['event_pattern'] == 'StandaloneEvent'
    assert dumped['eventbus_name'] == 'StandaloneBus'
    assert dumped.get('handler') is None

    loaded = EventHandler.model_validate(dumped)
    assert loaded.id == entry.id
    assert loaded.event_pattern == entry.event_pattern
    assert loaded.handler is None


def test_event_handler_model_detects_handler_file_path() -> None:
    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    assert entry.handler_file_path is not None
    expected_suffix = f'test_event_result_standalone.py:{handler.__code__.co_firstlineno}'
    assert entry.handler_file_path.endswith(expected_suffix)


def test_event_result_serializes_handler_metadata_and_derived_fields() -> None:
    """EventResult stores handler metadata and derives convenience fields from it."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    result = EventResult(
        event_id=str(uuid4()),
        handler=entry,
    )
    payload = result.model_dump(mode='json')

    assert payload['handler']['id'] == entry.id
    assert payload['handler']['handler_name'] == entry.handler_name
    assert payload['handler_id'] == entry.id
    assert payload['handler_name'] == entry.handler_name
    assert payload['eventbus_id'] == entry.eventbus_id
    assert payload['eventbus_name'] == entry.eventbus_name

    # Legacy constructor fields still round-trip into handler metadata.
    legacy = EventResult.model_validate(
        {
            'event_id': str(uuid4()),
            'handler_id': '123.456',
            'handler_name': 'legacy_handler',
            'eventbus_id': '42',
            'eventbus_name': 'LegacyBus',
        }
    )
    assert legacy.handler_id == '123.456'
    assert legacy.handler_name == 'legacy_handler'
    assert legacy.eventbus_id == '42'
    assert legacy.eventbus_name == 'LegacyBus'
