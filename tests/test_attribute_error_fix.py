"""Test that the AttributeError bug related to 'event_completed_at' is fixed"""

import asyncio
from datetime import UTC, datetime

from bubus import BaseEvent, EventBus


class SampleEvent(BaseEvent[str]):
    data: str = 'test'


def test_event_started_at_with_deserialized_event():
    """Test that event_started_at works even with events created through deserialization"""
    # Create an event and convert to dict (simulating serialization)
    event = SampleEvent(data='original')
    event_dict = event.model_dump()

    # Create a new event from the dict (simulating deserialization)
    deserialized_event = SampleEvent.model_validate(event_dict)

    # This should not raise AttributeError
    assert deserialized_event.event_started_at is None
    assert deserialized_event.event_completed_at is None


def test_event_started_at_with_json_deserialization():
    """Test that event_started_at works with JSON deserialization"""
    # Create an event and convert to JSON
    event = SampleEvent(data='json_test')
    json_str = event.model_dump_json()

    # Create a new event from JSON
    deserialized_event = SampleEvent.model_validate_json(json_str)

    # This should not raise AttributeError
    assert deserialized_event.event_started_at is None
    assert deserialized_event.event_completed_at is None


async def test_event_started_at_after_processing():
    """Test that event_started_at works correctly after event processing"""
    bus = EventBus(name='TestBus')

    # Handler that does nothing
    async def test_handler(event: SampleEvent) -> str:
        await asyncio.sleep(0.01)
        return 'done'

    bus.on('SampleEvent', test_handler)

    # Dispatch event
    event = await bus.dispatch(SampleEvent(data='processing_test'))

    # Check timestamps - should not raise AttributeError
    assert event.event_started_at is not None
    assert event.event_completed_at is not None
    assert isinstance(event.event_started_at, datetime)
    assert isinstance(event.event_completed_at, datetime)

    await bus.stop()


async def test_event_without_handlers():
    """Test that events without handlers still work with timestamp properties"""
    event = SampleEvent(data='no_handlers')

    # Should not raise AttributeError when accessing these properties
    assert event.event_started_at is None  # No handlers started
    assert event.event_completed_at is None  # Not complete yet

    # Initialize the completion signal (normally done when dispatched)
    _ = event.event_completed_signal

    # Mark as completed manually (simulating what happens in event_mark_complete_if_all_handlers_completed)
    event.event_mark_complete_if_all_handlers_completed()

    # After marking complete, it should be set
    # When no handlers but event is completed, event_started_at returns event_completed_at
    assert event.event_started_at is not None  # Uses event_completed_at
    assert event.event_completed_at is not None  # Now it's complete


def test_event_with_manually_set_completed_at():
    """Test events where event_completed_at is manually set (like in test_log_history_tree.py)"""
    event = SampleEvent(data='manual')

    # Initialize the completion signal
    _ = event.event_completed_signal

    # Manually set the completed timestamp (as done in tests)
    if hasattr(event, 'event_completed_at'):
        event.event_completed_at = datetime.now(UTC)

    # Should not raise AttributeError
    assert event.event_started_at is not None  # Should use event_completed_at
    # Note: Since we set event_completed_at and there are no handlers, event_completed_at will also return event_completed_at
    assert event.event_completed_at is not None

    # Add a handler result to make it incomplete
    event.event_result_update(handler=lambda e: None, status='started')
    assert event.event_completed_at is None  # Now it's not complete

    # Complete the handler
    list(event.event_results.values())[0].update(status='completed', result='done')
    event.event_mark_complete_if_all_handlers_completed()
    assert event.event_completed_at is not None


def test_event_copy_preserves_private_attrs():
    """Test that copying events preserves private attributes"""
    event = SampleEvent(data='copy_test')

    # Access properties to ensure private attrs are initialized
    _ = event.event_started_at
    _ = event.event_completed_at

    # Create a copy using model_copy
    copied_event = event.model_copy()

    # Should not raise AttributeError
    assert copied_event.event_started_at is None
    assert copied_event.event_completed_at is None


def test_event_started_at_is_serialized_and_recomputed_each_dump():
    """event_started_at should be included in JSON dumps and recomputed each read (not cached)."""
    event = SampleEvent(data='serialize-started-at')

    pending_payload = event.model_dump(mode='json')
    assert 'event_started_at' in pending_payload
    assert pending_payload['event_started_at'] is None

    event.event_result_update(handler=lambda e: None, status='started')
    first_started_at = event.model_dump(mode='json')['event_started_at']
    assert isinstance(first_started_at, str)

    forced_started_at = datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    result = next(iter(event.event_results.values()))
    result.started_at = forced_started_at

    second_started_at = event.model_dump(mode='json')['event_started_at']
    assert isinstance(second_started_at, str)
    parsed = datetime.fromisoformat(second_started_at.replace('Z', '+00:00'))
    assert parsed == forced_started_at


def test_event_status_is_serialized_and_recomputed_each_dump():
    """event_status should be included in JSON dumps and track live lifecycle state."""
    event = SampleEvent(data='serialize-status')

    pending_payload = event.model_dump(mode='json')
    assert pending_payload['event_status'] == 'pending'

    result = event.event_result_update(handler=lambda e: None, status='started')
    started_payload = event.model_dump(mode='json')
    assert started_payload['event_status'] == 'started'

    result.update(status='completed', result='ok')
    event.event_mark_complete_if_all_handlers_completed()
    completed_payload = event.model_dump(mode='json')
    assert completed_payload['event_status'] == 'completed'
