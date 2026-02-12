"""Test typed event results with automatic casting."""

# pyright: reportAssertTypeFailure=false
# pyright: reportUnnecessaryIsInstance=false

import asyncio
from typing import Any, Literal, assert_type

from pydantic import BaseModel

from bubus import BaseEvent, EventBus


class ScreenshotEventResult(BaseModel):
    screenshot_base64: bytes | None = None
    error: str | None = None


class ScreenshotEvent(BaseEvent[ScreenshotEventResult]):
    screenshot_width: int = 1080
    screenshot_height: int = 900


class StringEvent(BaseEvent[str]):
    pass


class IntEvent(BaseEvent[int]):
    pass


async def test_pydantic_model_result_casting():
    """Test that handler results are automatically cast to Pydantic models."""
    print('\n=== Test Pydantic Model Result Casting ===')

    bus = EventBus(name='pydantic_test_bus')

    def screenshot_handler(event: ScreenshotEvent):
        # Return a dict that should be cast to ScreenshotEventResult
        return {'screenshot_base64': b'fake_screenshot_data', 'error': None}

    bus.on('ScreenshotEvent', screenshot_handler)

    event = ScreenshotEvent(screenshot_width=1920, screenshot_height=1080)
    await bus.dispatch(event)

    # Get the result
    result = await event.event_result()

    # Verify it was cast to the correct type
    assert isinstance(result, ScreenshotEventResult)
    assert result.screenshot_base64 == b'fake_screenshot_data'
    assert result.error is None

    print(f'✅ Result correctly cast to {type(result).__name__}: {result}')
    await bus.stop(clear=True)


async def test_builtin_type_casting():
    """Test that handler results are automatically cast to built-in types."""
    print('\n=== Test Built-in Type Casting ===')

    bus = EventBus(name='builtin_test_bus')

    def string_handler(event: StringEvent):
        return '42'  # Return a proper string

    def int_handler(event: IntEvent):
        return 123  # Return a proper int

    bus.on('StringEvent', string_handler)
    bus.on('IntEvent', int_handler)

    # Test string validation
    string_event = StringEvent()
    await bus.dispatch(string_event)
    string_result = await string_event.event_result()
    assert isinstance(string_result, str)
    assert string_result == '42'
    print(f'✅ String "42" validated as str: "{string_result}"')

    # Test int validation
    int_event = IntEvent()
    await bus.dispatch(int_event)
    int_result = await int_event.event_result()
    assert isinstance(int_result, int)
    assert int_result == 123
    print(f'✅ Int 123 validated as int: {int_result}')
    await bus.stop(clear=True)


async def test_casting_failure_handling():
    """Test that casting failures are handled gracefully."""
    print('\n=== Test Casting Failure Handling ===')

    bus = EventBus(name='failure_test_bus')

    def bad_handler(event: IntEvent):
        return 'not_a_number'  # Should fail validation as int

    bus.on('IntEvent', bad_handler)

    event = IntEvent()
    await bus.dispatch(event)

    # The event should complete but the result should be an error
    try:
        await event.event_results_by_handler_id(raise_if_any=False)
        handler_id = list(event.event_results.keys())[0]
        event_result = event.event_results[handler_id]
    except Exception:
        # If event_results_by_handler_id raises, get the result directly
        handler_id = list(event.event_results.keys())[0]
        event_result = event.event_results[handler_id]

    assert event_result.status == 'error'
    assert isinstance(event_result.error, ValueError)
    assert 'expected event_result_type' in str(event_result.error)

    print(f'✅ Casting failure handled: {event_result.error}')
    await bus.stop(clear=True)


async def test_no_casting_when_no_result_schema():
    """Test that events without result_schema work normally."""
    print('\n=== Test No Casting When No Result Schema ===')

    bus = EventBus(name='normal_test_bus')

    class NormalEvent(BaseEvent[None]):
        pass  # No event_result_type specified

    def normal_handler(event: NormalEvent):
        return {'raw': 'data'}

    bus.on('NormalEvent', normal_handler)

    event = NormalEvent()
    await bus.dispatch(event)

    result = await event.event_result()

    # Should remain as original dict, no casting
    assert isinstance(result, dict)
    assert result == {'raw': 'data'}

    print(f'✅ No casting applied: {result}')
    await bus.stop(clear=True)


async def test_result_schema_stored_in_event_result():
    """Test that result_schema is stored in EventResult for inspection."""
    print('\n=== Test Result Schema Stored in EventResult ===')

    bus = EventBus(name='storage_test_bus')

    def handler(event: StringEvent):
        return '123'  # Already a string, will validate successfully

    bus.on('StringEvent', handler)

    event = StringEvent()
    await bus.dispatch(event)

    # Check that result_schema is accessible
    handler_id = list(event.event_results.keys())[0]
    event_result = event.event_results[handler_id]

    assert event_result.result_schema is str
    assert isinstance(event_result.result, str)
    assert event_result.result == '123'

    print(f'✅ Result schema stored: {event_result.result_schema}')
    await bus.stop(clear=True)


async def test_expect_type_inference():
    """Test that EventBus.expect() returns the correct typed event."""
    print('\n=== Test Expect Type Inference ===')

    bus = EventBus(name='expect_type_test_bus')

    class CustomResult(BaseModel):
        data: str

    class SpecificEvent(BaseEvent[CustomResult]):
        request_id: str = 'test123'

    # Validate inline isinstance usage works with await expect()
    async def dispatch_inline_isinstance():
        await asyncio.sleep(0.01)
        bus.dispatch(SpecificEvent(request_id='inline-isinstance'))

    inline_isinstance_task = asyncio.create_task(dispatch_inline_isinstance())
    assert isinstance(await bus.expect(SpecificEvent, timeout=1.0), SpecificEvent)
    await inline_isinstance_task

    # Validate inline assert_type usage works with await expect()
    async def dispatch_inline_assert_type():
        await asyncio.sleep(0.01)
        bus.dispatch(SpecificEvent(request_id='inline-assert-type'))

    inline_type_task = asyncio.create_task(dispatch_inline_assert_type())
    assert_type(await bus.expect(SpecificEvent, timeout=1.0), SpecificEvent | None)
    await inline_type_task

    # Validate assert_type with isinstance expression
    async def dispatch_inline_isinstance_type():
        await asyncio.sleep(0.01)
        bus.dispatch(SpecificEvent(request_id='inline-isinstance-type'))

    inline_isinstance_type_task = asyncio.create_task(dispatch_inline_isinstance_type())
    assert_type(isinstance(await bus.expect(SpecificEvent, timeout=1.0), SpecificEvent), bool)
    await inline_isinstance_type_task

    # Start a task that will dispatch the event
    async def dispatch_later():
        await asyncio.sleep(0.01)
        bus.dispatch(SpecificEvent(request_id='req456'))

    dispatch_task = asyncio.create_task(dispatch_later())

    # Use expect with the event class - should return SpecificEvent type
    expected_event = await bus.expect(SpecificEvent, timeout=1.0)
    assert expected_event is not None
    assert isinstance(expected_event, SpecificEvent)

    # Type checking - this should work without cast
    assert_type(expected_event, SpecificEvent)  # Verify type is SpecificEvent, not BaseEvent[Any]

    # Runtime check
    assert type(expected_event) is SpecificEvent
    assert expected_event.request_id == 'req456'

    # Test with filters - type should still be preserved
    async def dispatch_multiple():
        await asyncio.sleep(0.01)
        bus.dispatch(SpecificEvent(request_id='wrong'))
        bus.dispatch(SpecificEvent(request_id='correct'))

    dispatch_task2 = asyncio.create_task(dispatch_multiple())

    # Expect with include filter
    filtered_event = await bus.expect(
        SpecificEvent,
        include=lambda e: e.request_id == 'correct',
        timeout=1.0,
    )
    assert filtered_event is not None

    assert_type(filtered_event, SpecificEvent)  # Should still be SpecificEvent
    assert isinstance(filtered_event, SpecificEvent)
    assert type(filtered_event) is SpecificEvent
    assert filtered_event.request_id == 'correct'

    # Test with string event type - returns BaseEvent[Any]
    async def dispatch_string_event():
        await asyncio.sleep(0.01)
        bus.dispatch(BaseEvent(event_type='StringEvent'))

    dispatch_task3 = asyncio.create_task(dispatch_string_event())
    string_event = await bus.expect('StringEvent', timeout=1.0)
    assert string_event is not None

    assert_type(string_event, BaseEvent[Any])  # Should be BaseEvent[Any]
    assert string_event.event_type == 'StringEvent'

    await dispatch_task
    await dispatch_task2
    await dispatch_task3

    print(f'✅ Expect correctly preserved type: {type(expected_event).__name__}')
    print(f'✅ Expect with filter preserved type: {type(filtered_event).__name__}')
    print('✅ No cast() needed for expect() - type inference works!')
    await bus.stop(clear=True)


async def test_query_type_inference():
    """Test that EventBus.query() returns the correct typed event."""
    print('\n=== Test Query Type Inference ===')

    bus = EventBus(name='query_type_test_bus')

    class QueryEvent(BaseEvent[str]):
        pass

    # Dispatch an event so it appears in history
    event = bus.dispatch(QueryEvent())
    await bus.wait_until_idle()

    assert isinstance(await bus.query(QueryEvent, since=10), QueryEvent)
    assert_type(await bus.query(QueryEvent, since=10), QueryEvent | None)
    assert_type(isinstance(await bus.query(QueryEvent, since=10), QueryEvent), bool)
    queried = await bus.query(QueryEvent, since=10)

    assert queried is not None
    assert isinstance(queried, QueryEvent)
    assert_type(queried, QueryEvent)
    assert queried.event_id == event.event_id

    print(f'✅ Query correctly preserved type: {type(queried).__name__}')
    await bus.stop(clear=True)


async def test_dispatch_type_inference():
    """Test that EventBus.dispatch() returns the same type as its input."""
    print('\n=== Test Dispatch Type Inference ===')

    bus = EventBus(name='type_inference_test_bus')

    class CustomResult(BaseModel):
        value: str

    class CustomEvent(BaseEvent[CustomResult]):
        pass

    # Create an event instance
    original_event = CustomEvent()

    # Dispatch should return the same type WITHOUT needing cast()
    dispatched_event = bus.dispatch(original_event)
    assert isinstance(dispatched_event, CustomEvent)

    # Type checking - this should work without cast
    assert_type(dispatched_event, CustomEvent)  # Should be CustomEvent, not BaseEvent[Any]

    # Runtime check
    assert type(dispatched_event) is CustomEvent
    assert dispatched_event is original_event  # Should be the same object

    # The returned event should be fully typed
    async def handler(event: CustomEvent) -> CustomResult:
        return CustomResult(value='test')

    bus.on('CustomEvent', handler)

    # Validate inline isinstance usage works with dispatch()
    another_event = CustomEvent()
    assert isinstance(bus.dispatch(another_event), CustomEvent)

    # Validate assert_type captures dispatch() return type when called inline
    type_event = CustomEvent()
    dispatched_type_event = bus.dispatch(type_event)
    assert_type(dispatched_type_event, CustomEvent)

    # Validate assert_type with isinstance expression using dispatch()
    isinstance_type_event = CustomEvent()
    assert_type(isinstance(bus.dispatch(isinstance_type_event), CustomEvent), Literal[True])

    # We should be able to use it without casting
    result = await dispatched_event.event_result()

    # Type checking for the result
    assert_type(result, CustomResult | None)  # Should be CustomResult | None

    # Test that we can access type-specific attributes without cast
    # This would fail type checking if dispatched_event was BaseEvent[Any]
    assert dispatched_event.event_type == 'CustomEvent'

    # Demonstrate the improvement - no cast needed!
    # Before: event = cast(CustomEvent, bus.dispatch(CustomEvent()))
    # After: event = bus.dispatch(CustomEvent())  # Type is preserved!

    await another_event.event_result()
    await type_event.event_result()
    await isinstance_type_event.event_result()

    print(f'✅ Dispatch correctly preserved type: {type(dispatched_event).__name__}')
    print('✅ No cast() needed - type inference works!')
    await bus.stop(clear=True)


async def test_typed_event_results():
    """Run all typed event result tests."""
    await test_pydantic_model_result_casting()
    await test_builtin_type_casting()
    await test_casting_failure_handling()
    await test_no_casting_when_no_result_schema()
    await test_result_schema_stored_in_event_result()
    await test_expect_type_inference()
    await test_query_type_inference()
    await test_dispatch_type_inference()
    print('\n🎉 All typed event result tests passed!')


if __name__ == '__main__':
    asyncio.run(test_pydantic_model_result_casting())
