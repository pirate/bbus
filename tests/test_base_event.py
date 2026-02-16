import asyncio
import gc

import pytest

from bubus import BaseEvent, EventBus


@pytest.fixture(autouse=True)
async def cleanup_eventbus_instances():
    """Ensure EventBus instances are cleaned up between tests"""
    yield
    # Force garbage collection to clean up any lingering EventBus instances
    gc.collect()
    # Give event loops time to clean up
    await asyncio.sleep(0.01)


class MainEvent(BaseEvent[None]):
    message: str = 'test'


class ChildEvent(BaseEvent[None]):
    data: str = 'child'


class GrandchildEvent(BaseEvent[None]):
    info: str = 'grandchild'


async def test_event_bus_property_single_bus():
    """Test event_bus property with a single EventBus instance"""
    bus = EventBus(name='TestBus')

    # Track if handler was called
    handler_called = False
    dispatched_child = None

    async def handler(event: MainEvent):
        nonlocal handler_called, dispatched_child
        handler_called = True

        # Should be able to access event_bus inside handler
        assert event.event_bus == bus
        assert event.event_bus.name == 'TestBus'

        # Should be able to dispatch child events using the property
        dispatched_child = await event.event_bus.dispatch(ChildEvent())

    bus.on(MainEvent, handler)

    # Dispatch event and wait for completion
    await bus.dispatch(MainEvent())

    assert handler_called
    assert dispatched_child is not None
    assert isinstance(dispatched_child, ChildEvent)

    await bus.stop()


async def test_event_bus_property_multiple_buses():
    """Test event_bus property with multiple EventBus instances"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    handler1_called = False
    handler2_called = False

    async def handler1(event: MainEvent):
        nonlocal handler1_called
        handler1_called = True
        # Inside bus1 handler, event_bus should return bus1
        assert event.event_bus == bus1
        assert event.event_bus.name == 'Bus1'

    async def handler2(event: MainEvent):
        nonlocal handler2_called
        handler2_called = True
        # Inside bus2 handler, event_bus should return bus2
        assert event.event_bus == bus2
        assert event.event_bus.name == 'Bus2'

    bus1.on(MainEvent, handler1)
    bus2.on(MainEvent, handler2)

    # Dispatch to bus1
    await bus1.dispatch(MainEvent(message='bus1'))
    assert handler1_called

    # Dispatch to bus2
    await bus2.dispatch(MainEvent(message='bus2'))
    assert handler2_called

    await bus1.stop()
    await bus2.stop()


async def test_event_bus_property_with_forwarding():
    """Test event_bus property with event forwarding between buses"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    # Forward all events from bus1 to bus2
    bus1.on('*', bus2.dispatch)

    handler_bus = None
    handler_complete = asyncio.Event()

    async def handler(event: MainEvent):
        nonlocal handler_bus
        # When forwarded, the event_bus should be the bus currently processing
        handler_bus = event.event_bus
        handler_complete.set()

    bus2.on(MainEvent, handler)

    # Dispatch to bus1, which forwards to bus2
    event = bus1.dispatch(MainEvent())

    # Wait for handler to complete
    await handler_complete.wait()

    # The handler in bus2 should see bus2 as the event_bus
    assert handler_bus is not None
    assert handler_bus.name == 'Bus2'
    # Verify it's the same bus instance (they should be the same object)
    assert handler_bus is bus2

    # Also wait for the event to fully complete
    await event

    await bus1.stop()
    await bus2.stop()


async def test_event_bus_property_outside_handler():
    """Test that event_bus property raises error when accessed outside handler"""
    bus = EventBus(name='TestBus')

    event = MainEvent()

    # Should raise error when accessed outside handler context
    with pytest.raises(AttributeError, match='event_bus property can only be accessed from within an event handler'):
        _ = event.event_bus

    # Even after dispatching, accessing outside handler should fail
    dispatched_event = await bus.dispatch(event)

    with pytest.raises(AttributeError, match='event_bus property can only be accessed from within an event handler'):
        _ = dispatched_event.event_bus

    await bus.stop()


async def test_event_bus_property_nested_handlers():
    """Test event_bus property in nested handler scenarios"""
    bus = EventBus(name='MainBus')

    inner_bus_name = None

    async def outer_handler(event: MainEvent):
        # Dispatch a child event from within handler
        child = ChildEvent()

        async def inner_handler(child_event: ChildEvent):
            nonlocal inner_bus_name
            # Both parent and child should see the same bus
            assert child_event.event_bus == event.event_bus
            inner_bus_name = child_event.event_bus.name

        bus.on(ChildEvent, inner_handler)
        await event.event_bus.dispatch(child)

    bus.on(MainEvent, outer_handler)

    await bus.dispatch(MainEvent())

    assert inner_bus_name == 'MainBus'

    await bus.stop()


async def test_event_bus_property_no_active_bus():
    """Test event_bus property when EventBus has been garbage collected"""
    # This is a tricky edge case - create and destroy a bus

    event = None

    async def create_and_dispatch():
        nonlocal event
        bus = EventBus(name='TempBus')

        async def handler(e: MainEvent):
            # Save the event for later
            nonlocal event
            event = e

        bus.on(MainEvent, handler)
        await bus.dispatch(MainEvent())
        await bus.stop()
        # Bus goes out of scope here and may be garbage collected

    await create_and_dispatch()

    # Force garbage collection
    import gc

    gc.collect()

    # Event exists but bus might be gone
    assert event is not None

    # Create a new handler context to test
    new_bus = EventBus(name='NewBus')

    error_raised = False

    async def new_handler(e: MainEvent):
        nonlocal error_raised
        assert event is not None
        try:
            # The old event doesn't belong to this bus
            _ = event.event_bus
        except RuntimeError:
            error_raised = True

    new_bus.on(MainEvent, new_handler)
    await new_bus.dispatch(MainEvent())

    # Should have raised an error since the original bus is gone
    assert error_raised

    await new_bus.stop()


async def test_event_bus_property_child_dispatch():
    """Test event_bus property when dispatching child events from handlers"""
    bus = EventBus(name='MainBus')

    # Track execution order and bus references
    execution_order: list[str] = []
    child_event_ref = None
    grandchild_event_ref = None

    async def parent_handler(event: MainEvent):
        execution_order.append('parent_start')

        # Verify we can access event_bus
        assert event.event_bus == bus
        assert event.event_bus.name == 'MainBus'

        # Dispatch a child event using event.event_bus
        nonlocal child_event_ref
        child_event_ref = event.event_bus.dispatch(ChildEvent(data='from_parent'))

        # The child event should start processing immediately within our handler
        # (due to the deadlock prevention in BaseEvent.__await__)
        await child_event_ref

        execution_order.append('parent_end')

    async def child_handler(event: ChildEvent):
        execution_order.append('child_start')

        # Child should see the same bus
        assert event.event_bus == bus
        assert event.event_bus.name == 'MainBus'
        assert event.data == 'from_parent'

        # Dispatch a grandchild event
        nonlocal grandchild_event_ref
        grandchild_event_ref = event.event_bus.dispatch(GrandchildEvent(info='from_child'))

        # Wait for grandchild to complete
        await grandchild_event_ref

        execution_order.append('child_end')

    async def grandchild_handler(event: GrandchildEvent):
        execution_order.append('grandchild_start')

        # Grandchild should also see the same bus
        assert event.event_bus == bus
        assert event.event_bus.name == 'MainBus'
        assert event.info == 'from_child'

        execution_order.append('grandchild_end')

    # Register handlers
    bus.on(MainEvent, parent_handler)
    bus.on(ChildEvent, child_handler)
    bus.on(GrandchildEvent, grandchild_handler)

    # Dispatch the parent event
    parent_event = await bus.dispatch(MainEvent(message='start'))

    # Verify execution order - child events should complete before parent
    assert execution_order == ['parent_start', 'child_start', 'grandchild_start', 'grandchild_end', 'child_end', 'parent_end']

    # Verify all events completed
    assert parent_event.event_status == 'completed'
    assert child_event_ref is not None
    assert child_event_ref.event_status == 'completed'
    assert grandchild_event_ref is not None
    assert grandchild_event_ref.event_status == 'completed'

    # Verify parent-child relationships
    assert child_event_ref.event_parent_id == parent_event.event_id
    assert grandchild_event_ref.event_parent_id == child_event_ref.event_id

    await bus.stop()


async def test_event_bus_property_multi_bus_child_dispatch():
    """Test event_bus property when child events are dispatched across multiple buses"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    # Forward all events from bus1 to bus2
    bus1.on('*', bus2.dispatch)

    child_dispatch_bus = None
    child_handler_bus = None
    handlers_complete = asyncio.Event()

    async def parent_handler(event: MainEvent):
        # This handler runs in bus2 (due to forwarding)
        assert event.event_bus == bus2

        # Dispatch child using event.event_bus (should dispatch to bus2)
        nonlocal child_dispatch_bus
        child_dispatch_bus = event.event_bus
        await event.event_bus.dispatch(ChildEvent(data='from_bus2_handler'))

    async def child_handler(event: ChildEvent):
        # Child handler should see bus2 as well
        nonlocal child_handler_bus
        child_handler_bus = event.event_bus
        assert event.data == 'from_bus2_handler'
        handlers_complete.set()

    # Only register handlers on bus2
    bus2.on(MainEvent, parent_handler)
    bus2.on(ChildEvent, child_handler)

    # Dispatch to bus1, which forwards to bus2
    parent_event = bus1.dispatch(MainEvent(message='start'))

    # Wait for handlers to complete
    await asyncio.wait_for(handlers_complete.wait(), timeout=5.0)

    # Also await the parent event
    await parent_event

    # Verify child was dispatched to bus2
    assert child_dispatch_bus is not None
    assert child_handler_bus is not None
    assert id(child_dispatch_bus) == id(bus2)
    assert id(child_handler_bus) == id(bus2)

    await bus1.stop()
    await bus2.stop()
