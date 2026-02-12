"""Simple test for typed event results."""

import asyncio
from typing import Any

from pydantic import BaseModel

from bubus import BaseEvent, EventBus


class MyResult(BaseModel):
    value: str
    count: int


class TypedEvent(BaseEvent[MyResult]):
    event_result_type: Any = MyResult


async def test_simple_typed_result():
    """Test a simple typed result."""
    bus = EventBus(name='simple_test')

    def handler(event: TypedEvent) -> MyResult:
        return MyResult.model_validate({'value': 'hello', 'count': 42})

    bus.on('TypedEvent', handler)

    event = TypedEvent()
    completed_event = await bus.dispatch(event)

    # Check the result was cast to the correct type
    handler_ids = list(completed_event.event_results.keys())
    if handler_ids:
        result_obj = completed_event.event_results[handler_ids[0]]
        print(f'Result type: {type(result_obj.result)}')
        print(f'Result: {result_obj.result}')
        print(f'Status: {result_obj.status}')
        print(f'Result type setting: {result_obj.result_type}')
        if result_obj.error:
            print(f'Error: {result_obj.error}')

        # Let's test different constructor approaches
        import pydantic

        print(f'Pydantic version: {pydantic.VERSION}')

        try:
            test_result1 = MyResult.model_validate({'value': 'hello', 'count': 42})
            print(f'Constructor with dict: {test_result1}')
        except Exception as e:
            print(f'Constructor with dict fails: {e}')

    else:
        print('No results found')

    await bus.stop(clear=True)
    print('✅ Simple typed result test completed')


if __name__ == '__main__':
    asyncio.run(test_simple_typed_result())
