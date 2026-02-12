import logging
import time
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

# Define generic type variables for return type and parameters
R = TypeVar('R')
P = ParamSpec('P')


def extract_basemodel_generic_arg(cls: type) -> Any:
    """
    Extract T_EventResultType Generic arg from BaseEvent[T_EventResultType] subclasses using pydantic generic metadata.
    Needed because pydantic messes with the mro and obscures the Generic from the bases list.
    https://github.com/pydantic/pydantic/issues/8410
    """

    def _extract_arg_from_metadata(metadata_value: Any) -> Any:
        metadata = cast(dict[str, Any], metadata_value)
        origin: Any = metadata.get('origin')
        args: tuple[Any, ...] = cast(tuple[Any, ...], metadata.get('args') or ())
        if not args:
            return None
        # Avoid importing BaseEvent here to keep helpers.py decoupled from models.py.
        if getattr(origin, '__name__', None) == 'BaseEvent' and getattr(origin, '__module__', None) == 'bubus.models':
            return args[0]
        return None

    # Direct check first for speed - most subclasses will have it directly
    if hasattr(cls, '__pydantic_generic_metadata__'):
        generic_arg = _extract_arg_from_metadata(getattr(cls, '__pydantic_generic_metadata__'))
        if generic_arg is not None:
            return generic_arg

    # Only check MRO if direct check failed
    for parent in cls.__mro__[1:]:
        if hasattr(parent, '__pydantic_generic_metadata__'):
            generic_arg = _extract_arg_from_metadata(getattr(parent, '__pydantic_generic_metadata__'))
            if generic_arg is not None:
                return generic_arg

    return None


def time_execution(
    additional_text: str = '',
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]]:
    """Decorator that logs how much time execution of a function takes"""

    def decorator(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start_time = time.time()
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            # Only log if execution takes more than 0.25 seconds to avoid spamming the logs
            # you can lower this threshold locally when you're doing dev work to performance optimize stuff
            if execution_time > 0.25:
                self_has_logger = args and getattr(args[0], 'logger', None)
                if self_has_logger:
                    logger = getattr(args[0], 'logger')
                elif 'agent' in kwargs:
                    logger = getattr(kwargs['agent'], 'logger')
                elif 'browser_session' in kwargs:
                    logger = getattr(kwargs['browser_session'], 'logger')
                else:
                    logger = logging.getLogger(__name__)
                logger.debug(f'⏳ {additional_text.strip("-")}() took {execution_time:.2f}s')
            return result

        return wrapper

    return decorator


__all__ = [
    'extract_basemodel_generic_arg',
    'time_execution',
]
