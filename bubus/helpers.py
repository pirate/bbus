import asyncio
import logging
import traceback
import time
from collections import deque
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

# Define generic type variables for return type and parameters
R = TypeVar('R')
P = ParamSpec('P')
QueueEntryType = TypeVar('QueueEntryType')


class QueueShutDown(Exception):
    """Raised when putting on to or getting from a shut-down Queue."""

    pass


class CleanShutdownQueue(asyncio.Queue[QueueEntryType]):
    """asyncio.Queue subclass that handles shutdown cleanly without warnings."""

    _is_shutdown: bool = False
    _getters: deque[asyncio.Future[QueueEntryType]]
    _putters: deque[asyncio.Future[QueueEntryType]]

    def shutdown(self, immediate: bool = True):
        """Shutdown the queue and clean up all pending futures."""
        del immediate
        self._is_shutdown = True

        # Cancel all waiting getters without triggering warnings
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                # Set exception instead of cancelling to avoid "Event loop is closed" errors
                getter.set_exception(QueueShutDown())

        # Cancel all waiting putters
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_exception(QueueShutDown())

    async def get(self) -> QueueEntryType:
        """Remove and return an item from the queue, with shutdown support."""
        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown

            getter = cast(asyncio.Future[QueueEntryType], asyncio.get_running_loop().create_future())
            assert isinstance(getter, asyncio.Future)
            self._getters.append(getter)
            try:
                await getter
            except:
                # Clean up the getter if we're cancelled
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    self._getters.remove(getter)
                except ValueError:
                    pass
                # Re-raise the exception
                raise

        return self.get_nowait()

    async def put(self, item: QueueEntryType) -> None:
        """Put an item into the queue, with shutdown support."""
        while self.full():
            if self._is_shutdown:
                raise QueueShutDown

            putter = cast(asyncio.Future[QueueEntryType], asyncio.get_running_loop().create_future())
            assert isinstance(putter, asyncio.Future)
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    self._putters.remove(putter)
                except ValueError:
                    pass
                raise

        return self.put_nowait(item)

    def put_nowait(self, item: QueueEntryType) -> None:
        """Put an item into the queue without blocking, with shutdown support."""
        if self._is_shutdown:
            raise QueueShutDown
        return super().put_nowait(item)

    def get_nowait(self) -> QueueEntryType:
        """Remove and return an item if one is immediately available, with shutdown support."""
        if self._is_shutdown and self.empty():
            raise QueueShutDown
        return super().get_nowait()


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


def _log_filtered_traceback(exc: BaseException) -> str:
    """Format traceback while filtering noisy asyncio/stdlib frames."""
    trace_exc = traceback.TracebackException.from_exception(exc, capture_locals=False)

    def _filter(_: traceback.TracebackException):
        trace_exc.stack = traceback.StackSummary.from_list(
            [f for f in trace_exc.stack if 'asyncio/tasks.py' not in f.filename and 'lib/python' not in f.filename]
        )
        if trace_exc.__cause__:
            _filter(trace_exc.__cause__)
        if trace_exc.__context__:
            _filter(trace_exc.__context__)

    _filter(trace_exc)
    return ''.join(trace_exc.format())


__all__ = [
    '_log_filtered_traceback',
    'CleanShutdownQueue',
    'QueueShutDown',
    'extract_basemodel_generic_arg',
    'time_execution',
]
