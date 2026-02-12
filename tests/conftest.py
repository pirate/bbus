import os

import pytest


@pytest.fixture(autouse=True)
def set_log_level():
    os.environ['BUBUS_LOGGING_LEVEL'] = 'WARNING'
    import bubus  # noqa
