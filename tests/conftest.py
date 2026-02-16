import importlib
import os

import pytest


@pytest.fixture(autouse=True)
def set_log_level():
    os.environ['BUBUS_LOGGING_LEVEL'] = 'WARNING'
    importlib.import_module('bubus')
