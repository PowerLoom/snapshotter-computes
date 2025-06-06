"""
This file makes the fixtures defined in the shared `tests/shared_fixtures` directory
available to all tests within the `computes/tests` directory.

Pytest discovers `conftest.py` files in parent directories, but `tests/shared_fixtures`
is a sibling directory, not a parent. Therefore, we use this local conftest
to explicitly load the fixtures via the `pytest_plugins` mechanism.
"""

pytest_plugins = "tests.shared_fixtures.conftest" 