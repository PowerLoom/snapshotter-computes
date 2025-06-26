import sys
import os

"""
This file makes the fixtures defined in the shared `tests/shared_fixtures` directory
available to all tests within the `computes/tests` directory.

Pytest discovers `conftest.py` files in parent directories, but `tests/shared_fixtures`
is a sibling directory, not a parent. Therefore, we use this local conftest
to explicitly load the fixtures via the `pytest_plugins` mechanism.
"""

pytest_plugins = "tests.shared_fixtures.conftest" 

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# Import all the shared fixtures from the main testing framework
# We use the pytest fixtures from the shared conftest.py
# These include: app_config, rpc_helper, anchor_rpc_helper, redis_conn, 
# ipfs_reader, w3_instance, protocol_state_contract, load_abi_fn
try:
    from tests.shared_fixtures.conftest import (
        app_config,
        rpc_helper, 
        anchor_rpc_helper,
        redis_conn,
        ipfs_reader,
        w3_instance,
        protocol_state_contract,
        load_abi_fn
    )
except ImportError:
    # Fallback in case the import fails
    pass 