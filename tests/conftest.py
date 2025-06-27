"""
Configuration file for computes tests.

This file imports and re-exports the shared fixtures from tests/shared_fixtures/conftest.py
to make them available to the computes test suite.
"""

import sys
import os

# Add the project root to sys.path to ensure we can import shared fixtures
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import all fixtures from the shared fixtures
from tests.shared_fixtures.conftest import (
    app_config,
    rpc_helper,
    anchor_rpc_helper,
    redis_conn,
    ipfs_reader,
    w3_instance,
    protocol_state_contract,
    load_abi_fn,
)

# Re-export the fixtures so they're available to computes tests
__all__ = [
    'app_config',
    'rpc_helper',
    'anchor_rpc_helper',
    'redis_conn',
    'ipfs_reader',
    'w3_instance',
    'protocol_state_contract',
    'load_abi_fn',
] 