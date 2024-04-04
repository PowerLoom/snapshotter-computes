import asyncio
import json
from decimal import Decimal
from decimal import localcontext
import math
from eth_abi import abi
from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_asset_contract_data
from ..redis_keys import aave_cached_block_height_asset_data
from ..redis_keys import aave_cached_block_height_asset_details
from ..redis_keys import aave_cached_block_height_asset_rate_details
from ..redis_keys import aave_cached_block_height_assets_prices
from ..redis_keys import aave_cached_block_height_core_event_data
from ..redis_keys import aave_pool_asset_set_data
from ..settings.config import settings as worker_settings
from .constants import AAVE_EVENT_SIGS
from .constants import AAVE_EVENTS_ABI
from .constants import current_node
from .constants import erc20_abi
from .constants import HALF_RAY
from .constants import pool_contract_obj
from .constants import RAY
from .constants import SECONDS_IN_YEAR
from .constants import ui_pool_data_provider_contract_obj


helper_logger = logger.bind(module='PowerLoom|BaseSnapshots|Helpers')


def truncate(number, decimals=5):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor