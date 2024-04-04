import json
from asyncio import gather

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_cached_block_height_asset_price
from ..redis_keys import aave_cached_block_height_assets_prices
from ..redis_keys import aave_pool_asset_set_data
from ..settings.config import settings as worker_settings
from .constants import aave_oracle_abi
from .constants import pool_contract_obj

pricing_logger = logger.bind(module='PowerLoom|BaseSnapshots|Pricing')

