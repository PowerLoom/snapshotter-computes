import asyncio
import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

core_logger = logger.bind(module='PowerLoom|BaseSnapshots|Core')

