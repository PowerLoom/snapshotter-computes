import os
from web3 import Web3
import asyncio
import sys

from ..utils.constants import erc20_abi
from ..total_value_locked import _load_abi, calculate_reserves
from ..utils.helpers import get_pair_metadata
from ..redis_keys import uniswap_cached_tick_data_block_height
from snapshotter.settings.config import settings
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.rpc import RpcHelper


async def test_tick_cache():
    # Mock your parameters
    pair_address = Web3.to_checksum_address(
        "0x7858E59e0C01EA06Df3aF3D20aC7B0003275D4Bf"
    )
    from_block = 18766112
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool
    # await redis_conn.flushdb()
    pair_per_token_metadata = await get_pair_metadata(
        pair_address=pair_address, redis_conn=redis_conn, rpc_helper=rpc_helper
    )  # Replace with your data

    # Call your async function
    # Cache data from 30 blocks ago
    reserves = await calculate_reserves(
        pair_address, from_block - 30, pair_per_token_metadata, rpc_helper, redis_conn
    )

    # Check that it returns an array of correct form
    assert isinstance(reserves, list), "Should return a list"
    assert len(reserves) == 2, "Should have two elements"

    cached_tick_dict = await redis_conn.zrangebyscore(
        name=uniswap_cached_tick_data_block_height.format(
                Web3.to_checksum_address(pair_address),
        ),
        min=int(from_block - 30),
        max=int(from_block - 30),
    )

    assert(cached_tick_dict), "Failed to cache ticks"

    # Cache data for from block, should also delete previous data
    reserves = await calculate_reserves(
        pair_address, from_block, pair_per_token_metadata, rpc_helper, redis_conn
    )

    cached_tick_dict = await redis_conn.zrangebyscore(
        name=uniswap_cached_tick_data_block_height.format(
                Web3.to_checksum_address(pair_address),
        ),
        min=int(from_block),
        max=int(from_block),
    )

    assert(cached_tick_dict), "Failed to cache ticks"

    # try to retrieve delete data
    cached_tick_dict = await redis_conn.zrangebyscore(
        name=uniswap_cached_tick_data_block_height.format(
                Web3.to_checksum_address(pair_address),
        ),
        min=int(from_block - 30),
        max=int(from_block - 30),
    )

    assert(not cached_tick_dict), "Stale data was not deleted"

    print("PASSED")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_tick_cache())
