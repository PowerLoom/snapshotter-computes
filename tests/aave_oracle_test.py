import asyncio

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from computes.utils.helpers import get_asset_metadata
from computes.utils.pricing import get_asset_price_in_block_range


async def test_aave_oracle_pricing():
    # Mock your parameters
    from_block = 18780760
    to_block = from_block + 9
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    asset_address = Web3.to_checksum_address('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address, redis_conn=redis_conn, rpc_helper=rpc_helper,
    )

    asset_price_dict = await get_asset_price_in_block_range(
        asset_metadata=asset_metadata,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        debug_log=False,
    )

    assert isinstance(asset_price_dict, dict), 'Should return a dict'
    assert len(asset_price_dict) == (to_block - from_block + 1), 'Should return data for all blocks'

    for price in asset_price_dict.values():
        assert type(price) is float, 'Wrong data type in asset dict'
        assert price > 0, 'price cannot be negative'

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_aave_oracle_pricing())
