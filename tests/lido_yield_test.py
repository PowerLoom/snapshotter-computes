import asyncio

from snapshotter.modules.computes.utils.core import get_lido_staking_yield
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper


async def test_lido_staking_yield():
    from_block = 19685711
    to_block = from_block + 9

    snapshot_process_message = PowerloomSnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    await get_lido_staking_yield(
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
    )

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_lido_staking_yield())
