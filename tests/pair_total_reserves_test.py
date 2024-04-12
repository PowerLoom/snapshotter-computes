import asyncio

from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper

from snapshotter.modules.computes.pair_total_reserves import PairTotalReservesProcessor
from snapshotter.modules.computes.utils.models.message_models import UniswapPairTotalReservesSnapshot


async def test_pair_reserves_processor():
    # Mock your parameters
    from_block = 19634365
    to_block = from_block + 9
    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0x277667eb3e34f134adf870be9550e9f323d0dc24',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = PairTotalReservesProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    pair_reserves_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    from pprint import pprint

    pprint(pair_reserves_snapshot)

    assert isinstance(pair_reserves_snapshot, UniswapPairTotalReservesSnapshot)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_pair_reserves_processor())
