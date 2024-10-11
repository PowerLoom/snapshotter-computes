import asyncio

from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper

from computes.trade_volume import TradeVolumeProcessor
from computes.utils.models.message_models import UniswapTradesSnapshot


async def test_trade_volume_processor():
    # Mock your parameters
    from_block = 19635752
    to_block = from_block + 9
    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0x99132b53aB44694eeB372E87bceD3929e4ab8456',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = TradeVolumeProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    trade_volume_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    from pprint import pprint
    pprint(trade_volume_snapshot.dict())

    assert isinstance(trade_volume_snapshot, UniswapTradesSnapshot)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_trade_volume_processor())
