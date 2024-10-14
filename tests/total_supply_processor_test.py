import asyncio

from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper

from computes.pool_total_supply import AssetTotalSupplyProcessor
from computes.utils.helpers import get_bulk_asset_data
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot


async def test_total_supply_processor():
    # Mock your parameters
    from_block = 18780760
    to_block = from_block + 9
    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = AssetTotalSupplyProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    # simulate preloader call
    await get_bulk_asset_data(
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
    )

    asset_total_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    assert isinstance(asset_total_snapshot, AavePoolTotalAssetSnapshot)
    assert len(asset_total_snapshot.totalAToken) == (to_block - from_block + 1), 'Should return data for all blocks'
    assert len(asset_total_snapshot.liquidityIndex) == (to_block - from_block + 1), 'Should return data for all blocks'

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_processor())
