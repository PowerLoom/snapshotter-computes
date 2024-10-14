import asyncio

from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper

from computes.pool_supply_volume import AssetSupplyVolumeProcessor
from computes.utils.models.message_models import AaveSupplyVolumeSnapshot


async def test_total_supply_processor():
    # Mock your parameters
    from_block = 19287450  # WETH liquidationCall events
    to_block = from_block + 9
    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = AssetSupplyVolumeProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    asset_volume_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    assert isinstance(asset_volume_snapshot, AaveSupplyVolumeSnapshot)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_processor())
