import asyncio

from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper

from snapshotter.modules.computes.eth_price_dict import EthPriceDictProcessor
from snapshotter.modules.computes.utils.models.message_models import EthUsdPriceSnapshot


async def test_eth_price_processor():
    from_block = 19582850
    to_block = from_block + 9

    snapshot_process_message = PowerloomSnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = EthPriceDictProcessor()
    rpc_helper = RpcHelper()  
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    eth_usd_price_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    assert isinstance(eth_usd_price_snapshot, EthUsdPriceSnapshot)
    assert len(eth_usd_price_snapshot.blockEthUsdPrices) == (to_block - from_block + 1), 'Should return data for all blocks'

    print("PASSED")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_eth_price_processor())