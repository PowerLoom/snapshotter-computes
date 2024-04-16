import asyncio

from snapshotter.modules.computes.nft_mint import NftMintProcessor
from snapshotter.modules.computes.utils.models.message_models import NftMintSnapshot
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper


async def test_nft_mint_processor():
    from_block = 19663920
    to_block = from_block + 9

    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0xE96d3a6B52993377C476dE24D86871023046787a',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = NftMintProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    mint_data_snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    assert isinstance(mint_data_snapshot, NftMintSnapshot)

    print(mint_data_snapshot)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_nft_mint_processor())
