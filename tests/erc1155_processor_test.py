import asyncio

from snapshotter.modules.computes.erc1155_tracking import ERC1155TransferProcessor
from snapshotter.modules.computes.utils.models.message_models import NftTransfersSnapshot
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper


async def test_erc1155_processor():
    from_block = 19677311
    to_block = from_block + 9

    snapshot_process_message = PowerloomSnapshotProcessMessage(
        data_source='0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401',
        begin=from_block,
        end=to_block,
        epochId=1,
    )

    processor = ERC1155TransferProcessor()
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    snapshot = await processor.compute(
        epoch=snapshot_process_message,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    assert isinstance(snapshot, NftTransfersSnapshot)

    from pprint import pprint
    pprint(snapshot.dict())

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_erc1155_processor())
