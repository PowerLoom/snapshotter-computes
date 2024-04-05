from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

from .models.data_models import BlockDetails

core_logger = logger.bind(module='PowerLoom|BaseSnapshots|Core')


async def get_block_details(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):
    """
    Fetches block details from the chain.
    """
    block_details = await get_block_details_in_block_range(
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    block_details = BlockDetails(
        details=block_details,
    )

    return block_details