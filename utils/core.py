import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

from ..redis_keys import lido_contract_cached_block_height_shares
from .helpers import get_last_token_rebase
from .models.data_models import EthSharesData

core_logger = logger.bind(module='PowerLoom|StakingYieldSnapshots|Core')


async def get_lido_staking_yield(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):

    # get block details for the range in order to get the timestamps
    try:
        block_details_dict = await get_block_details_in_block_range(
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
    except Exception as err:
        core_logger.opt(exception=True).error(
            (
                'Error attempting to get block details of to_block {}:'
                ' {}, retrying again'
            ),
            to_block,
            err,
        )
        raise err

    # attempt to get the previous epoch shares data, if it exists it will be cached to the to_block
    prev_epoch_shares_data = await redis_conn.hget(
        lido_contract_cached_block_height_shares,
        str(from_block - 1),
    )

    if prev_epoch_shares_data:
        cached_shares_dict = json.loads(prev_epoch_shares_data)
        last_shares_data = EthSharesData(
            **cached_shares_dict,
        )
    else:
        # get the last epoch's shares data and save to cache
        last_shares_data = await get_last_token_rebase(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=from_block,
        )

    for block_num in range(from_block, to_block + 1):
        block_details = block_details_dict.get(block_num, {})
        block_timestamp = block_details.get('timestamp', None)
