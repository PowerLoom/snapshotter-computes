import asyncio
import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

from ..redis_keys import lido_contract_cached_block_height_shares
from .constants import lido_contract_object
from .constants import LIDO_EVENTS_ABI
from .constants import LIDO_EVENTS_SIG
from .helpers import calculate_staking_apr
from .helpers import get_last_token_rebase
from .models.data_models import LidoAprData

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

    lido_address = lido_contract_object.address

    event_sig, event_abi = get_event_sig_and_abi(
        LIDO_EVENTS_SIG,
        LIDO_EVENTS_ABI,
    )

    epoch_rebase_events = await rpc_helper.get_events_logs(
        **{
            'contract_address': lido_address,
            'to_block': to_block,
            'from_block': from_block,
            'topics': [event_sig],
            'event_abi': event_abi,
            'redis_conn': redis_conn,
        },
    )

    block_rebase_events = {block_num: {} for block_num in range(from_block, to_block + 1)}
    for event in epoch_rebase_events:
        block_rebase_events[event['blockNumber']] = LidoAprData(
            **event['args'],
        )

    # attempt to get the previous epoch apr data, if it exists it will be cached to the to_block
    prev_apr_data_cache = await redis_conn.hget(
        lido_contract_cached_block_height_shares,
        from_block - 1,
    )

    if prev_apr_data_cache:
        cached_apr_dict = json.loads(prev_apr_data_cache)
        last_apr_data = LidoAprData(
            **cached_apr_dict,
        )
    else:
        # get the last emitted TokenRebased event before the from_block
        last_apr_data = await get_last_token_rebase(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=from_block,
        )

    apr_data_dict = {}

    for block_num in range(from_block, to_block + 1):
        block_details = block_details_dict.get(block_num, {})
        block_timestamp = block_details.get('timestamp', None)

        if block_rebase_events.get(block_num, {}):
            last_apr_data = block_rebase_events[block_num]
            block_apr = calculate_staking_apr(
                rebase_data=last_apr_data,
            )
            last_apr_data.stakingApr = block_apr

        apr_data_dict[block_num] = {
            'reportTimestamp': last_apr_data.reportTimestamp,
            'timestamp': block_timestamp,
            'apr': last_apr_data.stakingApr,
        }

    # cache the last apr data for the next epoch
    await asyncio.gather(
        redis_conn.hset(
            lido_contract_cached_block_height_shares,
            to_block,
            json.dumps(last_apr_data.dict()),
        ),
        redis_conn.hdel(
            lido_contract_cached_block_height_shares,
            from_block - 1,
        ),
    )

    return apr_data_dict
