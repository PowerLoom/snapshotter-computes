import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import lido_contract_cached_block_height_shares
from .constants import current_node
from .constants import lido_contract_object
from .constants import LIDO_EVENTS_ABI
from .constants import LIDO_EVENTS_SIG
from .constants import SECONDS_IN_YEAR
from .models.data_models import LidoAprData

helper_logger = logger.bind(module='PowerLoom|StakingYieldSnapshots|Helpers')


def calculate_staking_apr(
    shares_data: LidoAprData,
):
    pre_share_rate = shares_data.preTotalEther * 1e27 / shares_data.preTotalShares
    post_share_rate = shares_data.postTotalEther * 1e27 / shares_data.postTotalShares

    apr = SECONDS_IN_YEAR * (
        (post_share_rate - pre_share_rate) / pre_share_rate
    ) / shares_data.timeElapsed

    return apr


async def get_last_token_rebase(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
):
    try:

        lido_address = lido_contract_object.address

        event_sig, event_abi = get_event_sig_and_abi(
            LIDO_EVENTS_SIG,
            LIDO_EVENTS_ABI,
        )

        # TokenRebased Event is emitted once every 24 hours when the protocol allocates rewards
        # Setting step to 1000 to reduce the number of calls since the last event
        # could be as far as 7200 blocks
        step = 1000

        end_block = from_block
        start_block = end_block - step

        last_rebase_events = []

        while not last_rebase_events:
            last_rebase_events = await rpc_helper.get_events_logs(
                **{
                    'contract_address': lido_address,
                    'to_block': end_block,
                    'from_block': start_block,
                    'topics': [event_sig],
                    'event_abi': event_abi,
                    'redis_conn': redis_conn,
                },
            )

            if not last_rebase_events:
                start_block -= step
                end_block -= step

        last_event = last_rebase_events[-1]

        shares_data = LidoAprData(
            timeElapsed=last_event['args']['timeElapsed'],
            preTotalShares=last_event['args']['preTotalShares'],
            preTotalEther=last_event['args']['preTotalEther'],
            postTotalShares=last_event['args']['postTotalShares'],
            postTotalEther=last_event['args']['postTotalEther'],
        )

        last_apr = calculate_staking_apr(
            shares_data=shares_data,
        )

        shares_data.stakingApr = last_apr

        return shares_data

    except Exception as e:
        helper_logger.error(f'Error in get_last_token_rebase: {e}')
        raise e
