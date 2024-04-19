import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from ..redis_keys import lido_contract_cached_block_height_shares
from .constants import lido_contract_abi
from .constants import lido_contract_object
from .constants import SECONDS_IN_YEAR
from .helpers import get_last_token_rebase
from .models.data_models import EthSharesData

core_logger = logger.bind(module='PowerLoom|StakingYieldSnapshots|Core')


async def get_lido_staking_yield(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    fetch_timestamp=True,
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

    # create dictionary of ABI {function_name -> {signature, abi, input, output}}
    lido_abi_dict = get_contract_abi_dict(lido_contract_abi)
    lido_address = lido_contract_object.address

    shares_array = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=lido_abi_dict,
        function_name='getTotalShares',
        contract_address=lido_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    eth_array = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=lido_abi_dict,
        function_name='getTotalPooledEther',
        contract_address=lido_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

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

    # pre_share_rate = last_shares_data.preTotalEther * 1e27 / last_shares_data.preTotalShares
    # post_share_rate = last_shares_data.postTotalEther * 1e27 / last_shares_data.postTotalShares

    # prev_apr = SECONDS_IN_YEAR * (
    #     (post_share_rate - pre_share_rate) / pre_share_rate
    # ) / 86400

    for index, block_num in enumerate(range(from_block, to_block + 1)):
        block_details = block_details_dict.get(block_num, {})
        block_timestamp = block_details.get('timestamp', None)
        block_shares = shares_array[index][0]
        block_eth = eth_array[index][0]

        time_elapsed = block_timestamp - last_shares_data.lastTimestamp

        pre_share_rate = last_shares_data.postTotalEther * 1e27 / last_shares_data.postTotalShares
        post_share_rate = block_eth * 1e27 / block_shares

        annual_rate_change = SECONDS_IN_YEAR * (
            (post_share_rate - pre_share_rate) / pre_share_rate
        ) / time_elapsed

        last_shares_data.lastTimestamp = block_timestamp
        last_shares_data.postTotalShares = block_shares
        last_shares_data.postTotalEther = block_eth
        prev_apr = annual_rate_change
