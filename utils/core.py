from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

from .models.data_models import BlockDetails
from .models.data_models import EthPriceDict

# Takes pooler's version of get_eth_price_usd, will be UniswapV2 or UniswapV3
# Snapshots will be tied to the namespace, so future snapshots built on these can specify
from snapshotter.utils.snapshot_utils import get_eth_price_usd

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


async def get_eth_price_dict(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):
    """
    Fetches eth price in USD from the chain.
    """

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
                'Error attempting to get block details of block-range'
                ' {}-{}: {}, retrying again'
            ),
            from_block,
            to_block,
            err,
        )
        raise err
    
    eth_usd_prices = await get_eth_price_usd(
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    eth_price_dict = dict()
    for block_num, price in eth_usd_prices.items():
        current_block_details = block_details_dict.get(block_num, None)

        timestamp = (
            current_block_details.get(
                'timestamp',
                None,
            )
            if current_block_details
            else None
        )

        eth_price_dict[block_num] = {
            'blockPrice': price,
            'timestamp': timestamp,
        }

    eth_price_dict = EthPriceDict(
        blockPrices=eth_price_dict,
    )

    return eth_price_dict
