import asyncio
import functools
import json
from decimal import Decimal
from decimal import getcontext
from typing import Optional, Union

from eth_typing import Address
from eth_typing.evm import Address
from eth_typing.evm import ChecksumAddress
from computes.utils.models.message_models import UniswapPoolMetadata
from snapshotter.utils.default_logger import logger
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper, get_contract_abi_dict
from web3 import Web3

from computes.utils.constants import helper_contract
from computes.utils.constants import MAX_TICK
from computes.utils.constants import MIN_TICK
from computes.utils.constants import pair_contract_abi

AddressLike = Union[Address, ChecksumAddress]
getcontext().prec = 36
tvl_logger = logger.bind(module='PowerLoom|UniswapTotalValueLocked')



def transform_tick_bytes_to_list(tick_bytes):
    """
    Transform tick data from decoded web3 call result to a list of dictionaries.

    Args:
        decoded_data: Decoded tick data from web3 call.

    Returns:
        list: A list of dictionaries containing liquidity_net and idx for each tick.
    """
    if len(tick_bytes) == 0:
        return []

    ticks = [
        {
            'liquidity_net': int.from_bytes(i[:-3], 'big', signed=True),
            'idx': int.from_bytes(i[-3:], 'big', signed=True),
        }
        for i in tick_bytes
    ]

    return ticks


def calculate_tvl_from_ticks(ticks, pair_metadata: UniswapPoolMetadata, sqrt_price):
    """
    Calculate the Total Value Locked (TVL) from tick data.

    Args:
        ticks (list): List of tick data.
        pair_metadata (dict): Metadata for the token pair.
        sqrt_price (int): Square root of the current price.

    Returns:
        tuple: A tuple containing the liquidity of token0 and token1.
    """
    sqrt_price = Decimal(sqrt_price) / Decimal(2 ** 96)

    liquidity_total = Decimal(0)
    token0_liquidity = Decimal(0)
    token1_liquidity = Decimal(0)
    tick_spacing = 1

    if len(ticks) == 0:
        return (0, 0)

    int_fee = int(pair_metadata.fee)

    # Set tick spacing based on fee
    if int_fee == 3000:
        tick_spacing = Decimal(60)
    elif int_fee == 500:
        tick_spacing = Decimal(10)
    elif int_fee == 10000:
        tick_spacing = Decimal(200)

    # https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf

    for i in range(len(ticks)):
        tick = ticks[i]
        idx = Decimal(tick['idx'])
        nextIdx = Decimal(ticks[i + 1]['idx']) \
            if i < len(ticks) - 1 \
            else idx + tick_spacing

        liquidity_net = Decimal(tick['liquidity_net'])
        liquidity_total += liquidity_net
        sqrtPriceLow = Decimal(1.0001) ** (idx / 2)
        sqrtPriceHigh = Decimal(1.0001) ** (nextIdx / 2)

        if sqrt_price <= sqrtPriceLow:
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
        elif sqrt_price >= sqrtPriceHigh:
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
        else:
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrt_price,
                sqrtPriceHigh,
            )
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrt_price,
            )

    return (int(token0_liquidity), int(token1_liquidity))


def get_token0_in_pool(
    liquidity: Decimal,
    sqrtPriceLow: Decimal,
    sqrtPriceHigh: Decimal,
) -> int:
    """
    Calculate the amount of token0 in the pool for a given price range.

    Args:
        liquidity (Decimal): The liquidity in the pool.
        sqrtPriceLow (Decimal): The square root of the lower price bound.
        sqrtPriceHigh (Decimal): The square root of the upper price bound.

    Returns:
        int: The amount of token0 in the pool.
    """
    result = liquidity * (sqrtPriceHigh - sqrtPriceLow) / (sqrtPriceLow * sqrtPriceHigh)
    return int(result)


def get_token1_in_pool(
    liquidity: Decimal,
    sqrtPriceLow: Decimal,
    sqrtPriceHigh: Decimal,
) -> int:
    """
    Calculate the amount of token1 in the pool for a given price range.

    Args:
        liquidity (Decimal): The liquidity in the pool.
        sqrtPriceLow (Decimal): The square root of the lower price bound.
        sqrtPriceHigh (Decimal): The square root of the upper price bound.

    Returns:
        int: The amount of token1 in the pool.
    """
    result = liquidity * (sqrtPriceHigh - sqrtPriceLow)
    return int(result)


def _load_abi(path: str) -> str:
    """
    Load ABI from a JSON file.

    Args:
        path (str): The path to the JSON file containing the ABI.

    Returns:
        str: The loaded ABI as a string.
    """
    with open(path) as f:
        abi: str = json.load(f)
    return abi


async def calculate_reserves(
    pair_address: str,
    from_block: int,
    to_block: int,
    pair_per_token_metadata: Optional[UniswapPoolMetadata],
    rpc_helper: RpcHelper,
):
    """
    Calculate reserves for a given pair address.
    """
    if not pair_per_token_metadata:
        return [0, 0]
    tvl_logger.debug(
        "[Epoch {}] Pool {} | Calculating token0 and token1 reserves",
        from_block,
        pair_address
    )
    ticks_list, slot0 = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        pair_per_token_metadata=pair_per_token_metadata,
    )
    if not ticks_list or not slot0:
        return [0, 0]
    sqrt_price = slot0[0]

    t0_reserves, t1_reserves = calculate_tvl_from_ticks(
        ticks_list,
        pair_per_token_metadata,
        sqrt_price,
    )

    return [int(t0_reserves), int(t1_reserves)]


async def get_tick_info(
    rpc_helper: RpcHelper,
    pair_address: str,
    from_block: int,
    to_block: int,
    pair_per_token_metadata: UniswapPoolMetadata,
):
    """Gets tick data and slot0 info for a given block range."""
    tvl_logger.debug(
        "[Epoch {}] Pool {} | Fetching tick information",
        from_block,
        pair_address
    )
    try:
        # Prepare tick_tasks as before
        fee = int(pair_per_token_metadata.fee)
        step = (MAX_TICK - MIN_TICK) // 16
        if fee == 500:
            step = (MAX_TICK - MIN_TICK) // 4
        elif fee == 3000:
            step = MAX_TICK - MIN_TICK // 2
        elif fee == 10000:
            step = MAX_TICK - MIN_TICK
        tick_tasks = []
        for idx in range(MIN_TICK, MAX_TICK + 1, step):
            tick_tasks.append(
                ('getTicks', [pair_address, idx, min(idx + step - 1, MAX_TICK)]),
            )

        # Execute RPC calls in parallel
        tickDataResponse, slot0ResponseList = await asyncio.gather(
            rpc_helper.web3_call(
                tasks=tick_tasks, 
                contract_addr=helper_contract.address,
                abi=helper_contract.abi,
                tasks_block_override=[from_block for _ in range(len(tick_tasks))],
            ),
            rpc_helper.batch_eth_call_on_block_range(
                abi_dict=get_contract_abi_dict(abi=pair_contract_abi),
                function_name='slot0',
                contract_address=pair_address,
                from_block=from_block,
                to_block=to_block,
                params=[],
            ),
            return_exceptions=True
        )

        # --- Handle potential errors from gather --- 
        if isinstance(tickDataResponse, Exception):
            tvl_logger.error(f"Error fetching tick data for {pair_address}: {tickDataResponse}")
            tickDataResponse = None

        if isinstance(slot0ResponseList, Exception):
            tvl_logger.error(f"Error fetching slot0 data for {pair_address}: {slot0ResponseList}")
            slot0ResponseList = None
        
        if tickDataResponse is None or slot0ResponseList is None:
             tvl_logger.error(f"Failed to gather all required data for {pair_address} between {from_block}-{to_block}")
             return None, None

        if not slot0ResponseList:
            tvl_logger.error(f"Batch call for slot0 returned empty list for {pair_address} between {from_block}-{to_block}")
            return None, None
        
        # Extract the slot0 result corresponding to the 'from_block'
        slot0_at_from_block = slot0ResponseList[0] 

        # Process tickDataResponse using the original logic
        ticks_list = []
        for ticks in tickDataResponse:
            ticks_list.append(transform_tick_bytes_to_list(ticks))
        
        # Flatten the list of lists if necessary
        if ticks_list:
             ticks_list = functools.reduce(lambda x, y: x + y, ticks_list)
        else:
             ticks_list = [] # Ensure it's an empty list if reduce fails on empty input

        tvl_logger.info(
            'Fetched tick and slot0 data for pool {} in range {}-{}',
            pair_address, from_block, to_block
        )
        # Return ticks list and the slot0 data for the *from_block*
        return ticks_list, slot0_at_from_block

    except Exception as e:
        tvl_logger.opt(exception=True).error(
            'Error in get_tick_info for pool {} | range {}-{}: {}',
            pair_address, from_block, to_block, e
        )
        return None, None
