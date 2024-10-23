import asyncio
import functools
import json
from decimal import Decimal
from decimal import getcontext
from typing import Union
from typing import List
from typing import Dict

from eth_abi import decode
from eth_typing import Address
from eth_typing.evm import Address
from eth_typing.evm import ChecksumAddress
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper

from snapshotter.modules.computes.utils.constants import helper_contract
from snapshotter.modules.computes.utils.constants import MAX_TICK
from snapshotter.modules.computes.utils.constants import MIN_TICK
from snapshotter.modules.computes.utils.constants import override_address
from snapshotter.modules.computes.utils.constants import pair_contract_abi
from snapshotter.modules.computes.utils.constants import UNISWAP_EVENTS_ABI
from snapshotter.modules.computes.utils.constants import UNISWAP_TRADE_EVENT_SIGS
from snapshotter.modules.computes.utils.constants import univ3_helper_bytecode

AddressLike = Union[Address, ChecksumAddress]
getcontext().prec = 36
tvl_logger = logger.bind(module='PowerLoom|UniswapTotalValueLocked')


def transform_tick_bytes_to_list(tickData: bytes) -> List[Dict[str, int]]:
    if not tickData:
        return []

    bytes_arr = decode(['bytes[]'], tickData)[0]
    
    return [
        {
            'liquidity_net': int.from_bytes(tick[:16], 'big', signed=True),
            'idx': int.from_bytes(tick[16:], 'big', signed=True),
        }
        for tick in bytes_arr
    ]


def calculate_tvl_from_ticks(ticks, pair_metadata, sqrt_price):
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

    int_fee = int(pair_metadata['pair']['fee'])

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
    liquidity: int,
    sqrtPriceLow: int,
    sqrtPriceHigh: int,
) -> int:
    """
    Calculate the amount of token0 in the pool for a given price range.

    Args:
        liquidity (int): The liquidity in the pool.
        sqrtPriceLow (int): The square root of the lower price bound.
        sqrtPriceHigh (int): The square root of the upper price bound.

    Returns:
        int: The amount of token0 in the pool.
    """
    return liquidity * (sqrtPriceHigh - sqrtPriceLow) / (sqrtPriceLow * sqrtPriceHigh) // 1


def get_token1_in_pool(
    liquidity: int,
    sqrtPriceLow: int,
    sqrtPriceHigh: int,
) -> int:
    """
    Calculate the amount of token1 in the pool for a given price range.

    Args:
        liquidity (int): The liquidity in the pool.
        sqrtPriceLow (int): The square root of the lower price bound.
        sqrtPriceHigh (int): The square root of the upper price bound.

    Returns:
        int: The amount of token1 in the pool.
    """
    return liquidity * (sqrtPriceHigh - sqrtPriceLow) // 1


async def get_events(
    pair_address: str,
    rpc: RpcHelper,
    from_block,
    to_block,
):
    """
    Fetch events for a given pair address within a block range.

    Args:
        pair_address (str): The address of the token pair.
        rpc (RpcHelper): An instance of RpcHelper for making RPC calls.
        from_block: The starting block number.
        to_block: The ending block number.

    Returns:
        list: A list of events for the specified pair and block range.
    """
    event_sig, event_abi = get_event_sig_and_abi(
        UNISWAP_TRADE_EVENT_SIGS,
        UNISWAP_EVENTS_ABI,
    )

    events = await rpc.get_events_logs(
        contract_address=pair_address,
        to_block=to_block,
        from_block=from_block,
        topics=[event_sig],
        event_abi=event_abi,
    )

    return events


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
    from_block,
    pair_per_token_metadata,
    rpc_helper: RpcHelper,
):
    """
    Calculate the reserves for a given pair address.

    Args:
        pair_address (str): The address of the token pair.
        from_block: The block number to calculate reserves from.
        pair_per_token_metadata (dict): Metadata for the token pair.
        rpc_helper (RpcHelper): An instance of RpcHelper for making RPC calls.

    Returns:
        list: A list containing the reserves of token0 and token1.
    """
    ticks_list, slot0 = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        pair_per_token_metadata=pair_per_token_metadata,
    )

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
        from_block,
        pair_per_token_metadata,
):
    """
    Fetch tick information for a given pair address.

    Args:
        rpc_helper (RpcHelper): An instance of RpcHelper for making RPC calls.
        pair_address (str): The address of the token pair.
        from_block: The block number to fetch tick info from.
        pair_per_token_metadata (dict): Metadata for the token pair.

    Returns:
        tuple: A tuple containing the list of ticks and slot0 data.

    Raises:
        Exception: If there's an error fetching tick data.
    """
    try:
        overrides = {
            override_address: {'code': univ3_helper_bytecode},
        }
        current_node = rpc_helper.get_current_node()

        # Determine step size based on fee
        fee = int(pair_per_token_metadata['pair']['fee'])
        step = (MAX_TICK - MIN_TICK) // 16

        if fee == 500:
            step = (MAX_TICK - MIN_TICK) // 4
        elif fee == 3000:
            step = MAX_TICK - MIN_TICK // 2
        elif fee == 10000:
            step = MAX_TICK - MIN_TICK

        tick_tasks = []

        # TODO: use rpc_helper batch_web3_call
        # getTicks() is inclusive for start and end ticks
        for idx in range(MIN_TICK, MAX_TICK + 1, step):
            tick_tasks.append(
                helper_contract.functions.getTicks(
                    pair_address, idx, min(idx + step - 1, MAX_TICK),
                ),
            )

        pair_contract = current_node['web3_client'].eth.contract(
            address=pair_address,
            abi=pair_contract_abi,
        )

        slot0_tasks = [
            pair_contract.functions.slot0(),
        ]

        # Execute RPC calls
        tickDataResponse, slot0Response = await asyncio.gather(
            rpc_helper.web3_call(
                tasks=tick_tasks,
                block=from_block,
                overrides=overrides,
            ),
            rpc_helper.web3_call(
                tasks=slot0_tasks,
                block=from_block,
            ),
        )

        # Process tick data
        ticks_list = []
        for ticks in tickDataResponse:
            ticks_list.append(transform_tick_bytes_to_list(ticks))

        ticks_list = functools.reduce(lambda x, y: x + y, ticks_list)

        slot0 = slot0Response[0]

        return ticks_list, slot0
    except Exception as err:
        tvl_logger.warning(
            'Failed to get tick data for pair {} at block {} with error {}',
            pair_address, from_block, err,
        )
        raise err

