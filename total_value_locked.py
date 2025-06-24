import asyncio
import functools
import json
from decimal import Decimal
from decimal import getcontext
from typing import Optional, Union, List, Tuple, Dict

from eth_typing import Address
from eth_typing.evm import Address
from eth_typing.evm import ChecksumAddress
from computes.utils.models.message_models import UniswapPoolMetadata
from computes.utils.models.data_models import TickData, Slot0Data
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper, get_contract_abi_dict

from computes.utils import constants

AddressLike = Union[Address, ChecksumAddress]
getcontext().prec = 36
tvl_logger = logger.bind(module='PowerLoom|UniswapTotalValueLocked')

class Slot0DataError(Exception):
    """Custom exception for errors during slot0 data fetching or processing."""
    pass


def transform_tick_bytes_to_list(tick_bytes) -> List[TickData]:
    """
    Transform tick data from decoded web3 call result to a list of TickData objects.

    Args:
        tick_bytes: Decoded bytes for a single tick range call result.

    Returns:
        list: A list of TickData objects.
    """
    if len(tick_bytes) == 0:
        return []

    ticks = [
        TickData(
            liquidity_net=int.from_bytes(i[:-3], 'big', signed=True),
            idx=int.from_bytes(i[-3:], 'big', signed=True),
        )
        for i in tick_bytes
    ]

    return ticks


def calculate_tvl_from_ticks(ticks: List[TickData], pair_metadata: UniswapPoolMetadata, sqrt_price: int) -> Tuple[int, int]:
    """
    Calculate the Total Value Locked (TVL) from tick data.

    Args:
        ticks (List[TickData]): List of tick data objects.
        pair_metadata (UniswapPoolMetadata): Metadata for the token pair.
        sqrt_price (int): Square root of the current price (uint160 from slot0).

    Returns:
        tuple: A tuple containing the liquidity of token0 and token1 as integers.
    """
    sqrt_price = Decimal(sqrt_price) / Decimal(2 ** 96)

    liquidity_total = Decimal(0)
    token0_liquidity = Decimal(0)
    token1_liquidity = Decimal(0)
    tick_spacing = Decimal(1)

    if not ticks:
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
        idx = Decimal(tick.idx)
        nextIdx = Decimal(ticks[i + 1].idx) if i < len(ticks) - 1 else idx + tick_spacing

        liquidity_net = Decimal(tick.liquidity_net)
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
    at_block: int,
    pair_per_token_metadata: Optional[UniswapPoolMetadata],
    rpc_helper: RpcHelper,
) -> Tuple[int, int]:
    """
    Calculate reserves for a given pair address based on the state at at_block.
    """
    if not pair_per_token_metadata:
        return (0, 0)
    tvl_logger.debug(
        "[Epoch starting at {}] Pool {} | Calculating token0 and token1 reserves based on state at block {}",
        at_block,
        pair_address,
        at_block,
    )
    # REFACTOR: clarify between tick data and slot0 data fetch. possibly use separate helper functions for each
    ticks_list = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        at_block=at_block,
        pair_per_token_metadata=pair_per_token_metadata,
    )
    
    if ticks_list is None:
        tvl_logger.warning(f"Could not get required tick info for {pair_address} at block {at_block}")
        return (0, 0)
    
    # get slot data at the at_block
    slot0_data_dict_at_block = await get_slot0_data_for_block_range(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=at_block,
        to_block=at_block,
    )
    if slot0_data_dict_at_block is None:
        tvl_logger.warning(f"Could not get required slot0 data for {pair_address} at block {at_block}")
        return (0, 0)
    sqrt_price = slot0_data_dict_at_block[at_block].sqrtPriceX96
    
    t0_reserves, t1_reserves = calculate_tvl_from_ticks(
        ticks_list,
        pair_per_token_metadata,
        sqrt_price,
    )

    return (int(t0_reserves), int(t1_reserves))


async def get_slot0_data_for_block_range(
    rpc_helper: RpcHelper,
    pair_address: str,
    from_block: int,
    to_block: int
) -> Dict[int, Slot0Data]:
    """
    Fetches, validates, and processes slot0 data for a given pair address over a block range.

    Returns:
        A dictionary mapping block numbers to Slot0Data objects.
    Raises:
        Slot0DataError: If there's an issue fetching, validating, or parsing slot0 data.
    """
    tvl_logger.debug(
        "[Range {}-{}] Pool {} | Fetching slot0 data",
        from_block, to_block, pair_address
    )

    if from_block > to_block:
        tvl_logger.debug(
            f"Invalid or empty block range ({from_block}-{to_block}) for slot0 data for {pair_address}, "
            f"returning empty dict."
        )
        return {}

    slot0ResponseListRaw = None
    try:
        slot0ResponseListRaw = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=get_contract_abi_dict(abi=constants.pair_contract_abi),
            function_name='slot0',
            contract_address=pair_address,
            from_block=from_block,
            to_block=to_block,
            params=[],
        )
    except Exception as e:
        msg = (
            f"Exception during batch_eth_call_on_block_range for slot0 data for {pair_address} "
            f"range {from_block}-{to_block}: {e}"
        )
        tvl_logger.error(msg)
        raise Slot0DataError(msg) from e

    if slot0ResponseListRaw is None:
        msg = f"Batch call for slot0 returned None for {pair_address} range {from_block}-{to_block}"
        tvl_logger.error(msg)
        raise Slot0DataError(msg)

    if not isinstance(slot0ResponseListRaw, list):
        msg = (
            f"Batch call for slot0 did not return a list for {pair_address} range {from_block}-{to_block}. "
            f"Got: {type(slot0ResponseListRaw)}"
        )
        tvl_logger.error(msg)
        raise Slot0DataError(msg)
        
    slot0_data_dict: Dict[int, Slot0Data] = {}
    expected_len = to_block - from_block + 1

    if len(slot0ResponseListRaw) != expected_len:
        msg = (
            f"Slot0 response list length ({len(slot0ResponseListRaw)}) does not match expected "
            f"block range length ({expected_len}) for {pair_address} range {from_block}-{to_block}. "
            f"Expected complete data."
        )
        tvl_logger.error(msg)
        raise Slot0DataError(msg)
         
    for i in range(expected_len):
        block_num = from_block + i
        slot0_tuple = slot0ResponseListRaw[i]
        try:
            # Field names must match the order in Slot0Data model and the tuple from eth_abi.decode
            field_names = [
                "sqrtPriceX96", 
                "tick", 
                "observationIndex", 
                "observationCardinality", 
                "observationCardinalityNext", 
                "feeProtocol", 
                "unlocked"
            ]
            if len(slot0_tuple) != len(field_names):
                raise ValueError(
                    f"Tuple length {len(slot0_tuple)} does not match expected number of fields {len(field_names)}"
                )
            
            data_dict = dict(zip(field_names, slot0_tuple))
            slot0_data_obj = Slot0Data(**data_dict)
            slot0_data_dict[block_num] = slot0_data_obj
        except Exception as e_slot0_parse:
            msg = (
                f"Failed to parse slot0 tuple {slot0_tuple} for {pair_address} at block {block_num} "
                f"(index {i}): {e_slot0_parse}."
            )
            tvl_logger.error(msg)
            raise Slot0DataError(msg) from e_slot0_parse
    
    tvl_logger.info(
        'Processed slot0 data ({}) for pool {} range {}-{}',
        len(slot0_data_dict), pair_address, from_block, to_block
    )
    return slot0_data_dict


async def get_tick_info(
    rpc_helper: RpcHelper,
    pair_address: str,
    at_block: int,
    pair_per_token_metadata: UniswapPoolMetadata,
) -> Optional[List[TickData]]:
    """Gets only tick data at a block number"""
    tvl_logger.debug(
        "[Block {}] Pool {} | Fetching tick information",
        at_block, pair_address
    )
    try:
        fee = int(pair_per_token_metadata.fee)
        
        if fee < 500:
            num_segments = 16
        elif fee >= 500 and fee < 3000:
            num_segments = 4
        elif fee >= 3000 and fee < 10000:
            num_segments = 2
        elif fee >= 10000:
            num_segments = 1
        
        tick_tasks = []
        total_range = constants.MAX_TICK - constants.MIN_TICK + 1  # 1774545
        segment_size = total_range // num_segments

        for i in range(num_segments):
            from_tick = constants.MIN_TICK + i * segment_size
            if i == num_segments - 1:  # Last segment goes to MAX_TICK
                to_tick = constants.MAX_TICK
            else:
                to_tick = from_tick + segment_size - 1
            
            tick_tasks.append(('getTicks', [pair_address, from_tick, to_tick]))

        try:
            tickDataResponse = await rpc_helper.web3_call(
                tasks=tick_tasks, 
                contract_addr=constants.helper_contract.address,
                abi=constants.helper_contract.abi,
                tasks_block_override=[at_block for _ in range(len(tick_tasks))],
            )
        except Exception as e:
            tvl_logger.opt(exception=True).error(
                'Unexpected error in get_tick_info for pool {} | block {}: {}',
                pair_address, at_block, e
            )
            return None

        if tickDataResponse is None:
            tvl_logger.error(
                'Failed to gather required data for pool {} | block {}. ',
                pair_address, at_block
            )
            return None

        ticks_list: List[TickData] = []
        temp_ticks_list_of_lists = []
        for ticks_bytes in tickDataResponse:
            if isinstance(ticks_bytes, Exception):
                tvl_logger.warning(f"A batched RPC call for tick data failed: {ticks_bytes}")
                continue
            temp_ticks_list_of_lists.append(transform_tick_bytes_to_list(ticks_bytes))
        
        if temp_ticks_list_of_lists:
             non_empty_tick_lists = [lst for lst in temp_ticks_list_of_lists if lst]
             if non_empty_tick_lists:
                 ticks_list = functools.reduce(lambda x, y: x + y, non_empty_tick_lists)

        tvl_logger.info(
            'Fetched tick data ({}) for pool {} @ block {}',
            len(ticks_list), pair_address, at_block
        )

        return ticks_list

    except Exception as e:
        tvl_logger.opt(exception=True).error(
            'Unexpected error in get_tick_info for pool {} | block {}: {}',
            pair_address, at_block, e
        )
        return None
