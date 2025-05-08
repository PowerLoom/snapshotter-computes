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

from computes.utils.constants import helper_contract
from computes.utils.constants import MAX_TICK
from computes.utils.constants import MIN_TICK
from computes.utils.constants import pair_contract_abi

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
    from_block: int,
    to_block: int,
    pair_per_token_metadata: Optional[UniswapPoolMetadata],
    rpc_helper: RpcHelper,
) -> Tuple[List[int], Optional[Dict[int, Slot0Data]]]:
    """
    Calculate reserves for a given pair address based on the state at from_block,
    and return the slot0 data for the full range.
    """
    if not pair_per_token_metadata:
        return [0, 0], None
    tvl_logger.debug(
        "[Epoch {}-{}] Pool {} | Calculating token0 and token1 reserves based on state at {}",
        from_block, to_block, 
        pair_address,
        from_block
    )
    
    ticks_list, slot0_data_dict = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        to_block=to_block, 
        pair_per_token_metadata=pair_per_token_metadata,
    )
    
    if ticks_list is None or slot0_data_dict is None:
        tvl_logger.warning(f"Could not get required tick or slot0 info for {pair_address} at block range {from_block}-{to_block}")
        return [0, 0], slot0_data_dict
        
    if not slot0_data_dict:
        tvl_logger.warning(f"slot0_data_dict is empty for {pair_address} at block range {from_block}-{to_block}. Cannot get specific from_block data.")
        return [0, 0], slot0_data_dict

    slot0_data_for_tvl = slot0_data_dict.get(from_block)
    
    sqrt_price = slot0_data_for_tvl.sqrtPriceX96

    t0_reserves, t1_reserves = calculate_tvl_from_ticks(
        ticks_list,
        pair_per_token_metadata,
        sqrt_price,
    )

    return [int(t0_reserves), int(t1_reserves)], slot0_data_dict


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
            abi_dict=get_contract_abi_dict(abi=pair_contract_abi),
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
    from_block: int,
    to_block: int,
    pair_per_token_metadata: UniswapPoolMetadata,
) -> Tuple[Optional[List[TickData]], Optional[Dict[int, Slot0Data]]]:
    """Gets tick data and slot0 info for a given block range."""
    tvl_logger.debug(
        "[Range {}-{}] Pool {} | Fetching tick and slot0 information",
        from_block, to_block, pair_address
    )
    try:
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

        results = await asyncio.gather(
            rpc_helper.web3_call(
                tasks=tick_tasks, 
                contract_addr=helper_contract.address,
                abi=helper_contract.abi,
                tasks_block_override=[from_block for _ in range(len(tick_tasks))],
            ),
            get_slot0_data_for_block_range(
                rpc_helper=rpc_helper,
                pair_address=pair_address,
                from_block=from_block,
                to_block=to_block
            ),
            return_exceptions=True
        )

        tickDataResponse = results[0]
        slot0_data_result = results[1]
        
        processed_slot0_data: Optional[Dict[int, Slot0Data]] = None

        if isinstance(tickDataResponse, Exception):
            tvl_logger.error(f"Error fetching tick data for {pair_address} @ {from_block}: {tickDataResponse}")
            tickDataResponse = None 

        if isinstance(slot0_data_result, Exception):
            tvl_logger.error(
                f"Exception caught from get_slot0_data_for_block_range for {pair_address} "
                f"range {from_block}-{to_block}: {slot0_data_result}"
            )
        else:
            # If no exception from gather, slot0_data_result is the Dict[int, Slot0Data]
            # or could be an empty dict if from_block > to_block
            processed_slot0_data = slot0_data_result 
        
        if tickDataResponse is None or processed_slot0_data is None:
             tvl_logger.error(
                 f"Failed to gather required data for {pair_address} range {from_block}-{to_block}. "
                 f"Tick data success: {tickDataResponse is not None}, Slot0 data success: {processed_slot0_data is not None}"
            )
             return None, None

        ticks_list: List[TickData] = []
        temp_ticks_list_of_lists = []
        for ticks_bytes in tickDataResponse:
             temp_ticks_list_of_lists.append(transform_tick_bytes_to_list(ticks_bytes))
        
        if temp_ticks_list_of_lists:
             non_empty_tick_lists = [lst for lst in temp_ticks_list_of_lists if lst]
             if non_empty_tick_lists:
                 ticks_list = functools.reduce(lambda x, y: x + y, non_empty_tick_lists)

        tvl_logger.info(
            'Fetched tick data ({}) and slot0 data ({}) for pool {} range {}-{}',
            len(ticks_list), len(processed_slot0_data), pair_address, from_block, to_block
        )

        return ticks_list, processed_slot0_data

    except Exception as e:
        tvl_logger.opt(exception=True).error(
            'Unexpected error in get_tick_info for pool {} | range {}-{}: {}',
            pair_address, from_block, to_block, e
        )
        return None, None
