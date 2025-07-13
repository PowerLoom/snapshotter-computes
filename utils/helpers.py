import asyncio
import json
from decimal import Decimal
from decimal import getcontext

from eth_typing import Address
from functools import reduce
from eth_typing.evm import ChecksumAddress

from typing import Optional, Union, List, Tuple, Dict
from redis import asyncio as aioredis
from computes.utils.models.data_models import UniswapEvent
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import get_contract_abi_dict
from rpc_helper.rpc import RpcHelper
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.utils.redis_keys import uniswap_tokens_pair_map
from computes.settings.config import settings as worker_settings
from computes.utils.constants import current_node
from computes.utils.constants import factory_contract_obj
from computes.utils.constants import pair_contract_abi
from computes.utils.constants import ZER0_ADDRESS
from computes.utils import constants
from computes.utils.models.data_models import TickData, Slot0Data
from computes.preloaders.eth_price.preloader import eth_price_preloader
from snapshotter.settings.config import settings
from computes.utils.models.message_models import UniswapPoolMetadata
from computes.utils.redis_keys import uniswap_v3_best_pool_map
from snapshotter.utils.data_utils import get_project_latest_snapshot

AddressLike = Union[Address, ChecksumAddress]
getcontext().prec = 36

helper_logger = logger.bind(module='PowerLoom|Uniswap|Helpers')

SCORE_BLOCK_MULTIPLIER = 1_000_000

WETH_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.WETH)
USDC_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.USDC)


class Slot0DataError(Exception):
    """
    Custom exception for errors during slot0 data fetching or processing.
    """
    pass

def transform_tick_bytes_to_list(tick_bytes) -> List[TickData]:
    """
    Convert a list of tick byte arrays (from a decoded web3 call) into a list of TickData objects.

    Args:
        tick_bytes: List of bytes objects, each representing a tick's data.

    Returns:
        List[TickData]: List of TickData objects parsed from the input bytes.
    """
    if len(tick_bytes) == 0:
        return []

    # Each tick is encoded as bytes: liquidity_net (all but last 3 bytes), idx (last 3 bytes)
    ticks = [
        TickData(
            liquidity_net=int.from_bytes(i[:-3], 'big', signed=True),
            idx=int.from_bytes(i[-3:], 'big', signed=True),
        )
        for i in tick_bytes
    ]

    return ticks

def calculate_tvl_from_ticks(
    ticks: List[TickData],
    pair_metadata: UniswapPoolMetadata,
    sqrt_price: int
) -> Tuple[int, int]:
    """
    Calculate the Total Value Locked (TVL) for token0 and token1 from tick data and pool metadata.

    Args:
        ticks (List[TickData]): List of tick data objects.
        pair_metadata (UniswapPoolMetadata): Metadata for the token pair (includes fee).
        sqrt_price (int): Square root of the current price (uint160 from slot0).

    Returns:
        Tuple[int, int]: (token0_liquidity, token1_liquidity) as integers.
    """
    sqrt_price = Decimal(sqrt_price) / Decimal(2 ** 96)

    liquidity_total = Decimal(0)
    token0_liquidity = Decimal(0)
    token1_liquidity = Decimal(0)
    tick_spacing = Decimal(1)

    if not ticks:
        return (0, 0)

    int_fee = int(pair_metadata.fee)

    # Set tick spacing based on fee tier
    if int_fee == 3000:
        tick_spacing = Decimal(60)
    elif int_fee == 500:
        tick_spacing = Decimal(10)
    elif int_fee == 10000:
        tick_spacing = Decimal(200)

    # See: https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf
    for i in range(len(ticks)):
        tick = ticks[i]
        idx = Decimal(tick.idx)
        # Next tick index, or add tick_spacing if last tick
        nextIdx = Decimal(ticks[i + 1].idx) if i < len(ticks) - 1 else idx + tick_spacing

        liquidity_net = Decimal(tick.liquidity_net)
        liquidity_total += liquidity_net
        sqrtPriceLow = Decimal(1.0001) ** (idx / 2)
        sqrtPriceHigh = Decimal(1.0001) ** (nextIdx / 2)

        if sqrt_price <= sqrtPriceLow:
            # All liquidity is in token0
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
        elif sqrt_price >= sqrtPriceHigh:
            # All liquidity is in token1
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
        else:
            # Liquidity is split between token0 and token1
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

async def calculate_reserves(
    pair_address: str,
    at_block: int,
    pair_per_token_metadata: Optional[UniswapPoolMetadata],
    rpc_helper: RpcHelper,
) -> Tuple[int, int]:
    """
    Calculate reserves for a given Uniswap V3 pool at a specific block.

    Args:
        pair_address (str): The address of the Uniswap V3 pool.
        at_block (int): The block number to query.
        pair_per_token_metadata (Optional[UniswapPoolMetadata]): Metadata for the pool.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        Tuple[int, int]: (token0_reserves, token1_reserves)
    """
    if not pair_per_token_metadata:
        return (0, 0)
    helper_logger.debug(
        "[Epoch starting at {}] Pool {} | Calculating token0 and token1 reserves based on state at block {}",
        at_block,
        pair_address,
        at_block,
    )

    # Fetch tick data for the pool at the given block
    ticks_list = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        at_block=at_block,
        pair_per_token_metadata=pair_per_token_metadata,
    )
    
    if ticks_list is None:
        helper_logger.warning(f"Could not get required tick info for {pair_address} at block {at_block}")
        return (0, 0)
    
    # Fetch slot0 data for the pool at the given block
    slot0_data_dict_at_block = await get_slot0_data_for_block_range(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=at_block,
        to_block=at_block,
    )
    if slot0_data_dict_at_block is None:
        helper_logger.warning(f"Could not get required slot0 data for {pair_address} at block {at_block}")
        return (0, 0)
    sqrt_price = slot0_data_dict_at_block[at_block].sqrtPriceX96
    
    # Calculate TVL from ticks and slot0 price
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
    Fetch, validate, and process slot0 data for a given pool over a block range.

    Args:
        rpc_helper (RpcHelper): Helper for making RPC calls.
        pair_address (str): The address of the Uniswap V3 pool.
        from_block (int): Start block (inclusive).
        to_block (int): End block (inclusive).

    Returns:
        Dict[int, Slot0Data]: Mapping from block number to Slot0Data object.

    Raises:
        Slot0DataError: If fetching or parsing slot0 data fails.
    """
    helper_logger.debug(
        "[Range {}-{}] Pool {} | Fetching slot0 data",
        from_block, to_block, pair_address
    )

    if from_block > to_block:
        helper_logger.debug(
            f"Invalid or empty block range ({from_block}-{to_block}) for slot0 data for {pair_address}, "
            f"returning empty dict."
        )
        return {}

    slot0ResponseListRaw = None
    try:
        # Batch call to fetch slot0 data for each block in the range
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
        helper_logger.error(msg)
        raise Slot0DataError(msg) from e

    if slot0ResponseListRaw is None:
        msg = f"Batch call for slot0 returned None for {pair_address} range {from_block}-{to_block}"
        helper_logger.error(msg)
        raise Slot0DataError(msg)

    if not isinstance(slot0ResponseListRaw, list):
        msg = (
            f"Batch call for slot0 did not return a list for {pair_address} range {from_block}-{to_block}. "
            f"Got: {type(slot0ResponseListRaw)}"
        )
        helper_logger.error(msg)
        raise Slot0DataError(msg)
        
    slot0_data_dict: Dict[int, Slot0Data] = {}
    expected_len = to_block - from_block + 1

    if len(slot0ResponseListRaw) != expected_len:
        msg = (
            f"Slot0 response list length ({len(slot0ResponseListRaw)}) does not match expected "
            f"block range length ({expected_len}) for {pair_address} range {from_block}-{to_block}. "
            f"Expected complete data."
        )
        helper_logger.error(msg)
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
            helper_logger.error(msg)
            raise Slot0DataError(msg) from e_slot0_parse
    
    helper_logger.info(
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
    """
    Fetch tick data for a Uniswap V3 pool at a specific block.

    Args:
        rpc_helper (RpcHelper): Helper for making RPC calls.
        pair_address (str): The address of the Uniswap V3 pool.
        at_block (int): The block number to query.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the pool (includes fee).

    Returns:
        Optional[List[TickData]]: List of TickData objects, or None if fetch fails.
    """
    helper_logger.debug(
        "[Block {}] Pool {} | Fetching tick information",
        at_block, pair_address
    )
    try:
        fee = int(pair_per_token_metadata.fee)
        
        # Determine number of segments to split tick range by fee tier
        if fee < 500:
            num_segments = 16
        elif fee >= 500 and fee < 3000:
            num_segments = 4
        elif fee >= 3000 and fee < 10000:
            num_segments = 2
        elif fee >= 10000:
            num_segments = 1
        
        tick_tasks = []
        total_range = constants.MAX_TICK - constants.MIN_TICK + 1
        segment_size = total_range // num_segments

        # Prepare tasks to fetch ticks in segments
        for i in range(num_segments):
            from_tick = constants.MIN_TICK + i * segment_size
            if i == num_segments - 1:
                to_tick = constants.MAX_TICK
            else:
                to_tick = from_tick + segment_size - 1
            
            tick_tasks.append(('getTicks', [pair_address, from_tick, to_tick]))

        try:
            # Batched call to fetch tick data for all segments at the given block
            tickDataResponse = await rpc_helper.web3_call(
                tasks=tick_tasks, 
                contract_addr=constants.helper_contract.address,
                abi=constants.helper_contract.abi,
                tasks_block_override=[at_block for _ in range(len(tick_tasks))],
            )
        except Exception as e:
            helper_logger.opt(exception=True).error(
                'Unexpected error in get_tick_info for pool {} | block {}: {}',
                pair_address, at_block, e
            )
            return None

        if tickDataResponse is None:
            helper_logger.error(
                'Failed to gather required data for pool {} | block {}. ',
                pair_address, at_block
            )
            return None

        ticks_list: List[TickData] = []
        temp_ticks_list_of_lists = []
        # Convert each segment's bytes to TickData objects
        for ticks_bytes in tickDataResponse:
            if isinstance(ticks_bytes, Exception):
                helper_logger.warning(f"A batched RPC call for tick data failed: {ticks_bytes}")
                continue
            temp_ticks_list_of_lists.append(transform_tick_bytes_to_list(ticks_bytes))
        
        if temp_ticks_list_of_lists:
            # Flatten the list of lists, skipping empty lists
            non_empty_tick_lists = [lst for lst in temp_ticks_list_of_lists if lst]
            if non_empty_tick_lists:
                ticks_list = reduce(lambda x, y: x + y, non_empty_tick_lists)

        helper_logger.info(
            'Fetched tick data ({}) for pool {} @ block {}',
            len(ticks_list), pair_address, at_block
        )

        return ticks_list

    except Exception as e:
        helper_logger.opt(exception=True).error(
            'Unexpected error in get_tick_info for pool {} | block {}: {}',
            pair_address, at_block, e
        )
        return None


async def get_pool_metadata(
    pool_address: str,
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    task_type: str = 'metadata:{poolAddress}:{Namespace}',
) -> UniswapPoolMetadata:
    """
    Retrieve metadata for a specific Uniswap V3 pool, checking Redis cache first, then fetching from chain if needed.

    Args:
        pool_address (str): The address of the pool to get metadata for.
        redis_conn (aioredis.Redis): Redis connection for cache operations.
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
        protocol_state_contract: Contract instance for protocol state.
        task_type (str): Format string for project ID construction.

    Returns:
        Optional[UniswapPoolMetadata]: Pool metadata if found, None otherwise.

    Raises:
        Exception: If metadata cannot be found or fetched.
    """
    # Check Redis cache first for existing metadata
    cache_key = f'pool_metadata:{pool_address}'
    cached_data = await redis_conn.get(cache_key)
    if cached_data:
        helper_logger.info(f"Found cached metadata for pool {pool_address}")
        return UniswapPoolMetadata(**json.loads(cached_data))

    try:
        # Get the latest snapshot from chain if not in cache
        project_id = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
        latest_snapshot = await get_project_latest_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id
        )
        if not latest_snapshot:
            helper_logger.error(f"No latest snapshot found for pool {pool_address} while processing metadata")
            raise Exception(f"No latest snapshot found for pool {pool_address} while processing metadata")
        return UniswapPoolMetadata(**latest_snapshot)
    except Exception as e:
        helper_logger.opt(exception=e).error(f"Error getting latest snapshot for pool {pool_address} while processing metadata")
        raise Exception(f"Error getting latest snapshot for pool {pool_address} while processing metadata")


async def get_token_price_in_block_range(
    pair_metadata: UniswapPoolMetadata,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Fetch the price of token0 and token1 for a given Uniswap V3 pool over a specified block range.

    This function queries the Uniswap V3 pool contract's `slot0` function for each block in the range,
    extracts the sqrtPriceX96 value, and converts it to token prices using the provided token decimals.
    The prices are returned as dictionaries mapping block numbers to prices for both token0 and token1.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool, including token decimals and address.
        from_block (int): The starting block number (inclusive).
        to_block (int): The ending block number (inclusive).
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): Helper object to perform batched RPC calls.

    Returns:
        Tuple[Dict[int, float], Dict[int, float]]:
            - token0_price: Mapping from block number to token0 price.
            - token1_price: Mapping from block number to token1 price.
    """
    response = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=get_contract_abi_dict(
            abi=pair_contract_abi,
        ),
        contract_address=pair_metadata.address,
        from_block=from_block,
        to_block=to_block,
        function_name='slot0',
        params=[],
    )

    # Log the slot0 responses for debugging and traceability.
    helper_logger.info(
        'Epoch {}-{} | Pool {} | Slot0 response: {}',
        from_block, to_block, pair_metadata.address, response
    )

    # Initialize dictionaries to store prices for each block.
    token0_price = {}
    token1_price = {}

    # Iterate over each block in the range and compute token prices.
    for i, block_num in enumerate(range(from_block, to_block + 1)):
        # Extract sqrtPriceX96 from the slot0 response for the current block.
        sqrtP = response[i][0]
        # Convert sqrtPriceX96 to token0 and token1 prices using the correct decimals.
        price0, price1 = eth_price_preloader.sqrtPriceX96ToTokenPrices(
            sqrtP,
            pair_metadata.token0.decimals,
            pair_metadata.token1.decimals,
        )
        # Store the computed prices in the result dictionaries.
        token0_price[block_num] = price0
        token1_price[block_num] = price1

    # Cache the raw prices for future use
    await cache_token_price_raw_at_height(
        token0_address=pair_metadata.token0.address,
        token0_price_dict=token0_price,
        token1_address=pair_metadata.token1.address,
        token1_price_dict=token1_price,
        redis_conn=redis_conn,
    )

    # Return the price mappings for token0 and token1.
    return token0_price, token1_price


async def get_token_price_in_usd_in_block_range(
    pair_metadata: UniswapPoolMetadata,
    from_block: int,
    to_block: int,
    redis_conn,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader,
    protocol_state_contract,
    rpc_helper: RpcHelper,
):
    """
    Fetch the price of token0 and token1 in USD for a given Uniswap V3 pool over a specified block range.

    This function first checks if the prices are already cached in Redis. If not, it computes the prices
    using the best available method depending on whether the tokens are WETH, USDC, or require a reference pool.
    The computed prices are then cached for future use.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool.
        from_block (int): The starting block number (inclusive).
        to_block (int): The ending block number (inclusive).
        redis_conn: Redis connection object.
        anchor_rpc_helper (RpcHelper): Helper for anchor chain RPC calls.
        ipfs_reader: IPFS reader object for metadata.
        protocol_state_contract: Protocol state contract object.
        rpc_helper (RpcHelper): Helper object to perform batched RPC calls.

    Returns:
        Tuple[
            Dict[int, float],  # token0_price_raw: token0 price in terms of token1 for each block
            Dict[int, float],  # token1_price_raw: token1 price in terms of token0 for each block
            Dict[int, float],  # token0_price: token0 price in USD for each block
            Dict[int, float],  # token1_price: token1 price in USD for each block
        ]
    """
    token0_price_raw, token1_price_raw = await get_token_price_in_block_range(
        pair_metadata=pair_metadata,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )
 
    # If either token0 or token1 is WETH, use ETH/USD price for conversion.
    if Web3.to_checksum_address(pair_metadata.token0.address) == WETH_ADDRESS or Web3.to_checksum_address(pair_metadata.token1.address) == WETH_ADDRESS:
        # Fetch ETH/USD price for the block range.
        eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        if Web3.to_checksum_address(pair_metadata.token0.address) == WETH_ADDRESS:
            # token0 is WETH: its price is ETH/USD, token1 is relative to ETH.
            token0_price = eth_usd_price_dict
            token1_price = {
                block_num: eth_usd_price_dict[block_num] * token1_price_raw[block_num]
                for block_num in token1_price_raw
            }
        else:
            # token1 is WETH: its price is ETH/USD, token0 is relative to ETH.
            token0_price = {
                block_num: eth_usd_price_dict[block_num] * token0_price_raw[block_num]
                for block_num in token0_price_raw
            }
            token1_price = eth_usd_price_dict

    # If either token0 or token1 is USDC, use 1 USD as the price for USDC.
    elif Web3.to_checksum_address(pair_metadata.token0.address) == USDC_ADDRESS or Web3.to_checksum_address(pair_metadata.token1.address) == USDC_ADDRESS:
        if Web3.to_checksum_address(pair_metadata.token0.address) == USDC_ADDRESS:
            # token0 is USDC: price is 1 USD, token1 is relative to USDC.
            token0_price = {
                block_num: 1 for block_num in token0_price_raw
            }
            token1_price = token1_price_raw
        else:
            # token1 is USDC: price is 1 USD, token0 is relative to USDC.
            token0_price = token0_price_raw
            token1_price = {
                block_num: 1 for block_num in token1_price_raw
            }
    else:
        # For other tokens, identify the best pool (with WETH or USDC) to use as a price reference.
        best_pool_token_address = await identify_best_pool_to_calculate_price(
            pair_metadata=pair_metadata,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        # Generate pool metadata for the best reference pool.
        best_pool_metadata = await get_pool_metadata(
            pool_address=best_pool_token_address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )

        # Get the addresses of the tokens in the best pool.
        best_pool_tokens = [Web3.to_checksum_address(best_pool_metadata.token0.address), Web3.to_checksum_address(best_pool_metadata.token1.address)]
        # Recursively fetch the USD prices for the tokens in the best pool.
        _, _, best_pool_token0_price_usd, best_pool_token1_price_usd = await get_token_price_in_usd_in_block_range(
            pair_metadata=best_pool_metadata,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            rpc_helper=rpc_helper,
        )

        # Determine which token in the best pool matches token0 or token1 of the original pair.
        if Web3.to_checksum_address(pair_metadata.token0.address) in best_pool_tokens:
            # If token0 is in the best pool, use its USD price directly.
            if Web3.to_checksum_address(pair_metadata.token0.address) == Web3.to_checksum_address(best_pool_metadata.token0.address):
                reference_token_price_usd = best_pool_token0_price_usd
            else:
                reference_token_price_usd = best_pool_token1_price_usd

            token0_price = reference_token_price_usd
            # token1 price is token0 price * token1/token0 price ratio.
            token1_price = {
                block_num: reference_token_price_usd[block_num] * token1_price_raw[block_num]
                for block_num in token1_price_raw
            }
        else:
            # If token1 is in the best pool, use its USD price directly.
            if Web3.to_checksum_address(pair_metadata.token1.address) == Web3.to_checksum_address(best_pool_metadata.token0.address):
                reference_token_price_usd = best_pool_token0_price_usd
            else:
                reference_token_price_usd = best_pool_token1_price_usd

            # token0 price is token1 price * token0/token1 price ratio.
            token0_price = {
                block_num: reference_token_price_usd[block_num] * token0_price_raw[block_num]
                for block_num in token0_price_raw
            }
            token1_price = reference_token_price_usd

    return token0_price_raw, token1_price_raw, token0_price, token1_price


async def identify_best_liquidity_pool(
    token0: str,
    token1: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the best Uniswap V3 pair address for two tokens based on liquidity across different fee tiers.

    This function queries the Uniswap V3 factory for all possible pools between token0 and token1
    at common fee tiers, then fetches the liquidity for each pool, and returns the address of the pool
    with the highest liquidity.

    Args:
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        Tuple[str, int]: The address of the best pair contract and its liquidity.
    """
    # Prepare tasks to get the pool address for each fee tier.
    tasks = [
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(10000), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(3000), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(500), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(100), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
    ]
    # Fetch all pool addresses for the given token pair and fee tiers.
    pair_address_list = await asyncio.gather(*tasks)
    # Filter out zero addresses (non-existent pools).
    pair_address_list = [pair for pair in pair_address_list if pair != ZER0_ADDRESS]

    if len(pair_address_list) > 0:
        # For each valid pool, create a contract object.
        pair_contracts = [
            current_node['web3_client'].eth.contract(
                address=Web3.to_checksum_address(pair),
                abi=pair_contract_abi,
            ) for pair in pair_address_list
        ]

        # Prepare tasks to fetch the liquidity for each pool.
        tasks = [
            asyncio.create_task(
                rpc_helper.web3_call(
                    tasks=[('liquidity', [])],
                    contract_addr=pair_contract.address,
                    abi=pair_contract.abi,
                )
            ) for pair_contract in pair_contracts
        ]

        # Fetch all liquidity values.
        liquidity_list = await asyncio.gather(*tasks)
        # Pair each pool address with its liquidity.
        pair_liquidity_dict = zip(pair_address_list, liquidity_list)
        # Find the pool with the highest liquidity.
        best_pair, best_liquidity = max(pair_liquidity_dict, key=lambda x: x[1])
        return best_pair, best_liquidity[0]

    # If no valid pools found, return zero address and zero liquidity.
    return ZER0_ADDRESS, 0


async def identify_best_pool_to_calculate_price(
    pair_metadata: UniswapPoolMetadata,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Identify the best Uniswap V3 pool to use as a price reference for a given pair.

    This function checks if the best pool is already cached in Redis. If not, it tries all combinations
    of the pair's tokens with WETH and USDC, finds the pool with the highest liquidity, and caches the result.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool.
        redis_conn: Redis connection object.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The address of the best pool to use for price calculation.
    """
    # Check if the best pool address is already cached in Redis.
    best_pair_address = await redis_conn.hget(
        uniswap_v3_best_pool_map,
        pair_metadata.address,
    )
    if best_pair_address:
        return best_pair_address.decode('utf-8')

    # Prepare all token combinations with WETH and USDC.
    token_options = [WETH_ADDRESS, USDC_ADDRESS]
    all_token_options = (
        [(pair_metadata.token0.address, token_option) for token_option in token_options] +
        [(pair_metadata.token1.address, token_option) for token_option in token_options]
    )

    best_pair_address, best_liquidity = ZER0_ADDRESS, 0
    # Prepare tasks to identify the best liquidity pool for each token combination.
    tasks = [
        asyncio.create_task(
            identify_best_liquidity_pool(
                token0=token_option[0], token1=token_option[1], redis_conn=redis_conn, rpc_helper=rpc_helper)
        ) for token_option in all_token_options
    ]
    # Fetch all results.
    results = await asyncio.gather(*tasks)
    # Find the pool with the highest liquidity among all combinations.
    for pair_address, liquidity in results:
        helper_logger.info(f"Pair address: {pair_address}, liquidity: {liquidity}")
        if pair_address != ZER0_ADDRESS and liquidity > best_liquidity:
            best_pair_address = pair_address
            best_liquidity = liquidity

    # Cache the best pool address in Redis for future use.
    if best_pair_address != ZER0_ADDRESS:
        await redis_conn.hset(
            uniswap_v3_best_pool_map,
            mapping={
                pair_metadata.address: best_pair_address,
            },
        )

    return best_pair_address


async def get_events_from_cache(
    pool_address: str,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis
) -> Dict[int, List[UniswapEvent]]:
    """
    Fetch event logs from Redis cache for a given pool address and block range.

    Args:
        pool_address (str): The pool contract address.
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        redis_conn (aioredis.Redis): Redis connection.

    Returns:
        Dict[int, List[UniswapEvent]]: Dictionary mapping block numbers to lists of event objects.

    Example event JSON entry:
    {
        "eventName": "Swap",
        "filterName": "uniswapv3_pool_events",
        "txHash": "0x2f82087ed4d3bbf77c559d1337e3903a7108927cf9a2c9dae33bcce36f88933c",
        "blockNumber": 22394920,
        "txIndex": 4,
        "logIndex": 38,
        "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "topics": [
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
            "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
            "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"
        ],
        "data": "0x00000000000000000000000000000000000000000000000000000000203d7eb8fffffffffffffffffffffffffffffffffffffffffffffffffbe1f9518d38ad830000000000000000000000000000000000005b81d1e1ed548107534638248648000000000000000000000000000000000000000000000000516f1f5b22a9ea090000000000000000000000000000000000000000000000000000000000031219",
        "args": {
            "sender": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
            "recipient": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
            "amount0": 540901048,
            "amount1": -296681971772772989,
            "sqrtPriceX96": 1855984662392763862973509127145032,
            "liquidity": 5867943315771091465,
            "tick": 201241
        },
        "_score": 22394920000038
    }
    """
    # Calculate score range for zrangebyscore
    min_score = from_block * SCORE_BLOCK_MULTIPLIER
    max_score = (to_block + 1) * SCORE_BLOCK_MULTIPLIER - 1  # -1 to not include next block's events
    
    # Get events from Redis zset
    events = await redis_conn.zrangebyscore(
        name=f"events:{settings.namespace}:address:{pool_address}",
        min=min_score,
        max=max_score,
        withscores=True
    )

    helper_logger.info(f"Found {len(events)} events in raw cache for pool {pool_address} from block {from_block} to block {to_block}")

    if len(events) > 0:
        helper_logger.info(f"First event: {events[0]}")
        helper_logger.info(f"Last event: {events[-1]}")
    
    # Group events by block number
    block_events: Dict[int, List[UniswapEvent]] = {}
    for block in range(from_block, to_block + 1):
        block_events[block] = []

    for event_json, score in events:
        event_data = json.loads(event_json)
        event_data['_score'] = score  # Add score to event data
        
        try:
            event = UniswapEvent.model_validate(event_data)
        except Exception as e_parse:
            helper_logger.error(f"Failed to parse event data: {event_data}. Error: {e_parse}")
            continue
            
        block_number = event.blockNumber
        
        if block_number not in block_events:
            block_events[block_number] = []
            
        block_events[block_number].append(event)

    helper_logger.info(f"Found {len(block_events)} events in cache for pool {pool_address} from block {from_block} to block {to_block}")
    
    return block_events

async def get_pair(
    factory_contract_obj,
    token0,
    token1,
    fee,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the pair address for two tokens and the given fee, using Redis cache when available.

    Args:
        factory_contract_obj: The factory contract object.
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        fee (int): The fee for the pair.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The pair address.
    """
    # Check if pair address is cached in Redis
    pair_address_cache = await redis_conn.hget(
        uniswap_tokens_pair_map,
        f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}|{fee}',
    )
    if pair_address_cache:
        pair_address_cache = pair_address_cache.decode('utf-8')
        return Web3.to_checksum_address(pair_address_cache)

    # If not cached, fetch from chain
    tasks = [
        ('getPool', [Web3.to_checksum_address(token0), Web3.to_checksum_address(token1), fee]),
    ]

    result = await rpc_helper.web3_call(
        tasks=tasks,
        contract_addr=factory_contract_obj.address,
        abi=factory_contract_obj.abi,
    )
    pair = result[0]
    # Cache the pair address in Redis for future use
    await redis_conn.hset(
        name=uniswap_tokens_pair_map,
        mapping={
            f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}|{fee}': Web3.to_checksum_address(
                pair,
            ),
        },
    )

    return pair
