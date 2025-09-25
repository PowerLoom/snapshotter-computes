import json
import time
from typing import Dict, Optional, Any, Tuple, List

from redis import asyncio as aioredis
from computes.utils.models.message_models import UniswapBaseSnapshot, UniswapPoolMetadata, EpochBaseSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.utils.redis_keys import uniswap_pair_cached_block_height_reserves
from computes.utils.helpers import calculate_reserves
from computes.utils.constants import UNISWAPV3_FEE_DIV
from computes.utils.helpers import get_events_from_cache
from computes.utils.models.data_models import UniswapEvent, UniswapProcessedLog
from computes.utils.models.data_models import PairBlockDetail
from computes.utils.models.data_models import TradeData
from computes.utils.helpers import get_token_price_in_usd_in_block_range
from computes.utils.helpers import get_uniswap_v3_pool_metadata

core_logger = logger.bind(module='PowerLoom|UniswapCore')


async def fetch_initial_reserves(
    pair_address: str,
    at_block: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    pair_per_token_metadata: UniswapPoolMetadata,
    use_cache: bool = False,
) -> Optional[Tuple[int, int]]:
    """
    Fetch the initial reserves for a given Uniswap V3 pool contract address at a specific block.

    This function first attempts to retrieve the reserves from Redis cache for the block immediately
    preceding the given block. If not found, it calculates the reserves using the blockchain.

    Args:
        pair_address (str): The Uniswap V3 pool contract address.
        at_block (int): The block number at which to fetch reserves.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.

    Returns:
        Optional[Tuple[int, int]]: Tuple of (token0_reserves, token1_reserves) or None if not found.
    """
    if use_cache:
        # Attempt to fetch previous epoch end block reserves from Redis cache
        cached_reserves_dict = await redis_conn.zrangebyscore(
            name=uniswap_pair_cached_block_height_reserves.format(
                Web3.to_checksum_address(pair_address),
            ),
            min=int(at_block - 1),
            max=int(at_block - 1),
        )

        if cached_reserves_dict:
            # Cached reserves found, use them
            loaded_dict = json.loads(cached_reserves_dict[0])
            initial_reserves = [int(loaded_dict['token0_reserves']), int(loaded_dict['token1_reserves'])]
            core_logger.debug(
                "[Block {}] Pool {} | Using cached reserves: token0={}, token1={}",
                at_block,
                pair_address,
                initial_reserves[0],
                initial_reserves[1]
            )
            return initial_reserves
    # No cache found, calculate reserves from chain
    initial_reserves = await calculate_reserves(
        pair_address,
        at_block - 1,
        pair_per_token_metadata,
        rpc_helper,
    )
    core_logger.info(
        "[Block {}] Pool {} | Calculated initial reserves: token0={}, token1={}",
        at_block,
        pair_address,
        initial_reserves[0],
        initial_reserves[1]
    )

    return initial_reserves


async def _cache_pair_reserves_for_final_block(
    pair_address: str,
    redis_conn: aioredis.Redis,
    pair_reserves_dict: Dict[int, PairBlockDetail],
    to_block: int,
) -> None:
    """
    Cache the pair reserves for the final block of the epoch in Redis.

    This allows the reserves to be used as a starting point in the next epoch.

    Args:
        pair_address (str): The Uniswap V3 pool contract address.
        redis_conn (aioredis.Redis): Redis connection for caching.
        pair_reserves_dict (Dict[int, PairBlockDetail]): Dictionary of block number to reserves.
        to_block (int): The final block number of the epoch.
    """
    # Get the reserves for the final block in the epoch
    end_block_data_for_cache = pair_reserves_dict.get(to_block, None)

    if end_block_data_for_cache:
        # Prepare the cache mapping for Redis sorted set
        redis_cache_mapping = {
            json.dumps({
                'blockHeight': to_block,
                'token0_reserves': end_block_data_for_cache.token0Reserves,
                'token1_reserves': end_block_data_for_cache.token1Reserves
            }): int(to_block),
        }
        pipeline = redis_conn.pipeline()
        # Add the reserves for the final block to the cache
        pipeline.zadd(
            name=uniswap_pair_cached_block_height_reserves.format(Web3.to_checksum_address(pair_address)),
            mapping=redis_cache_mapping,
        )
        # Remove old cache entries (older than to_block - 20)
        pipeline.zremrangebyscore(
            name=uniswap_pair_cached_block_height_reserves.format(
                Web3.to_checksum_address(pair_address),
            ),
            min=0,
            max=to_block - 20,
        )
        await pipeline.execute()


async def generate_pair_reserves_dict_and_trade_data(
    pair_address: str,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    pair_per_token_metadata: UniswapPoolMetadata,
    token0_price_map: Dict[int, float],
    token1_price_map: Dict[int, float],
    token0_price_raw: Dict[int, float],
    token1_price_raw: Dict[int, float],
    block_details_dict: Dict[int, Dict[str, Any]],
) -> Tuple[Dict[int, PairBlockDetail], TradeData]:
    """
    Generate a dictionary of reserves per block and aggregate trade data for a Uniswap V3 pool.

    This function iterates over each block in the given range, processes all events,
    updates reserves, and accumulates trade data for the epoch.

    Args:
        pair_address (str): The Uniswap V3 pool contract address.
        from_block (int): Starting block number for the epoch.
        to_block (int): Ending block number for the epoch.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.
        token0_price_map (Dict[int, float]): Mapping of block number to token0 USD price.
        token1_price_map (Dict[int, float]): Mapping of block number to token1 USD price.
        token0_price_raw (Dict[int, float]): Mapping of block number to token0 price in token1.
        token1_price_raw (Dict[int, float]): Mapping of block number to token1 price in token0.
        block_details_dict (Dict[int, Dict[str, Any]]): Block details including timestamps.

    Returns:
        Tuple[Dict[int, PairBlockDetail], TradeData]: 
            - Dictionary mapping block number to PairBlockDetail.
            - Aggregated TradeData for the epoch.
    """
    # Fetch initial reserves at the start of the epoch
    initial_reserves = await fetch_initial_reserves(
        pair_address=pair_address,
        at_block=from_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        pair_per_token_metadata=pair_per_token_metadata,
    )

    # Initialize reserve amounts
    token0Amount = initial_reserves[0]
    token1Amount = initial_reserves[1]
    
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Initial reserves: token0={}, token1={}",
        from_block, to_block, pair_address, token0Amount, token1Amount
    )
    # Initialize accumulators for epoch-wide trade data
    epoch_total_trade_data = TradeData()

    # Fetch all events for the pool in the block range from cache
    events_dict = await get_events_from_cache(
        pool_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )
    
    # Normalize initial reserves
    token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
    token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))
    
    pair_reserves_dict = dict()

    # Iterate over each block in the range
    for block_num in range(from_block, to_block + 1):
        event_list = events_dict.get(block_num, [])

        # Track the net change in reserves for this block
        block_delta_token0 = 0
        block_delta_token1 = 0

        # Process each event in the block
        for i, event_data_obj in enumerate(event_list):
            # Extract trade data from the event log
            current_event_trade_data, _ = extract_trade_volume_log(
                event_name=event_data_obj.eventName,
                log=event_data_obj,
                pair_per_token_metadata=pair_per_token_metadata,
                token0_price_map=token0_price_map,
                token1_price_map=token1_price_map,
                block_details_dict=block_details_dict,
            )
            if current_event_trade_data:
                # Accumulate trade data for the epoch (uses __add__ method)
                epoch_total_trade_data += current_event_trade_data

            # Update reserve deltas based on event type
            event_amount0 = event_data_obj.args['amount0']
            event_amount1 = event_data_obj.args['amount1']
            
            if event_data_obj.eventName == 'Burn':
                # Burn events remove liquidity from the pool
                block_delta_token0 -= event_amount0
                block_delta_token1 -= event_amount1
            else:
                # Mint and Swap events add liquidity or swap tokens
                # Swap events use a negative value for the token that was removed from the pool
                block_delta_token0 += event_amount0
                block_delta_token1 += event_amount1
            
            core_logger.debug(
                "[Block {}] Pool {} | Event {} | Post-event deltas: token0_delta={}, token1_delta={}",
                block_num, pair_address, i+1, block_delta_token0, block_delta_token1
            )
        
        token0Amount += block_delta_token0
        token1Amount += block_delta_token1

        # Normalize reserves for this block
        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))

        # Get block details (e.g., timestamp)
        current_block_details = block_details_dict.get(block_num, {})

        # Store reserves and price data for this block
        pair_reserves_dict[block_num] = PairBlockDetail(
            token0ReservesNormalized=token0AmountNormalized,
            token1ReservesNormalized=token1AmountNormalized,
            token0Reserves=token0Amount,
            token1Reserves=token1Amount,
            token0ReservesUSD=token0AmountNormalized * token0_price_map.get(block_num, 0),
            token1ReservesUSD=token1AmountNormalized * token1_price_map.get(block_num, 0),
            token0Price=token0_price_map.get(block_num, 0),
            token1Price=token1_price_map.get(block_num, 0),
            token0PriceInToken1=token0_price_raw.get(block_num, 0),
            token1PriceInToken0=token1_price_raw.get(block_num, 0),
            timestamp=current_block_details.get('timestamp', 0),
        )
        
        # Cache the reserves for the final block of the epoch
        await _cache_pair_reserves_for_final_block(
            pair_address=pair_address,
            redis_conn=redis_conn,
            pair_reserves_dict=pair_reserves_dict,
            to_block=to_block,
        )

    core_logger.info(
        "[Epoch {}-{}] Pool {} | Pair reserves dict: {}",
        from_block,
        to_block,
        pair_address,
        pair_reserves_dict
    )

    core_logger.info(
        "[Epoch {}-{}] Pool {} | Epoch total trade data: {}",
        from_block,
        to_block,
        pair_address,
        epoch_total_trade_data
    )

    return pair_reserves_dict, epoch_total_trade_data


async def generate_base_snapshot(
    pair_address: str,
    from_block: int,
    to_block: int,
    pair_reserves_dict: Dict[int, PairBlockDetail],
    epoch_total_trade_data: TradeData,
    pair_per_token_metadata: UniswapPoolMetadata,
    block_details_dict: Dict[int, Dict[str, Any]],
) -> Optional[UniswapBaseSnapshot]:
    """
    Generate a UniswapBaseSnapshot object containing all per-block and aggregated data for the epoch.

    Args:
        pair_address (str): The Uniswap V3 pool contract address.
        from_block (int): Starting block number for the epoch.
        to_block (int): Ending block number for the epoch.
        pair_reserves_dict (Dict[int, PairBlockDetail]): Per-block reserves and price data.
        epoch_total_trade_data (TradeData): Aggregated trade data for the epoch.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.
        block_details_dict (Dict[int, Dict[str, Any]]): Block details including timestamps.

    Returns:
        Optional[UniswapBaseSnapshot]: The snapshot object, or None if failed.
    """
    # Initialize dictionaries to hold per-block data for the snapshot
    token0ReservesSnap = {}
    token1ReservesSnap = {}
    token0ReservesUSDSnap = {}
    token1ReservesUSDSnap = {}
    token0PricesSnap = {}
    token1PricesSnap = {}
    token0PricesUSDSnap = {}
    token1PricesUSDSnap = {}
    timestampsSnap = {}

    # Populate per-block data for the snapshot
    for block_num_snap in range(from_block, to_block + 1):
        block_data_obj: Optional[PairBlockDetail] = pair_reserves_dict.get(block_num_snap, {})

        token0ReservesSnap[block_num_snap] = block_data_obj.token0ReservesNormalized
        token1ReservesSnap[block_num_snap] = block_data_obj.token1ReservesNormalized
        token0ReservesUSDSnap[block_num_snap] = block_data_obj.token0ReservesUSD
        token1ReservesUSDSnap[block_num_snap] = block_data_obj.token1ReservesUSD
        token0PricesSnap[block_num_snap] = block_data_obj.token0PriceInToken1
        token1PricesSnap[block_num_snap] = block_data_obj.token1PriceInToken0
        # USD price of token0
        token0PricesUSDSnap[block_num_snap] = block_data_obj.token0Price
        # USD price of token1
        token1PricesUSDSnap[block_num_snap] = block_data_obj.token1Price
        timestampsSnap[block_num_snap] = block_data_obj.timestamp

    # Set the snapshot timestamp to the timestamp of the end block, if available
    snapshot_timestamp = 0  # Default timestamp
    end_block_data: Optional[PairBlockDetail] = pair_reserves_dict.get(to_block)
    if end_block_data and end_block_data.timestamp is not None:
        snapshot_timestamp = end_block_data.timestamp

    # Create the UniswapBaseSnapshot object
    base_reserves_snapshot = UniswapBaseSnapshot(
        address=pair_address,
        epoch=EpochBaseSnapshot(
            begin=from_block,
            end=to_block,
        ),
        timestamps=timestampsSnap,
        token0=Web3.to_checksum_address(pair_per_token_metadata.token0.address),
        token1=Web3.to_checksum_address(pair_per_token_metadata.token1.address),
        token0Reserves=token0ReservesSnap,
        token1Reserves=token1ReservesSnap,
        token0ReservesUSD=token0ReservesUSDSnap,
        token1ReservesUSD=token1ReservesUSDSnap,
        token0Prices=token0PricesSnap,
        token1Prices=token1PricesSnap,
        token0PricesUSD=token0PricesUSDSnap,
        token1PricesUSD=token1PricesUSDSnap,
        # Add aggregated trade volume data
        totalTrade=epoch_total_trade_data.totalTradesUSD,
        totalTradeMintBurn=epoch_total_trade_data.totalTradesMintBurnUSD,
        totalFee=epoch_total_trade_data.totalFeeUSD,
        token0MintBurnVolume=epoch_total_trade_data.token0MintBurnVolume,
        token1MintBurnVolume=epoch_total_trade_data.token1MintBurnVolume,
        token0MintBurnVolumeUSD=epoch_total_trade_data.token0MintBurnVolumeUSD,
        token1MintBurnVolumeUSD=epoch_total_trade_data.token1MintBurnVolumeUSD,
        token0TradeVolume=epoch_total_trade_data.token0TradeVolume,
        token1TradeVolume=epoch_total_trade_data.token1TradeVolume,
        token0TradeVolumeUSD=epoch_total_trade_data.token0TradeVolumeUSD,
        token1TradeVolumeUSD=epoch_total_trade_data.token1TradeVolumeUSD,
    )
    return base_reserves_snapshot


async def base_snapshot_from_block_range(
    pair_address: str,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_details_dict: dict = dict(),
) -> Optional[UniswapBaseSnapshot]:
    """
    Generate a comprehensive base snapshot for a Uniswap V3 pool over a block range.

    This function orchestrates the entire snapshot generation process by:
        1. Fetching block details if not provided.
        2. Retrieving pool metadata.
        3. Getting token price data.
        4. Generating pair reserves and trade data.
        5. Creating the final snapshot.

    Args:
        pair_address (str): The Uniswap V3 pool contract address.
        from_block (int): Starting block number for the epoch.
        to_block (int): Ending block number for the epoch.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        anchor_rpc_helper (RpcHelper): Anchor RPC helper for metadata.
        ipfs_reader (AsyncIPFSClient): IPFS client for reading metadata.
        protocol_state_contract: Protocol state contract instance.
        block_details_dict (dict, optional): Pre-fetched block details.

    Returns:
        Optional[UniswapBaseSnapshot]: Snapshot containing all pool data for the epoch, or None if failed.
    """
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Starting base snapshot generation | Wall time: {}",
        from_block,
        to_block,
        pair_address,
        time.time()
    )
    
    try:
        # Normalize address format to checksum
        pair_address = Web3.to_checksum_address(pair_address)

        # Fetch block details if not provided
        if not block_details_dict:
            core_logger.debug(
                "[Epoch {}-{}] Pool {} | Fetching block details",
                from_block,
                to_block,
                pair_address
            )
            block_details_dict = await get_block_details_in_block_range(
                from_block,
                to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
            core_logger.debug(
                "[Epoch {}-{}] Pool {} | Block details fetched successfully",
                from_block,
                to_block,
                pair_address
            )

        # Get pool metadata (token addresses, decimals, fee, etc.)
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Fetching pool metadata",
            from_block,
            to_block,
            pair_address
        )
        pair_per_token_metadata: Optional[UniswapPoolMetadata] = await get_uniswap_v3_pool_metadata(
            pool_address=pair_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )

        if not pair_per_token_metadata:
            # If metadata could not be fetched, log error and return None
            core_logger.error(
                "[Epoch {}-{}] Pool {} | Failed to fetch pool metadata",
                from_block,
                to_block,
                pair_address
            )
            return None

        core_logger.debug(
            '[Epoch {}-{}] Pool {} | Pool metadata retrieved: {}',
            from_block,
            to_block,
            pair_address,
            (
                pair_per_token_metadata.model_dump() 
                if hasattr(pair_per_token_metadata, 'model_dump') 
                else str(pair_per_token_metadata)
            )
        )

        # Get token price data for the block range
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Fetching token price data",
            from_block,
            to_block,
            pair_address
        )
        (
            token0_price_raw,
            token1_price_raw,
            token0_price_map,
            token1_price_map
        ) = await get_token_price_in_usd_in_block_range(
            pair_metadata=pair_per_token_metadata,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            rpc_helper=rpc_helper,
        )

        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Token prices fetched successfully",
            from_block,
            to_block,
            pair_address
        )

        # Generate per-block reserves and aggregate trade data
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Generating reserves and trade data",
            from_block,
            to_block,
            pair_address
        )
        pair_reserves_dict, epoch_total_trade_data = await generate_pair_reserves_dict_and_trade_data(
            pair_address=pair_address,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            pair_per_token_metadata=pair_per_token_metadata,
            token0_price_map=token0_price_map,
            token1_price_map=token1_price_map,
            token0_price_raw=token0_price_raw,
            token1_price_raw=token1_price_raw,
            block_details_dict=block_details_dict,
        )

        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Reserves and trade data generated successfully",
            from_block,
            to_block,
            pair_address
        )

        # Generate the final base snapshot object
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Creating final base snapshot",
            from_block,
            to_block,
            pair_address
        )
        base_snapshot = await generate_base_snapshot(
            pair_address=pair_address,
            from_block=from_block,
            to_block=to_block,
            pair_reserves_dict=pair_reserves_dict,
            epoch_total_trade_data=epoch_total_trade_data,
            pair_per_token_metadata=pair_per_token_metadata,
            block_details_dict=block_details_dict,
        )

        if base_snapshot:
            core_logger.info(
                "[Epoch {}-{}] Pool {} | Base snapshot generated successfully | "
                "Total trade: ${:.2f} | Total fee: ${:.2f}",
                from_block,
                to_block,
                pair_address,
                base_snapshot.totalTrade,
                base_snapshot.totalFee
            )
        else:
            core_logger.error(
                "[Epoch {}-{}] Pool {} | Failed to generate base snapshot",
                from_block,
                to_block,
                pair_address
            )

        return base_snapshot

    except Exception as e:
        # Log any exception that occurs during snapshot generation
        core_logger.opt(exception=True).error(
            "[Epoch {}-{}] Pool {} | Failed to generate base snapshot: {}",
            from_block,
            to_block,
            pair_address,
            str(e)
        )
        return None


def token_native_and_usd_amount(
    log: UniswapEvent,
    pair_per_token_metadata: UniswapPoolMetadata,
    token_key: str,
    token_type: str,
    current_token_price_map: Dict[int, float],
) -> Tuple[float, float]:
    """
    Calculate the native and USD amounts for a token from an event log.

    Args:
        log (UniswapEvent): The Uniswap event log containing transaction data.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.
        token_key (str): The token key ('token0' or 'token1').
        token_type (str): The amount type ('amount0' or 'amount1').
        current_token_price_map (Dict[int, float]): Mapping of block numbers to token prices.

    Returns:
        Tuple[float, float]: (native_amount, usd_amount)
    """
    # Get the raw token amount from the event log
    token_amount = log.args.get(token_type, 0)
    if token_amount == 0:
        return 0.0, 0.0

    # Get token metadata (decimals, etc.)
    token_metadata = getattr(pair_per_token_metadata, token_key)

    # Convert from raw amount to normalized amount using token decimals
    native_amount = token_amount / (10 ** int(token_metadata.decimals))

    # Calculate USD value using the price for this block
    token_price_usd = current_token_price_map.get(log.blockNumber, 0)
    usd_amount = native_amount * token_price_usd

    return native_amount, usd_amount


def extract_trade_volume_log(
    event_name: str,
    log: UniswapEvent,
    pair_per_token_metadata: UniswapPoolMetadata,
    token0_price_map: Dict[int, float],
    token1_price_map: Dict[int, float],
    block_details_dict: Dict[int, Dict[str, Any]],
) -> Tuple[TradeData, UniswapProcessedLog]:
    """
    Extract trade volume and fee information from a single event log.

    Args:
        event_name (str): The name of the event ('Swap', 'Mint', or 'Burn').
        log (UniswapEvent): The event log data as a Pydantic model.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.
        token0_price_map (Dict[int, float]): Price map for token0.
        token1_price_map (Dict[int, float]): Price map for token1.
        block_details_dict (Dict[int, Dict[str, Any]]): Block details including timestamps.

    Returns:
        Tuple[TradeData, UniswapProcessedLog]: 
            - TradeData object with trade volume and fee information.
            - UniswapProcessedLog model instance with processed log data.
    """
    # Initialize token amounts and USD values
    token0_amount = 0.0
    token1_amount = 0.0
    token0_amount_usd = 0.0
    token1_amount_usd = 0.0

    # Extract amounts based on event type
    if event_name == 'Swap':
        # For Swap events, get absolute values of amounts
        amount0, amount0_usd = token_native_and_usd_amount(
            log=log,
            pair_per_token_metadata=pair_per_token_metadata,
            token_key='token0',
            token_type='amount0',
            current_token_price_map=token0_price_map,
        )
        amount1, amount1_usd = token_native_and_usd_amount(
            log=log,
            pair_per_token_metadata=pair_per_token_metadata,
            token_key='token1',
            token_type='amount1',
            current_token_price_map=token1_price_map,
        )

        token0_amount = abs(amount0)
        token1_amount = abs(amount1)
        token0_amount_usd = abs(amount0_usd)
        token1_amount_usd = abs(amount1_usd)

    elif event_name in ['Mint', 'Burn']:
        # For Mint/Burn events, use amounts as-is (no abs)
        token0_amount, token0_amount_usd = token_native_and_usd_amount(
            log=log,
            pair_per_token_metadata=pair_per_token_metadata,
            token_key='token0',
            token_type='amount0',
            current_token_price_map=token0_price_map,
        )
        token1_amount, token1_amount_usd = token_native_and_usd_amount(
            log=log,
            pair_per_token_metadata=pair_per_token_metadata,
            token_key='token1',
            token_type='amount1',
            current_token_price_map=token1_price_map,
        )

    # Calculate fee and get timestamp
    trade_volume_usd = 0.0
    trade_fee_usd = 0.0
    fee_rate = int(pair_per_token_metadata.fee) / UNISWAPV3_FEE_DIV
    block_details = block_details_dict.get(log.blockNumber, {})
    current_timestamp = block_details.get('timestamp', None)

    # Create trade_data object based on event type
    if event_name == 'Swap':
        # Calculate trade volume as the higher of the two USD amounts
        if token1_amount_usd and token0_amount_usd:
            trade_volume_usd = max(token1_amount_usd, token0_amount_usd)
        else:
            trade_volume_usd = token1_amount_usd or token0_amount_usd

        # Calculate trading fee (fee is taken from the token that was removed from the pool)
        trade_fee_usd = (
            token1_amount_usd * fee_rate if token1_amount_usd
            else token0_amount_usd * fee_rate
        )

        trade_data_obj = TradeData(
            totalTradesUSD=trade_volume_usd,
            totalTradesMintBurnUSD=0,
            totalFeeUSD=trade_fee_usd,
            token0TradeVolume=token0_amount,
            token1TradeVolume=token1_amount,
            token0TradeVolumeUSD=token0_amount_usd,
            token1TradeVolumeUSD=token1_amount_usd,
        )

    else:  # Mint or Burn
        # For Mint/Burn, combine both token amounts for total volume
        trade_volume_usd = token0_amount_usd + token1_amount_usd

        trade_data_obj = TradeData(
            totalTradesUSD=0,
            totalTradesMintBurnUSD=trade_volume_usd,
            totalFeeUSD=0,
            token0TradeVolume=0,
            token1TradeVolume=0,
            token0TradeVolumeUSD=0,
            token1TradeVolumeUSD=0,
            token0MintBurnVolume=token0_amount,
            token1MintBurnVolume=token1_amount,
            token0MintBurnVolumeUSD=token0_amount_usd,
            token1MintBurnVolumeUSD=token1_amount_usd,
        )

    # Create UniswapProcessedLog instance for this event
    processed_log_data = log.model_dump(by_alias=True)
    processed_log = UniswapProcessedLog(
        **processed_log_data,
        token0_amount=token0_amount,
        token1_amount=token1_amount,
        timestamp=current_timestamp,
        trade_amount_usd=trade_volume_usd
    )

    return trade_data_obj, processed_log




# asynchronously get trades on a pair contract
async def get_pair_trade_volume(
    pair_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_details_dict: dict = dict(),
):
    """
    Fetch and calculate trade volume for a given Uniswap V3 pool contract address over a block range.
    """
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Starting trade volume computation",
        from_block,
        to_block,
        pair_address
    )

    # Ensure consistent address casing for cache lookups and further processing
    pair_address = Web3.to_checksum_address(pair_address)

    # Only fetch block details if not provided
    if not block_details_dict:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block,
                to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                "[Epoch {}-{}] Pool {} | Failed to fetch block details: {}",
                from_block,
                to_block,
                pair_address,
                err
            )
            raise err

        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Block details fetched successfully",
            from_block,
            to_block,
            pair_address
        )

    pair_per_token_metadata = await get_uniswap_v3_pool_metadata(
        pool_address=pair_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )

    if not pair_per_token_metadata:
        core_logger.error(
            "[Epoch {}-{}] Pool {} | Failed to fetch pair metadata",
            from_block,
            to_block,
            pair_address
        )
        raise Exception(f'Error attempting to get pair metadata for: {pair_address}')

    _, _, token0_price_map, token1_price_map = await get_token_price_in_usd_in_block_range(
        pair_metadata=pair_per_token_metadata,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        rpc_helper=rpc_helper,
    )

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Token prices fetched successfully",
        from_block,
        to_block,
        pair_address
    )

    events_by_block = await get_events_from_cache(
        pool_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Found {} events_by_block entries to process",
        from_block,
        to_block,
        pair_address,
        len(events_by_block)
    )

    # Process events and calculate trade volumes
    processed_trades_list: List[UniswapProcessedLog] = []
    total_events_attempted = 0
    for block_num, events_in_block in events_by_block.items(): 
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Block {} | Found {} events in this block to attempt processing.",
            from_block, to_block, pair_address, block_num, len(events_in_block)
        )
        total_events_attempted += len(events_in_block)
        for event_to_process in events_in_block: 
            core_logger.info(
                "[Epoch {}-{}] Pool {} | Block {} | Processing event: {}",
                from_block, to_block, pair_address, block_num, event_to_process.model_dump_json(indent=2)
            )
            try:
                returned_trade_data, processed_log_event_candidate = extract_trade_volume_log(
                    event_name=event_to_process.eventName,
                    log=event_to_process,
                    pair_per_token_metadata=pair_per_token_metadata,
                    token0_price_map=token0_price_map,
                    token1_price_map=token1_price_map,
                    block_details_dict=block_details_dict,
                )
                core_logger.info(
                    "[Epoch {}-{}] Pool {} | Block {} | extract_trade_volume_log returned: trade_data={}, processed_log_event={}",
                    from_block, to_block, pair_address, block_num,
                    returned_trade_data.model_dump_json(indent=2) if returned_trade_data else "None",
                    processed_log_event_candidate.model_dump_json(indent=2) if processed_log_event_candidate else "None"
                )

                if processed_log_event_candidate:
                    processed_trades_list.append(processed_log_event_candidate)
                else:
                    core_logger.warning(
                        "[Epoch {}-{}] Pool {} | Block {} | Event {} with name '{}' processed by extract_trade_volume_log but resulted in no UniswapProcessedLog object (was None). Skipping.",
                        from_block, to_block, pair_address, block_num, event_to_process.txHash, event_to_process.eventName
                    )
            except Exception as e_extract:
                core_logger.opt(exception=True).error(
                    "[Epoch {}-{}] Pool {} | Block {} | Exception during extract_trade_volume_log for event {} with name '{}': {}",
                    from_block, to_block, pair_address, block_num, event_to_process.txHash, event_to_process.eventName, e_extract
                )

    # More precise initial log based on total events found across all blocks in events_by_block
    if total_events_attempted > 0 and not processed_trades_list:
        core_logger.warning(
            "[Epoch {}-{}] Pool {} | Attempted to process {} events from cache, but none resulted in a UniswapProcessedLog.",
            from_block, to_block, pair_address, total_events_attempted
        )

    core_logger.info(
        "[Epoch {}-{}] Pool {} | Trade volume computation completed | Processed {} trades (from {} events attempted in cache)",
        from_block,
        to_block,
        pair_address,
        len(processed_trades_list),
        total_events_attempted
    )

    return {
        "address": pair_address,
        "epoch": {"begin": from_block, "end": to_block},
        "trades": processed_trades_list,
    }