import asyncio
import json
from pydantic import BaseModel
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from typing import List, Optional, Tuple, Type, Dict, Any
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.utils.redis_keys import uniswap_eth_usd_price_zset
from computes.settings.config import settings as computes_settings
from computes.utils.models.message_models import UniswapBaseSnapshot, UniswapTradesSnapshot, TradeType, AllUniswapTradesSnapshot
from computes.api.models.data_models import (
    UniswapPoolMetadata, 
    UniswapTokenPoolsSnapshot, 
    UniswapEthPriceSnapshot, 
)
from snapshotter.utils.data_utils import (
    get_project_latest_snapshot,
    get_project_last_finalized_epoch,
    get_project_epoch_snapshot,
    get_last_submitted_snapshot_data,
    get_submission_data,
    get_current_epoch_id,
    get_tail_epoch_id,
    get_project_epoch_snapshot_bulk,
    get_source_chain_epoch_size,
    _fetch_snapshots_for_epochs,
    _fetch_missing_timestamps,
    _fallback_fetch_block_at_timestamp,
    get_block_number_closest_to_timestamp,
    get_source_chain_block_time,
)
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import block_number_to_timestamp_key
from snapshotter.utils.models.data_models import BlockSearchType
from computes.utils.constants import ERC20_ABI
from computes.utils.constants import POOL_ABI
from typing import Union
from eth_typing import Address

logger = default_logger.bind(module='uniswap_v3_api_data_utils')
WETH = Web3.to_checksum_address(computes_settings.contract_addresses.WETH)


async def get_pool_metadata(
    pool_address: Union[str, Address],
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
) -> Optional[Dict[str, Any]]:
    """Get comprehensive metadata from a UniswapV3Pool.

    Args:
        pool_address: The address of the UniswapV3Pool contract

    Returns:
        Dict containing pool metadata or None if not a valid pool
    """
    try:
        pool_address = Web3.to_checksum_address(pool_address)

        # Check cache first
        cache_key = f'pool_metadata:{pool_address}'
        cached_data = await redis_conn.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        # Create pool contract instance
        current_node = rpc_helper.get_current_node()
        pool_contract = current_node['web3_client'].eth.contract(address=pool_address, abi=POOL_ABI)

        # Get basic pool data
        try:
            token0_address = await pool_contract.functions.token0().call()
            token1_address = await pool_contract.functions.token1().call()
            factory_address = await pool_contract.functions.factory().call()
            fee = await pool_contract.functions.fee().call()
            tick_spacing = await pool_contract.functions.tickSpacing().call()
        except Exception as e:
            logger.error(f"Failed to get basic pool data for {pool_address}: {str(e)}")
            return None

        # Normalize addresses
        token0_address = Web3.to_checksum_address(token0_address)
        token1_address = Web3.to_checksum_address(token1_address)
        factory_address = Web3.to_checksum_address(factory_address)

        # Get token metadata
        token0_metadata = await _get_erc20_metadata(token0_address, redis_conn, rpc_helper)
        token1_metadata = await _get_erc20_metadata(token1_address, redis_conn, rpc_helper)

        if not token0_metadata or not token1_metadata:
            logger.error(f"Failed to get token metadata for pool {pool_address}")
            return None

        # Build metadata
        pool_metadata = {
            'address': pool_address,
            'token0': {
                'address': token0_address,
                **token0_metadata
            },
            'token1': {
                'address': token1_address,
                **token1_metadata
            },
            'fee': fee,
            'tick_spacing': tick_spacing,
            'factory': factory_address
        }

        # Cache the result
        await redis_conn.set(cache_key, json.dumps(pool_metadata))
        return pool_metadata

    except Exception as e:
        logger.error(f"Error getting pool metadata for {pool_address}: {str(e)}")
        return None


async def _get_erc20_metadata(
    token_address: Union[str, Address], 
    redis_conn: aioredis.Redis, 
    rpc_helper: RpcHelper,
) -> Optional[Dict[str, Any]]:
    """Get ERC20 token metadata with robust error handling."""
    try:
        token_address = Web3.to_checksum_address(token_address)

        # Check cache first
        cache_key = f'erc20_metadata:{token_address}'
        cached_data = await redis_conn.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        # Create token contract
        current_node = rpc_helper.get_current_node()
        token_contract = current_node['web3_client'].eth.contract(address=token_address, abi=ERC20_ABI)

        # Get metadata with fallbacks
        try:
            name = await token_contract.functions.name().call()
        except Exception:
            name = "Unknown Token"

        try:
            symbol = await token_contract.functions.symbol().call()
        except Exception:
            symbol = "UNKNOWN"

        try:
            decimals = await token_contract.functions.decimals().call()
        except Exception:
            decimals = 18  # Default to 18 decimals

        metadata = {
            'name': name,
            'symbol': symbol,
            'decimals': decimals
        }

        # Cache for 24 hours
        await redis_conn.set(cache_key, json.dumps(metadata))
        return metadata

    except Exception as e:
        logger.error(f"Error getting ERC20 metadata for {token_address}: {str(e)}")
        return None


async def get_uniswap_v3_pool_metadata(
        pool_address: str, 
        redis_conn: aioredis.Redis, 
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        
    ) -> Optional[UniswapPoolMetadata]:
    """
    Retrieves metadata for a Uniswap V3 pool from the snapshotter system.
    
    This function first checks the Redis cache for existing pool metadata. If not found,
    it fetches the latest snapshot data for the pool from the protocol state and 
    constructs the metadata object.
    
    Args:
        pool_address (str): The Ethereum address of the Uniswap V3 pool
        redis_conn (aioredis.Redis): Redis connection for caching
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        
    Returns:
        Optional[UniswapPoolMetadata]: Pool metadata object containing token information,
                                     decimals, symbols, etc. Returns None if metadata 
                                     cannot be retrieved.
                                     
    Raises:
        Exception: If there's an error fetching the latest snapshot data
    """
    # Check redis cache first for existing metadata
    project_id: str = 'metadata:{poolAddress}:{Namespace}'
    
    cache_key = f'pool_metadata:{pool_address}'
    cached_data = await redis_conn.get(cache_key)
    
    if cached_data:
        logger.info(f"Found cached metadata for pool {pool_address}")
        return UniswapPoolMetadata(**json.loads(cached_data))

    # If not cached, fetch from latest snapshot
    try:
        latest_snapshot = await get_project_latest_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id.format(poolAddress=pool_address, Namespace=settings.namespace)
        )
        if not latest_snapshot:
            pool_metadata_raw = await get_pool_metadata(pool_address, redis_conn, anchor_rpc_helper)
            if not pool_metadata_raw:
                logger.error(f"Unable to get pool metadata for pool {pool_address}")
                return None
            return UniswapPoolMetadata(**pool_metadata_raw)
        else:
            logger.info(f"Found cached metadata for pool {pool_address}")
            return UniswapPoolMetadata(**latest_snapshot)
    except Exception as e:
        logger.opt(exception=e).error(f"Error getting latest snapshot for pool {pool_address} while processing metadata")
        return None


async def get_uniswapv3_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    project_id: str,
    message_model: Type[BaseModel],
    block_number: Optional[int] = None,
) -> Optional[Tuple[int, BaseModel]]:
    """
    Retrieves a Uniswap V3 snapshot for a given project and optionally a specific block number.
    
    This function handles the logic of determining the target epoch based on whether a block_number
    is provided or not. If no block_number is given, it uses the last finalized epoch. If a 
    block_number is provided, it seeks around that epoch to find the closest available data.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions  
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        project_id (str): The project identifier for the snapshot
        message_model (Type[BaseModel]): Pydantic model class to parse the snapshot data
        block_number (Optional[int]): Specific block number to target, if None uses latest
        
    Returns:
        Optional[Tuple[int, BaseModel]]: Tuple of (epoch_id, parsed_snapshot) if found,
                                        None if no valid snapshot data is available
                                        
    Note:
        When block_number is provided, the function assumes epoch equals block number
        in the data market contract configuration.
    """
    # Determine target epoch based on input parameters
    seek = False
    if not block_number:
        # Use last submitted or finalized epoch when no specific block requested
        last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
        if last_submitted_snapshot_data:
            target_epoch = last_submitted_snapshot_data['epochId']
        else:
            target_epoch = await get_project_last_finalized_epoch(
                redis_conn=redis_conn,
                state_contract_obj=protocol_state_contract,
                rpc_helper=anchor_rpc_helper,
                project_id=project_id,
            )

        if not target_epoch:
            logger.error(f"No last finalized epoch found for project {project_id}")
            return None
        else:
            logger.info(f"Using epoch {target_epoch} for fetch against project {project_id}")
    # if block_number is provided, use that as the target epoch and seek around it if needed
    else:
        # TODO: assumes epoch is set to block number in data market contract, may need to add config flag for this and derive epoch from block number if false
        target_epoch = block_number
        seek = True
        
    # Fetch snapshot data for the determined epoch
    snapshot_response = await get_project_epoch_snapshot(
        redis_conn=redis_conn,
        state_contract_obj=protocol_state_contract,
        rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        epoch_id=target_epoch,
        project_id=project_id,
        seek=seek
    )
    
    # Process exact match response
    if snapshot_response.exact_match:
        try:
            parsed_snapshot = message_model(**snapshot_response.exact_match.data)
            return target_epoch, parsed_snapshot
        except Exception as e:
            logger.error(f"Failed to parse snapshot data for project {project_id} against epoch {target_epoch}: {e}")
            return None
    else:
        # Handle case when exact match not found but nearby epochs available
        if snapshot_response.has_closest_epochs:
            logger.info(f"No exact match found for project {project_id} against epoch {target_epoch}, but nearby epochs found: {snapshot_response.closest_epochs}") 
            
            # Try previous epoch first
            previous_epoch = snapshot_response.closest_epochs.previous        
            if previous_epoch:
                logger.info(f"Fetching previous epoch {previous_epoch} CID for project {project_id} against actual sought epoch {target_epoch}")
                target_epoch = previous_epoch.epoch_id
                snapshot_response = await get_submission_data(
                    cid=previous_epoch.snapshot_cid,
                    ipfs_reader=ipfs_reader,
                )
                if snapshot_response:
                    parsed_snapshot = message_model(**snapshot_response)
                    return target_epoch, parsed_snapshot
                else:
                    logger.error(f"No snapshot data found for project {project_id} against nearby epoch {previous_epoch.epoch_id} with CID {previous_epoch.snapshot_cid}")
                    return None
                    
            # Fallback to next epoch if previous not available        
            next_epoch = snapshot_response.closest_epochs.next
            if next_epoch:
                logger.info(f"Fetching next epoch {next_epoch} CID for project {project_id} against actual sought epoch {target_epoch}")
                target_epoch = next_epoch.epoch_id
                snapshot_response = await get_submission_data(
                    cid=next_epoch.snapshot_cid,
                    ipfs_reader=ipfs_reader,
                )
                if snapshot_response:
                    parsed_snapshot = message_model(**snapshot_response)
                    return target_epoch, parsed_snapshot
                else:
                    logger.error(f"No snapshot data found for project {project_id} against nearby epoch {next_epoch.epoch_id} with CID {next_epoch.snapshot_cid}")
                    return None
        return None


async def get_uniswap_v3_token_pools_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
):
    """
    Retrieves the token pools snapshot for a specific token on Uniswap V3.
    
    This function fetches snapshot data that contains information about all pools
    that include the specified token address.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token to get pools for
        
    Returns:
        Optional[UniswapTokenPoolsSnapshot]: Snapshot containing pool information
                                           for the token, or None if not found
    """
    token_address = Web3.to_checksum_address(token_address)

    project_id = f"tokenPools:{token_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapTokenPoolsSnapshot,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_base_snapshots_for_token(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,

):
    """
    Retrieves base snapshots for all pools containing a specific token.
    
    This function first gets the list of pools for a token, then fetches
    the base snapshot data for each of those pools.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token
        
    Returns:
        Optional[Dict[str, UniswapBaseSnapshot]]: Dictionary mapping pool addresses
                                                to their base snapshots, or None if
                                                no pools found for the token
    """
    token_pools = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )
    if not token_pools:
        logger.error(f"No token pools found for token {token_address}")
        return None
    
    if len(token_pools.pools) > 20:
        raise Exception(f"Too many pools found for token {token_address}, this is not supported yet")
    
    data = {}
    for pool in token_pools.pools:
        base_snapshot = await get_uniswap_v3_base_snapshot(
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            pool_address=pool,
        )
        if not base_snapshot:
            logger.error(f"No base snapshot found for pool {pool}")
            continue
        data[pool] = base_snapshot
    return data


async def get_uniswap_v3_base_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the base snapshot for a specific Uniswap V3 pool.
    
    Base snapshots contain fundamental pool data including token information,
    liquidity, pricing data, and other core metrics.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        pool_address (str): Ethereum address of the pool
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapBaseSnapshot]: Base snapshot data for the pool,
                                     or None if not found
    """
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    return snapshot_data


async def get_uniswap_v3_trades_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the trades snapshot for a specific Uniswap V3 pool.
    
    Trade snapshots contain information about individual trades/swaps
    that occurred in the pool during the snapshot period.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        pool_address (str): Ethereum address of the pool
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapTradesSnapshot]: Trades snapshot data for the pool,
                                       or None if not found
    """
    project_id = f"tradesSnapshot:{pool_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapTradesSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    return snapshot_data


async def get_uniswap_v3_all_trades_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_number: Optional[int] = None,
):
    """
    Retrieves the trades snapshot for all Uniswap V3 pools.
    
    Trade snapshots contain information about individual trades/swaps
    that occurred in the pool during the snapshot period.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        pool_address (str): Ethereum address of the pool
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapTradesSnapshot]: Trades snapshot data for the pool,
                                       or None if not found
    """
    project_id = f"allTradesSnapshot:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=AllUniswapTradesSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    return snapshot_data


async def get_uniswap_v3_eth_price_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_number: Optional[int] = None,
):
    """
    Retrieves the ETH price snapshot from the Uniswap V3 ecosystem.
    
    This function fetches the latest ETH price data as captured by the
    snapshotter system from various Uniswap V3 pools.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapEthPriceSnapshot]: ETH price snapshot data,
                                         or None if not found
    """
    project_id = f'price:ETH:{settings.namespace}'
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapEthPriceSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_token_price_pool(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the USD price of a specific token from a specific pool.
    
    This function fetches the base snapshot for a pool and extracts the USD price
    for the specified token. It determines if the token is token0 or token1 in
    the pool and returns the appropriate price data.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token to get price for
        pool_address (str): Ethereum address of the pool to get price from
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[float]: USD price of the token in the specified pool,
                        or None if token not found in pool or data unavailable
                        
    Note:
        Assumes snapshot_epoch corresponds to the block number when accessing
        price data from the snapshot.
    """
    base_project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"

    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=base_project_id,
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {base_project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if not snapshot_data:
        logger.error(f"No base snapshot data found for project {base_project_id} against epoch {snapshot_epoch}")
        return None

    # Determine which token in the pair and extract its price
    if Web3.to_checksum_address(token_address) == snapshot_data.token0:
        # NOTE: assumes snapshot_epoch is the block number
        token_price = snapshot_data.token0PricesUSD[snapshot_epoch]
    elif Web3.to_checksum_address(token_address) == snapshot_data.token1:
        token_price = snapshot_data.token1PricesUSD[snapshot_epoch]
    else:
        logger.error(f"Token address {token_address} not found in base snapshot data for project {base_project_id} against epoch {snapshot_epoch}")
        return None
    
    return token_price


async def get_uniswap_v3_token_prices_all_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
    block_number: Optional[int] = None,
):
    """
    Get token prices from all pools for a given token address.
    Uses batch processing with asyncio tasks to fetch prices concurrently.
    Returns a dict mapping pool addresses to their respective token prices.
    """
    logger.info(f"Getting pool addresses for token {token_address}")
    token_pools_snapshot_result = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )
    if not token_pools_snapshot_result:
        logger.error(f"No token pools snapshot found for token {token_address}")
        return None
    
    if not token_pools_snapshot_result or not token_pools_snapshot_result.pools:
        logger.error(f"No token pools snapshot data found for token {token_address}")
        return None

    if len(token_pools_snapshot_result.pools) > 20:
        raise Exception(f"Too many pools found for token {token_address}, this is not supported yet")
    
    logger.info(f"Token pools snapshot result against token {token_address}: {token_pools_snapshot_result}")
    # Get list of pool addresses
    pool_addresses = list(token_pools_snapshot_result.pools.keys())
    if not pool_addresses:
        logger.error(f"No pools found for token {token_address}")
        return None

    # Process pools in batches of 20
    BATCH_SIZE = 20
    results = {}
    
    for i in range(0, len(pool_addresses), BATCH_SIZE):
        batch_pools = pool_addresses[i:i + BATCH_SIZE]
        logger.info(f"Processing batch of {len(batch_pools)} pools against token {token_address} for prices: {batch_pools}")
        # Create tasks for each pool in the batch
        tasks = [
            get_uniswap_v3_token_price_pool(
                redis_conn=redis_conn,
                anchor_rpc_helper=anchor_rpc_helper,
                ipfs_reader=ipfs_reader,
                protocol_state_contract=protocol_state_contract,
                token_address=token_address,
                pool_address=pool_address,
                block_number=block_number,
            )
            for pool_address in batch_pools
        ]
        
        try:
            # Execute batch of tasks concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for pool_address, result in zip(batch_pools, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error getting price for pool {pool_address}: {str(result)}")
                    results[pool_address] = None
                else:
                    results[pool_address] = result
                    
        except Exception as e:
            logger.error(f"Error processing batch of pools: {str(e)}")
            # Continue with next batch even if current batch fails
    
    return results


async def get_uniswap_trade_volume_agg(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    project_id: str,
):
    """
    Calculates aggregated trade volume for a project over a specified time interval.
    
    This function uses intelligent caching to minimize data fetching and provides
    incremental updates when possible for optimal performance.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to aggregate over
        project_id (str): Project identifier for the data
        
    Returns:
        Dict[str, Union[int, float]]: Dictionary containing totalTradeVolume
                                    and timeInterval values
                                    
    Raises:
        ValueError: If input parameters are invalid
        Exception: If critical blockchain or data fetch operations fail
    """
    # Input validation
    if time_interval <= 0:
        raise ValueError(f"Invalid time_interval: {time_interval}. Must be > 0")
    if not project_id:
        raise ValueError("Invalid project_id: cannot be empty")
    
    # Check last indexed epoch
    last_indexed_epoch = await redis_conn.get(
        f"trade_volume_data:{project_id}:{time_interval}:latest:epoch"
    )
    if last_indexed_epoch:
        last_indexed_epoch = int(last_indexed_epoch)
    else:
        last_indexed_epoch = 0
    
    try:
        
        current_epoch = await get_current_epoch_id(
            anchor_rpc_helper, protocol_state_contract
        )
        tail_epoch_id, _ = await get_tail_epoch_id(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            current_epoch, time_interval, project_id
        )
    except Exception as e:
        logger.error(f"Failed to get epoch information for project {project_id}: {e}")
        raise Exception(f"Cannot determine epoch range for project {project_id}: {e}")

    logger.info(
        f"Trade volume aggregation - Project: {project_id}, "
        f"Last indexed epoch: {last_indexed_epoch}, "
        f"tail epoch id: {tail_epoch_id}, current epoch: {current_epoch}"
    )

    total_trade_volume = 0.0

    if last_indexed_epoch > tail_epoch_id:
        epochs_to_correct = current_epoch - last_indexed_epoch
        # Fetch cached volume data
        logger.info(
            f"Using cached data with correction for project {project_id}, "
            f"epochs {last_indexed_epoch} to {current_epoch} "
            f"for time interval {time_interval}"
        )
        cached_volume = await redis_conn.get(
            f"trade_volume_data:{project_id}:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )
        if cached_volume:
            total_trade_volume = float(cached_volume)
            # Apply incremental updates if needed
            if epochs_to_correct > 0:
                logger.info(
                    f"Applying incremental updates for project {project_id}, "
                    f"fetching {epochs_to_correct} new epochs and removing old ones"
                )
                
                # Fetch new snapshots to add
                new_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, last_indexed_epoch + 1, current_epoch, project_id
                )
                
                # Fetch old snapshots to remove
                old_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, tail_epoch_id - epochs_to_correct, 
                    tail_epoch_id - 1, project_id
                )
                
                # Add volume from new snapshots
                for snapshot in new_snapshots:
                    if snapshot and 'totalTrade' in snapshot:
                        volume = snapshot['totalTrade']
                        if isinstance(volume, (int, float)) and volume > 0:
                            total_trade_volume += volume
                
                # Subtract volume from old snapshots
                for snapshot in old_snapshots:
                    if snapshot and 'totalTrade' in snapshot:
                        volume = snapshot['totalTrade']
                        if isinstance(volume, (int, float)) and volume > 0:
                            total_trade_volume -= volume
                
                # Ensure volume doesn't go negative due to data inconsistencies
                total_trade_volume = max(0.0, total_trade_volume)
        else:
            # No cached data found, fall back to full calculation
            logger.info(
                f"No cached data found for project {project_id}, "
                f"calculating full volume from {tail_epoch_id} to {current_epoch}"
            )
            snapshots = await get_project_epoch_snapshot_bulk(
                redis_conn, protocol_state_contract, anchor_rpc_helper, 
                ipfs_reader, tail_epoch_id, current_epoch, project_id
            )
            for snapshot in snapshots:
                if snapshot and 'totalTrade' in snapshot:
                    volume = snapshot['totalTrade']
                    if isinstance(volume, (int, float)) and volume > 0:
                        total_trade_volume += volume
    else:
        # Fresh calculation needed
        logger.info(
            f"Performing fresh calculation for project {project_id} "
            f"from {tail_epoch_id} to {current_epoch}"
        )
        snapshots = await get_project_epoch_snapshot_bulk(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            ipfs_reader, tail_epoch_id, current_epoch, project_id
        )
        for snapshot in snapshots:
            if snapshot and 'totalTrade' in snapshot:
                volume = snapshot['totalTrade']
                if isinstance(volume, (int, float)) and volume > 0:
                    total_trade_volume += volume
    pipeline = redis_conn.pipeline()
    # Set data in redis (same pattern as active pools/tokens)
    pipeline.set(
        f"trade_volume_data:{project_id}:{time_interval}:{current_epoch}:{settings.namespace}", 
        str(total_trade_volume)
    ).set(
        f"trade_volume_data:{project_id}:{time_interval}:latest:epoch", current_epoch
    )
    # Remove old data
    if last_indexed_epoch > 0 and last_indexed_epoch != current_epoch:
        pipeline.delete(
            f"trade_volume_data:{project_id}:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )
    await pipeline.execute()
    
    return {
        'totalTradeVolume': total_trade_volume,
        'timeInterval': time_interval,
    }


async def get_active_pools(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    page: int,
    size: int,
    metadata: bool,
):
    """
    Retrieves the most active pools over a specified time interval.
    
    This function aggregates pool activity data from snapshots and returns
    pools sorted by their activity frequency.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to analyze
        page (int): Page number for pagination (1-based)
        size (int): Number of items per page
        metadata (bool): Whether to include pool metadata
        
    Returns:
        Tuple[List[Dict], int]: List of pool data with pagination info and total count
    """
    # Input validation
    if time_interval <= 0:
        raise ValueError(f"Invalid time_interval: {time_interval}. Must be > 0")
    if page <= 0:
        raise ValueError(f"Invalid page: {page}. Must be > 0")
    if size <= 0:
        raise ValueError(f"Invalid size: {size}. Must be > 0")
    
    # Check last indexed epoch
    last_indexed_epoch = await redis_conn.get(
        f"active_pool_data:{time_interval}:latest:epoch"
    )
    if last_indexed_epoch:
        last_indexed_epoch = int(last_indexed_epoch)
    else:
        last_indexed_epoch = 0
    
    project_id = f"activePools:{settings.namespace}"
    
    try:
        last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
        if last_submitted_snapshot_data:
            last_submitted_epoch = last_submitted_snapshot_data['epochId']
        else:
            last_submitted_epoch = await get_project_last_finalized_epoch(
                redis_conn=redis_conn,
                state_contract_obj=protocol_state_contract,
                rpc_helper=anchor_rpc_helper,
                project_id=project_id,
            )
        tail_epoch_id, _ = await get_tail_epoch_id(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            last_submitted_epoch, time_interval, project_id
        )
    except Exception as e:
        logger.error(f"Failed to get epoch information: {e}")
        raise Exception(f"Cannot determine epoch range: {e}")

    logger.info(
        f"Last indexed epoch: {last_indexed_epoch}, "
        f"tail epoch id: {tail_epoch_id}, current epoch: {last_submitted_epoch}"
    )
        
    if last_indexed_epoch > tail_epoch_id:
        epochs_to_correct = last_submitted_epoch - last_indexed_epoch
        # Fetch indexed data
        logger.info(
            f"Correcting indexed data for epochs {last_indexed_epoch} "
            f"to {last_submitted_epoch} for time interval {time_interval}"
        )
        active_pools_cached = await redis_conn.get(
            f"active_pool_data:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )
        if active_pools_cached:
            active_pools = json.loads(active_pools_cached)
            # Fetch snapshots for epochs_to_correct
            if epochs_to_correct > 0:
                logger.info(
                    f"Fetching new snapshots for epochs {last_indexed_epoch} "
                    f"to {last_indexed_epoch + epochs_to_correct}"
                )
                new_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, last_indexed_epoch + 1, last_submitted_epoch, project_id
                )
                logger.info(
                    f"Fetching old snapshots for epochs "
                    f"{tail_epoch_id - epochs_to_correct} to {tail_epoch_id}"
                )
                old_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, tail_epoch_id - epochs_to_correct, 
                    tail_epoch_id - 1, project_id
                )
                
                # Add new snapshots to indexed data
                for snapshot in new_snapshots:
                    if snapshot and 'pools' in snapshot:
                        for pool_address, frequency in snapshot['pools'].items():
                            if pool_address not in active_pools:
                                active_pools[pool_address] = 0
                            active_pools[pool_address] += frequency

                # Remove old snapshots from indexed data
                for snapshot in old_snapshots:
                    if snapshot and 'pools' in snapshot:
                        for pool_address, frequency in snapshot['pools'].items():
                            if pool_address in active_pools:
                                active_pools[pool_address] -= frequency
                                # Remove pools with zero or negative frequency
                                if active_pools[pool_address] <= 0:
                                    del active_pools[pool_address]
        else:
            # No cached data found, fall back to fetching all snapshots
            logger.info(
                f"No cached data found, fetching all snapshots "
                f"from {tail_epoch_id} to {last_submitted_epoch}"
            )
            snapshots = await get_project_epoch_snapshot_bulk(
                redis_conn, protocol_state_contract, anchor_rpc_helper, 
                ipfs_reader, tail_epoch_id, last_submitted_epoch, project_id
            )
            active_pools = {}
            for snapshot in snapshots:
                if snapshot and 'pools' in snapshot:
                    for pool_address, frequency in snapshot['pools'].items():
                        if pool_address not in active_pools:
                            active_pools[pool_address] = 0
                        active_pools[pool_address] += frequency
    else:
        snapshots = await get_project_epoch_snapshot_bulk(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            ipfs_reader, tail_epoch_id, last_submitted_epoch, project_id
        )
        active_pools = {}
        for snapshot in snapshots:
            if snapshot and 'pools' in snapshot:
                for pool_address, frequency in snapshot['pools'].items():
                    if pool_address not in active_pools:
                        active_pools[pool_address] = 0
                    active_pools[pool_address] += frequency

    # Set data in redis
    await redis_conn.set(
        f"active_pool_data:{time_interval}:{last_submitted_epoch}:{settings.namespace}", 
        json.dumps(active_pools),
        ex=3600
    )
    await redis_conn.set(
        f"active_pool_data:{time_interval}:latest:epoch", last_submitted_epoch,
        ex=3600
    )
    # Remove old data
    if last_indexed_epoch > 0 and last_indexed_epoch != last_submitted_epoch:
        await redis_conn.delete(
            f"active_pool_data:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )

    active_pool_data = [
        (pool_address, frequency) 
        for pool_address, frequency in active_pools.items()
    ]
    active_pool_data.sort(key=lambda x: x[1], reverse=True)

    # Calculate start and end indices for pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size - 1

    total_pools = len(active_pool_data)
    active_pools_page = active_pool_data[start_idx:end_idx + 1]
    
    # Format the response
    pools_data = []
    for pool, score in active_pools_page:
        pool_data = {
            "pool_address": pool,
            "frequency": score
        }
        pools_data.append(pool_data)
    
    # Add metadata if requested (parallelized in batches)
    if metadata:
        # Process pools in batches of 50
        batch_size = 50
        for i in range(0, len(pools_data), batch_size):
            batch = pools_data[i:i + batch_size]
            
            # Create metadata fetch tasks for this batch
            metadata_tasks = []
            for pool_data in batch:
                task = get_uniswap_v3_pool_metadata(
                    redis_conn=redis_conn,
                    protocol_state_contract=protocol_state_contract,
                    anchor_rpc_helper=anchor_rpc_helper,
                    ipfs_reader=ipfs_reader,
                    pool_address=Web3.to_checksum_address(pool_data["pool_address"]),
                )
                metadata_tasks.append(task)
            
            # Fetch metadata for all pools in this batch in parallel
            try:
                metadata_results = await asyncio.gather(
                    *metadata_tasks, return_exceptions=True
                )
                
                # Assign metadata results back to pool data
                for j, metadata_result in enumerate(metadata_results):
                    if isinstance(metadata_result, Exception):
                        logger.error(
                            f"Exception fetching metadata for pool "
                            f"{batch[j]['pool_address']}: {metadata_result}"
                        )
                        batch[j]["metadata"] = None
                    else:
                        batch[j]["metadata"] = metadata_result
            except Exception as e:
                logger.error(f"Error in batch metadata fetch: {e}")
                # Set metadata to None for all pools in this batch
                for pool_data in batch:
                    pool_data["metadata"] = None

    return pools_data, total_pools


async def get_token_metadata(
    redis_conn: aioredis.Redis,
    protocol_state_contract,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    token_address: str,
):
    """
    Get token metadata from cache, Redis, or IPFS with LRU caching.
    
    This function implements a three-tier caching strategy:
    1. In-memory LRU cache (fastest)
    2. Redis cache (fast)
    3. IPFS fetch (slowest, but most comprehensive)
    """
    # Normalize token address for consistent caching
    token_address = Web3.to_checksum_address(token_address)
    
    # Check if data is in redis
    token_metadata = await redis_conn.get(f'erc20_metadata:{token_address}')
    if token_metadata:
        parsed_metadata = json.loads(token_metadata)
        return parsed_metadata
    else:
        # Fetch from ipfs
        token_metadata = await get_uniswap_v3_token_pools_snapshot(
            redis_conn=redis_conn,
            protocol_state_contract=protocol_state_contract,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            token_address=token_address,
        )
        if token_metadata:
            token_pool_metadata = next(iter(token_metadata.pools.values()))
            if token_pool_metadata.token0.address == token_address:
                token_metadata = token_pool_metadata.token0
            elif token_pool_metadata.token1.address == token_address:
                token_metadata = token_pool_metadata.token1
            
        else:
            token_metadata = None

        if token_metadata:
            metadata_dict = token_metadata.__dict__
            # Cache in redis
            await redis_conn.set(
                f'erc20_metadata:{token_address}', 
                json.dumps(metadata_dict), 
                ex=86400
            )
            return metadata_dict

        return token_metadata
    

async def get_active_tokens(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    page: int,
    size: int,
    metadata: bool,
):
    """
    Retrieves the most active tokens over a specified time interval.
    
    This function aggregates token activity data from snapshots and returns
    tokens sorted by their activity frequency.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to analyze
        page (int): Page number for pagination (1-based)
        size (int): Number of items per page
        metadata (bool): Whether to include token metadata
        
    Returns:
        Tuple[List[Dict], int]: List of token data with pagination info and total count
    """
    # Input validation
    if time_interval <= 0:
        raise ValueError(f"Invalid time_interval: {time_interval}. Must be > 0")
    if page <= 0:
        raise ValueError(f"Invalid page: {page}. Must be > 0")
    if size <= 0:
        raise ValueError(f"Invalid size: {size}. Must be > 0")
    
    # Check last indexed epoch
    last_indexed_epoch = await redis_conn.get(
        f"active_token_data:{time_interval}:latest:epoch"
    )
    if last_indexed_epoch:
        last_indexed_epoch = int(last_indexed_epoch)
    else:
        last_indexed_epoch = 0
    
    project_id = f"activeTokens:{settings.namespace}"
    
    try:
        last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
        if last_submitted_snapshot_data:
            last_submitted_epoch = last_submitted_snapshot_data['epochId']
        else:
            last_submitted_epoch = await get_project_last_finalized_epoch(
                redis_conn=redis_conn,
                state_contract_obj=protocol_state_contract,
                rpc_helper=anchor_rpc_helper,
                project_id=project_id,
            )

        tail_epoch_id, _ = await get_tail_epoch_id(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            last_submitted_epoch, time_interval, project_id
        )
    except Exception as e:
        logger.error(f"Failed to get epoch information: {e}")
        raise Exception(f"Cannot determine epoch range: {e}")

    logger.info(
        f"Last indexed epoch: {last_indexed_epoch}, "
        f"tail epoch id: {tail_epoch_id}, current epoch: {last_submitted_epoch}"
    )

    if last_indexed_epoch > tail_epoch_id:
        epochs_to_correct = last_submitted_epoch - last_indexed_epoch
        # Fetch indexed data
        logger.info(
            f"Correcting indexed data for epochs {last_indexed_epoch} "
            f"to {last_submitted_epoch} for time interval {time_interval}"
        )
        active_tokens_cached = await redis_conn.get(
            f"active_token_data:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )
        if active_tokens_cached:
            active_tokens = json.loads(active_tokens_cached)
            if epochs_to_correct > 0:
                # Fetch snapshots for epochs_to_correct
                logger.info(
                    f"Fetching new snapshots for epochs {last_indexed_epoch} "
                    f"to {last_indexed_epoch + epochs_to_correct}"
                )
                new_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, last_indexed_epoch + 1, last_submitted_epoch, project_id
                )
                logger.info(
                    f"Fetching old snapshots for epochs "
                    f"{tail_epoch_id - epochs_to_correct} to {tail_epoch_id}"
                )
                old_snapshots = await get_project_epoch_snapshot_bulk(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, tail_epoch_id - epochs_to_correct, 
                    tail_epoch_id - 1, project_id
                )
                
                # Add new snapshots to indexed data
                for snapshot in new_snapshots:
                    if snapshot and 'tokens' in snapshot:
                        for token_address, frequency in snapshot['tokens'].items():
                            if token_address not in active_tokens:
                                active_tokens[token_address] = 0
                            active_tokens[token_address] += frequency

                # Remove old snapshots from indexed data
                for snapshot in old_snapshots:
                    if snapshot and 'tokens' in snapshot:
                        for token_address, frequency in snapshot['tokens'].items():
                            if token_address in active_tokens:
                                active_tokens[token_address] -= frequency
                                # Remove tokens with zero or negative frequency
                                if active_tokens[token_address] <= 0:
                                    del active_tokens[token_address]
        else:
            # No cached data found, fall back to fetching all snapshots
            logger.info(
                f"No cached data found, fetching all snapshots "
                f"from {tail_epoch_id} to {last_submitted_epoch}"
            )
            snapshots = await get_project_epoch_snapshot_bulk(
                redis_conn, protocol_state_contract, anchor_rpc_helper, 
                ipfs_reader, tail_epoch_id, last_submitted_epoch, project_id
            )
            active_tokens = {}
            for snapshot in snapshots:
                if snapshot and 'tokens' in snapshot:
                    for token_address, frequency in snapshot['tokens'].items():
                        if token_address not in active_tokens:
                            active_tokens[token_address] = 0
                        active_tokens[token_address] += frequency
    else:    
        snapshots = await get_project_epoch_snapshot_bulk(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            ipfs_reader, tail_epoch_id, last_submitted_epoch, project_id
        )
        active_tokens = {}
        for snapshot in snapshots:
            if snapshot and 'tokens' in snapshot:
                for token_address, frequency in snapshot['tokens'].items():
                    if token_address not in active_tokens:
                        active_tokens[token_address] = 0
                    active_tokens[token_address] += frequency

    # Set data in redis
    await redis_conn.set(
        f"active_token_data:{time_interval}:{last_submitted_epoch}:{settings.namespace}", 
        json.dumps(active_tokens), 
        ex=3600
    )
    await redis_conn.set(
        f"active_token_data:{time_interval}:latest:epoch", last_submitted_epoch,
        ex=3600
    )
    # Remove old data
    if last_indexed_epoch > 0 and last_indexed_epoch != last_submitted_epoch:
        await redis_conn.delete(
            f"active_token_data:{time_interval}:{last_indexed_epoch}:"
            f"{settings.namespace}"
        )

    active_token_data = [
        (token_address, frequency) 
        for token_address, frequency in active_tokens.items()
    ]
    active_token_data.sort(key=lambda x: x[1], reverse=True)

    # Calculate start and end indices for pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size - 1
    
    # Get total count of tokens
    total_tokens = len(active_token_data)
    
    # Get paginated tokens from the sorted set
    active_tokens_page = active_token_data[start_idx:end_idx + 1]
    
    # Format the response
    tokens_data = []
    for token, score in active_tokens_page:
        token_data = {
            "token_address": token,
            "frequency": score
        }
        tokens_data.append(token_data)
    
    # Add metadata if requested (parallelized in batches)
    if metadata:
        # Process tokens in batches of 50
        batch_size = 50
        for i in range(0, len(tokens_data), batch_size):
            batch = tokens_data[i:i + batch_size]
            
            # Create metadata fetch tasks for this batch
            metadata_tasks = []
            for token_data in batch:
                task = get_token_metadata(
                    redis_conn=redis_conn,
                    protocol_state_contract=protocol_state_contract,
                    anchor_rpc_helper=anchor_rpc_helper,
                    ipfs_reader=ipfs_reader,
                    token_address=Web3.to_checksum_address(
                        token_data["token_address"]
                    ),
                )
                metadata_tasks.append(task)
            
            # Fetch metadata for all tokens in this batch in parallel
            try:
                metadata_results = await asyncio.gather(
                    *metadata_tasks, return_exceptions=True
                )
                
                # Assign metadata results back to token data
                for j, metadata_result in enumerate(metadata_results):
                    if isinstance(metadata_result, Exception):
                        logger.error(
                            f"Exception fetching metadata for token "
                            f"{batch[j]['token_address']}: {metadata_result}"
                        )
                        batch[j]["metadata"] = None
                    elif not metadata_result:
                        batch[j]["metadata"] = None
                    else:
                        batch[j]["metadata"] = metadata_result
            except Exception as e:
                logger.error(f"Error in batch metadata fetch: {e}")
                # Set metadata to None for all tokens in this batch
                for token_data in batch:
                    token_data["metadata"] = None

    return tokens_data, total_tokens


async def get_uniswap_trade_volume_agg_all_pools(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    token_address: str,
):
    """
    Calculates aggregated trade volume across all pools containing a specific token.
    
    This function first finds all pools that contain the specified token, then
    aggregates trade volume data from all those pools over the time interval.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to aggregate over
        token_address (str): Ethereum address of the token to analyze
        
    Returns:
        Optional[Dict[str, Union[int, float]]]: Dictionary containing cumulative
                                              totalTradeVolume and timeInterval,
                                              or None if no pools found
    """
    token_address = Web3.to_checksum_address(token_address)
    token_pools = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )

    tasks = []
    if not token_pools:
        logger.error(f"No token pools found for token {token_address}")
        return None

    if len(token_pools.pools) > 20:
        raise Exception(f"Too many pools found for token {token_address}, this is not supported yet")
    
    for pool in token_pools.pools:
        tasks.append(get_uniswap_trade_volume_agg(
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            time_interval=time_interval,
            project_id=f"baseSnapshot:{pool}:{settings.namespace}",
        ))
    results = await asyncio.gather(*tasks)
    
    cumulative_trade = {
        'totalTradeVolume': 0,
        'timeInterval': time_interval,
    }
    for data in results:
        cumulative_trade['totalTradeVolume'] += data['totalTradeVolume']

    return cumulative_trade


async def _extract_price_information(
    snapshots: List[Optional[Dict]],
    target_token_address: str,
    project_id: str,
    tail_epoch_id: int,
    target_epochs: List[int],
) -> Dict[str, Any]:
    """Extract price information from snapshots with comprehensive validation."""
    
    snapshot_prices_map = {}
    target_token_price_key = None
    tail_epoch_covered = False
    
    # Determine price key from available snapshots
    for snapshot in snapshots:
        if not snapshot:
            continue
            
        token0 = snapshot.get('token0')
        token1 = snapshot.get('token1')
        
        if target_token_address == token0:
            target_token_price_key = 'token0PricesUSD'
            break
        elif target_token_address == token1:
            target_token_price_key = 'token1PricesUSD'
            break

    if not target_token_price_key:
        logger.warning(f"Cannot determine price key for token {target_token_address} in project {project_id}")
        return {
            'has_data': False,
            'target_token_price_key': None,
            'snapshot_prices_map': {},
            'tail_epoch_covered': False,
        }

    # Extract price data from snapshots
    for i, snapshot in enumerate(snapshots):
        if not snapshot:
            continue
            
        prices_data = snapshot.get(target_token_price_key)
        if not isinstance(prices_data, dict):
            if prices_data is not None:
                logger.warning(f"Price data for {target_token_price_key} is not a dict: {type(prices_data)}")
            continue
            
        for block_str, price_val in prices_data.items():
            try:
                block_num = int(block_str)
                price = float(price_val)
                snapshot_prices_map[block_num] = price
                
                if block_num == tail_epoch_id:
                    tail_epoch_covered = True
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid price data: block='{block_str}', price='{price_val}': {e}")
                continue

    logger.info(f"Extracted {len(snapshot_prices_map)} price points using key '{target_token_price_key}'")
    
    return {
        'has_data': len(snapshot_prices_map) > 0,
        'target_token_price_key': target_token_price_key,
        'snapshot_prices_map': snapshot_prices_map,
        'tail_epoch_covered': tail_epoch_covered,
    }


async def _handle_fallback_snapshot(
    redis_conn: aioredis.Redis,
    protocol_state_contract,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    project_id: str,
    target_token_address: str,
    tail_epoch_id: int,
    target_epochs: List[int],
) -> Dict[str, Any]:
    """Handle fallback to latest available snapshot when no target epoch data exists."""
    
    logger.info(f"No price data found in target epochs, attempting fallback for project {project_id}")
    
    try:
        latest_snapshot = await get_project_latest_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id
        )
        
        if not latest_snapshot:
            raise ValueError(f"No latest snapshot available for project {project_id}")
            
        # Create price info from latest snapshot
        fallback_info = await _extract_price_information(
            [latest_snapshot], target_token_address, project_id, tail_epoch_id, target_epochs
        )
        
        if fallback_info['has_data']:
            logger.info(f"Successfully using latest snapshot as fallback for project {project_id}")
            # When using fallback, we need to find the anchor price and block for ETH adjustments
            snapshot_prices_map = fallback_info['snapshot_prices_map']
            
            # Find the most relevant price from the fallback snapshot to use as anchor
            if snapshot_prices_map:
                # Use the price closest to tail epoch as the anchor
                anchor_block = None
                anchor_price = None
                
                # Look for price at or before tail_epoch_id
                for block_num in sorted(snapshot_prices_map.keys(), reverse=True):
                    if block_num <= tail_epoch_id:
                        anchor_block = block_num
                        anchor_price = snapshot_prices_map[block_num]
                        break
                
                # If no price at or before tail, use the earliest available
                if anchor_block is None:
                    anchor_block = min(snapshot_prices_map.keys())
                    anchor_price = snapshot_prices_map[anchor_block]
                
                # Store the anchor information for later ETH price adjustments
                fallback_info['fallback_anchor_block'] = anchor_block
                fallback_info['fallback_anchor_price'] = anchor_price
                logger.info(f"Using fallback anchor: price={anchor_price} at block={anchor_block}")
                
            return fallback_info
        else:
            raise ValueError(f"Latest snapshot contains no usable price data for token {target_token_address}")
            
    except Exception as e:
        logger.error(f"Fallback snapshot fetch failed for project {project_id}: {e}")
        raise ValueError(
            f"No snapshot data available for project {project_id} and token {target_token_address}. "
            f"Both target epochs and fallback snapshot are unavailable: {e}"
        )


async def _fetch_eth_prices(
    redis_conn: aioredis.Redis,
    price_info: Dict[str, Any],
    target_epochs: List[int],
    tail_epoch_id: int,
    current_epoch: int,
) -> Dict[str, Any]:
    """Fetch ETH prices needed for price adjustments."""
    
    # Determine block range for ETH prices
    price_blocks = list(price_info['snapshot_prices_map'].keys())
    all_blocks = sorted(set(target_epochs + price_blocks))
    
    eth_price_min_block = min(all_blocks) if all_blocks else tail_epoch_id
    eth_price_max_block = max(all_blocks) if all_blocks else current_epoch
    
    logger.info(f"Fetching ETH prices for block range [{eth_price_min_block}, {eth_price_max_block}]")
    
    block_to_eth_price_map = {}
    
    try:
        eth_prices_raw = await redis_conn.zrangebyscore(
            uniswap_eth_usd_price_zset,
            min=eth_price_min_block,
            max=eth_price_max_block,
            withscores=False
        )
        
        for item_raw in eth_prices_raw:
            try:
                item_data = json.loads(item_raw.decode('utf-8'))
                block_height = int(item_data.get('blockHeight', 0))
                price_eth_val = float(item_data.get('price', 0))
                
                if block_height > 0 and price_eth_val > 0:
                    block_to_eth_price_map[block_height] = price_eth_val
                    
            except (json.JSONDecodeError, TypeError, ValueError, AttributeError) as e:
                logger.warning(f"Invalid ETH price data: {item_raw}: {e}")
                continue
                
        logger.info(f"Loaded {len(block_to_eth_price_map)} ETH price points")
        
    except Exception as e:
        logger.warning(f"Failed to fetch ETH prices: {e}")

    return {
        'block_to_eth_price_map': block_to_eth_price_map,
        'eth_price_min_block': eth_price_min_block,
        'eth_price_max_block': eth_price_max_block,
    }


async def _generate_price_series(
    target_epochs: List[int],
    blocks_of_interest: List[int],
    block_to_timestamp_map: Dict[int, int],
    price_info: Dict[str, Any],
    eth_price_info: Dict[str, Any],
    project_id: str,
) -> List[Dict[str, Any]]:
    """Generate price series with ETH price adjustments for target epochs only."""
    
    price_data = []
    snapshot_prices_map = price_info['snapshot_prices_map']
    block_to_eth_price_map = eth_price_info['block_to_eth_price_map']
    
    # Get all blocks that have price data, sorted for efficient lookup
    price_blocks = sorted(snapshot_prices_map.keys())
    
    # Include fallback anchor if available
    if 'fallback_anchor_block' in price_info and 'fallback_anchor_price' in price_info:
        fallback_block = price_info['fallback_anchor_block']
        fallback_price = price_info['fallback_anchor_price']
        
        # Add fallback to price data if not already present
        if fallback_block not in snapshot_prices_map:
            snapshot_prices_map[fallback_block] = fallback_price
            price_blocks = sorted(snapshot_prices_map.keys())
            
        logger.info(f"Added fallback anchor: price={fallback_price} at block={fallback_block}")
    
    # Only generate price data for target epochs (requested time interval)
    # Use blocks_of_interest for price calculations but don't include them in output
    for block_num in sorted(target_epochs):
        timestamp = block_to_timestamp_map.get(block_num)
        if timestamp is None:
            logger.debug(f"Skipping block {block_num} - no timestamp available")
            continue
            
        # Check if we have direct price data from snapshot
        direct_price = snapshot_prices_map.get(block_num)
        
        if direct_price is not None:
            # Use direct price - no adjustment needed
            price_to_add = direct_price
            logger.debug(f"Using direct price for block {block_num}: {direct_price}")
            
        else:
            # Find the closest previous epoch with price data
            closest_previous_block = None
            for price_block in reversed(price_blocks):  # Start from the latest and go backwards
                if price_block <= block_num:
                    closest_previous_block = price_block
                    break
            
            if closest_previous_block is None:
                logger.debug(f"No previous price data available for block {block_num}")
                continue
                
            # Get the base price from the closest previous epoch (snapshot epoch)
            base_price = snapshot_prices_map[closest_previous_block]
            
            # Get ETH prices for both epochs
            eth_price_at_snapshot_epoch = block_to_eth_price_map.get(closest_previous_block)
            eth_price_at_current_epoch = block_to_eth_price_map.get(block_num)
            
            if (eth_price_at_snapshot_epoch is not None and 
                eth_price_at_current_epoch is not None and 
                eth_price_at_snapshot_epoch > 0):
                
                # Price scaling: (base_price / eth_price_at_snapshot_epoch) * eth_price_at_current_epoch
                # This normalizes the price by removing ETH effect from snapshot epoch,
                # then applies ETH effect for current epoch
                normalized_price = base_price / eth_price_at_snapshot_epoch
                price_to_add = normalized_price * eth_price_at_current_epoch
                
                logger.debug(
                    f"ETH-adjusted price for block {block_num}: "
                    f"base_price={base_price:.4f} (from block {closest_previous_block}) "
                    f"/ eth_snapshot={eth_price_at_snapshot_epoch:.2f} "
                    f"* eth_current={eth_price_at_current_epoch:.2f} "
                    f"= {price_to_add:.4f}"
                )
            else:
                # If no ETH price data available, use the base price without adjustment
                price_to_add = base_price
                logger.debug(
                    f"Using base price without ETH adjustment for block {block_num}: "
                    f"price={base_price:.4f} (from block {closest_previous_block}), "
                    f"eth_snapshot={eth_price_at_snapshot_epoch}, eth_current={eth_price_at_current_epoch}"
                )
            
        if price_to_add is not None and price_to_add > 0:
            price_data.append({
                'blockNumber': block_num,
                'price': price_to_add,
                'timestamp': timestamp,
            })

    logger.info(f"Generated {len(price_data)} price data points for project {project_id}")
    return price_data


def _apply_time_spacing(price_data: List[Dict[str, Any]], step_seconds: int) -> List[Dict[str, Any]]:
    """Apply time-based spacing to price data points."""
    
    if not price_data:
        return []
        
    # Sort by timestamp to ensure proper spacing
    price_data.sort(key=lambda x: x['timestamp'])
    
    spaced_data = [price_data[0]]  # Always include first point
    last_timestamp = price_data[0]['timestamp']
    
    for entry in price_data[1:]:
        if entry['timestamp'] >= last_timestamp + step_seconds:
            spaced_data.append(entry)
            last_timestamp = entry['timestamp']
            
    return spaced_data


async def get_uniswap_price_series_agg(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    project_id: str,
    token_address: str,
    step_seconds: int,
) -> Dict[str, Any]:
    """
    Generates a production-ready time series of token prices with comprehensive error handling.
    
    This function retrieves price data for a token from snapshots, handles missing timestamps 
    by fetching from RPC, and creates a time series with evenly spaced intervals.
    
    Args:
        redis_conn: Redis connection for data access and caching
        rpc_helper: RPC helper for fetching block timestamps from blockchain
        anchor_rpc_helper: RPC helper for protocol interactions
        ipfs_reader: IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval: Total time interval in seconds to analyze
        project_id: Project identifier for the price data
        token_address: Ethereum address of the token to get prices for
        step_seconds: Time spacing in seconds between price data points
        
    Returns:
        Dictionary containing:
        - priceSeries: List of price entries with blockNumber, price, timestamp
        - timeInterval: The time interval used for the analysis
        
    Raises:
        ValueError: If input parameters are invalid or no price data can be generated
        Exception: If critical blockchain or data fetch operations fail
    """
    if time_interval <= 0 or step_seconds <= 0:
        raise ValueError(f"Invalid time_interval ({time_interval}) or step_seconds ({step_seconds})")
    
    if not project_id or not token_address:
        raise ValueError("Invalid project_id or token_address")

    # Normalize token address
    try:
        target_token_address = Web3.to_checksum_address(token_address)
    except Exception as e:
        raise ValueError(f"Invalid token address format '{token_address}': {e}")

    logger.info(
        f"Starting price series generation for project {project_id}, token {target_token_address}, "
        f"time_interval={time_interval}s, step={step_seconds}s"
    )

    # 1. Get current epoch and calculate tail epoch
    try:
        current_epoch_data = await anchor_rpc_helper.web3_call(
            tasks=[('currentEpoch', [Web3.to_checksum_address(settings.data_market)])],
            contract_addr=protocol_state_contract.address,
            abi=protocol_state_contract.abi,
        )
        current_epoch = current_epoch_data[0][2]
        
        tail_epoch_id, _ = await get_tail_epoch_id(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            current_epoch, time_interval, project_id
        )
        
        if current_epoch < tail_epoch_id:
            raise ValueError(
                f"Invalid epoch range: current_epoch ({current_epoch}) < tail_epoch_id ({tail_epoch_id})"
            )
            
        logger.info(f"Epoch range: {tail_epoch_id} to {current_epoch}")
        
    except Exception as e:
        logger.error(f"Failed to get epoch information: {e}")
        raise Exception(f"Cannot determine epoch range: {e}")

    # 2. Get chain parameters and calculate target epochs
    try:
        chain_params = await asyncio.gather(
            get_source_chain_epoch_size(redis_conn, protocol_state_contract, anchor_rpc_helper),
            get_source_chain_block_time(redis_conn, protocol_state_contract, anchor_rpc_helper),
            return_exceptions=True
        )
        
        for i, param in enumerate(chain_params):
            if isinstance(param, Exception):
                param_name = ["epoch_size", "block_time"][i]
                raise Exception(f"Failed to get {param_name}: {param}")
        
        source_chain_epoch_size, source_chain_block_time = chain_params
        
        if source_chain_epoch_size <= 0 or source_chain_block_time <= 0:
            raise ValueError(f"Invalid chain params: epoch_size={source_chain_epoch_size}, block_time={source_chain_block_time}")
            
        # Calculate epoch step size based on time step
        epoch_step_size = max(1, int(step_seconds / (source_chain_epoch_size * source_chain_block_time)))
        
        # Generate target epochs with step intervals
        target_epochs = list(range(tail_epoch_id, current_epoch + 1, epoch_step_size))
        if target_epochs[-1] != current_epoch:
            target_epochs.append(current_epoch)
        
        logger.info(f"Targeting {len(target_epochs)} epochs with step size {epoch_step_size}")
        
    except Exception as e:
        logger.error(f"Failed to calculate target epochs: {e}")
        raise Exception(f"Cannot determine target epochs: {e}")

    # 3. Fetch cached block timestamps
    block_to_timestamp_map = {}
    try:
        timestamp_data = await redis_conn.zrangebyscore(
            block_number_to_timestamp_key(settings.namespace),
            min=tail_epoch_id,
            max=current_epoch,
            withscores=True
        )
        
        for json_timestamp_str, block_num_score in timestamp_data:
            if json_timestamp_str:
                try:
                    block_num = int(block_num_score)
                    timestamp = json.loads(json_timestamp_str)
                    if isinstance(timestamp, int):
                        block_to_timestamp_map[block_num] = timestamp
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    logger.warning(f"Invalid timestamp data for block {block_num_score}: {e}")
                    continue
                    
        logger.info(f"Loaded {len(block_to_timestamp_map)} cached timestamps")
        
    except Exception as e:
        logger.warning(f"Failed to load cached timestamps: {e}")

    # 4. Fetch snapshots concurrently with robust error handling
    snapshot_data = await _fetch_snapshots_for_epochs(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, 
        target_epochs, project_id
    )
    
    # 5. Process snapshots and extract price information
    price_info = await _extract_price_information(
        snapshot_data, target_token_address, project_id, 
        tail_epoch_id, target_epochs
    )
    
    # 6. Handle fallback scenarios if no snapshot data found
    if not price_info['has_data']:
        price_info = await _handle_fallback_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
            project_id, target_token_address, tail_epoch_id, target_epochs
        )

    # 7. Fetch ETH prices for price adjustments
    eth_price_info = await _fetch_eth_prices(
        redis_conn, price_info, target_epochs, tail_epoch_id, current_epoch
    )

    # 8. Build blocks of interest and fetch missing timestamps
    blocks_of_interest = sorted(list(set(target_epochs + list(price_info['snapshot_prices_map'].keys()))))
    
    missing_timestamps = await _fetch_missing_timestamps(
        redis_conn, rpc_helper, blocks_of_interest, block_to_timestamp_map, project_id
    )
    block_to_timestamp_map.update(missing_timestamps)

    # 9. Generate price series with ETH adjustments (only for target epochs)
    price_data = await _generate_price_series(
        target_epochs, blocks_of_interest, block_to_timestamp_map, price_info, eth_price_info, project_id
    )

    # 10. Apply time-based spacing and return results
    if not price_data:
        raise ValueError(
            f"No price data generated for project {project_id}, token {target_token_address}. "
            f"This may indicate missing price anchors or ETH price data."
        )

    spaced_price_data = _apply_time_spacing(price_data, step_seconds)
    
    logger.info(f"Generated {len(spaced_price_data)} spaced price points for project {project_id}")
    
    return {
        'priceSeries': spaced_price_data,
        'timeInterval': time_interval,
    }


async def get_uniswap_v3_pool_trades(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    project_id: str,
    pool_address: str,
    start_timestamp: int,
    end_timestamp: int,
    protocol_state_contract,
) -> List[Dict]:
    """Fetches Uniswap V3 pool trades for a given pool and time range.

    The time range is inclusive of the start_timestamp and exclusive of the end_timestamp, i.e., [start_timestamp, end_timestamp).

    Args:
        redis_conn: Async Redis connection object.
        anchor_rpc_helper: RPC helper for anchor chain.
        rpc_helper: RPC helper for source chain.
        ipfs_reader: Async IPFS client.
        project_id: Project ID for fetching trade snapshots.
        pool_address: Address of the Uniswap V3 pool.
        start_timestamp: Unix timestamp for the start of the period (inclusive).
        end_timestamp: Unix timestamp for the end of the period (exclusive).
        protocol_state_contract: Protocol state contract object.

    Returns:
        A list of dictionaries, each representing a trade.
    
    Raises:
        Exception: If pool metadata or essential block information cannot be retrieved.
    """
    
    if start_timestamp >= end_timestamp:
        raise ValueError(f"start_timestamp ({start_timestamp}) must be less than end_timestamp ({end_timestamp})")

    pool_metadata = await get_uniswap_v3_pool_metadata(
        pool_address=pool_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )

    if not pool_metadata:
        logger.error(f"No pool metadata found for project {project_id} and pool {pool_address}.")
        raise Exception(f"No pool metadata found for project {project_id} and pool {pool_address}.")

    # Determine start_block (timestamp >= start_timestamp)
    start_block = await get_block_number_closest_to_timestamp(
        redis_conn=redis_conn,
        target_timestamp=start_timestamp,
        search_type=BlockSearchType.AFTER_OR_AT,
    )

    if not start_block:
        logger.warning(f"No closest start block found for project {project_id}, pool {pool_address}, timestamp {start_timestamp}. Attempting fallback.")
        try:
            start_block = await _fallback_fetch_block_at_timestamp(
                redis_conn=redis_conn,
                anchor_rpc_helper=anchor_rpc_helper,
                rpc_helper=rpc_helper,
                protocol_state_contract=protocol_state_contract,
                target_timestamp=start_timestamp,
                search_type=BlockSearchType.AFTER_OR_AT,
            )
        except Exception as e:
            err_msg = f"Error during fallback mechanism for start_block for project {project_id}, pool {pool_address}: {e}"
            logger.error(err_msg, exc_info=True)
            raise Exception(f"No closest start block found for project {project_id} and pool {pool_address}, and fallback failed: {e}")

    # Determine end_block (timestamp < end_timestamp, so target is end_timestamp - 1)
    effective_end_target_timestamp = end_timestamp - 1

    end_block = await get_block_number_closest_to_timestamp(
        redis_conn=redis_conn,
        target_timestamp=effective_end_target_timestamp,
        search_type=BlockSearchType.BEFORE_OR_AT,
    )

    if not end_block:
        logger.warning(f"No closest end block found for project {project_id}, pool {pool_address}, timestamp {effective_end_target_timestamp}. Attempting fallback.")
        try:
            end_block = await _fallback_fetch_block_at_timestamp(
                redis_conn=redis_conn,
                anchor_rpc_helper=anchor_rpc_helper,
                rpc_helper=rpc_helper,
                protocol_state_contract=protocol_state_contract,
                target_timestamp=effective_end_target_timestamp,
                search_type=BlockSearchType.BEFORE_OR_AT,
            )
        except Exception as e:
            err_msg = f"Error during fallback mechanism for end_block for project {project_id}, pool {pool_address} (end_timestamp: {end_timestamp}): {e}"
            logger.error(err_msg, exc_info=True)
            raise Exception(f"No closest end block found for project {project_id} and pool {pool_address} (end_timestamp: {end_timestamp}), and fallback failed: {e}")
    
    logger.info(f"Determined block range for project {project_id}, pool {pool_address}: start_block={start_block}, end_block={end_block}.")

    if start_block is None or end_block is None:
        err_msg = f"Could not determine valid start_block ({start_block}) or end_block ({end_block}) for project {project_id}, pool {pool_address}."
        logger.error(err_msg)
        raise Exception(err_msg)

    if start_block > end_block:
        logger.warning(
            f"Calculated start_block {start_block} is after end_block {end_block} for project {project_id}, pool {pool_address}. "
            f"Timestamps: start={start_timestamp}, end={end_timestamp}."
        )
        return []

    # Fetch trade snapshot data for the determined range
    try:
        trade_snapshots_raw = await get_project_epoch_snapshot_bulk(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, start_block, end_block, project_id,
        )
    except Exception as e:
        err_msg = f"Error fetching trade snapshots for project {project_id} and pool {pool_address} covering blocks {start_block} to {end_block}: {e}"
        logger.error(err_msg)
        raise Exception(err_msg)

    # Determine base token (WETH is typically the base)
    if Web3.to_checksum_address(pool_metadata.token0.address) == WETH:
        base_token_num = 0
    elif Web3.to_checksum_address(pool_metadata.token1.address) == WETH:
        base_token_num = 1
    else:
        logger.error(f"Neither token in pool {pool_address} is WETH. Cannot determine base token.")
        raise Exception(f"Pool {pool_address} does not contain WETH")

    token0_symbol = pool_metadata.token0.symbol
    token1_symbol = pool_metadata.token1.symbol

    processed_trades = []

    for trade_snapshot_raw_item in trade_snapshots_raw:
        if not trade_snapshot_raw_item:
            continue

        try:
            trade_snapshot = UniswapTradesSnapshot.model_validate(trade_snapshot_raw_item)
        except Exception as e:
            logger.error(f"Error validating trade snapshot for project {project_id} and pool {pool_address}: {e}. Data: {str(trade_snapshot_raw_item)[:200]}")
            continue

        # Process individual trades within the snapshot
        for trade in trade_snapshot.trades:

            if trade.tradeType == TradeType.SWAP:
                block_timestamp = trade.data.get('block_timestamp')
                token0_amount = trade.data['amount0']
                token1_amount = trade.data['amount1']
                transaction_hash = trade.log['transactionHash']
                
                # Adjust amounts for token decimals
                token0_amount_adjusted = abs(token0_amount) / 10 ** pool_metadata.token0.decimals
                token1_amount_adjusted = abs(token1_amount) / 10 ** pool_metadata.token1.decimals
                trade_amount_usd = trade.data.get('calculated_trade_amount_usd', 0.0)
                trade_type_str = "Swap"

                price_of_non_base_token_in_weth = 0.0
                if base_token_num == 0:
                    if token1_amount_adjusted > 1e-18: 
                        price_of_non_base_token_in_weth = token0_amount_adjusted / token1_amount_adjusted
                    elif token0_amount_adjusted > 1e-18:
                        logger.warning(f"Token1 amount is zero for trade where WETH is token0. Pool: {pool_address}, tx: {transaction_hash}")
                    else:
                        logger.warning(f"Both token amounts are zero for trade where WETH is token0. Pool: {pool_address}, tx: {transaction_hash}")
                else:
                    if token0_amount_adjusted > 1e-18:
                        price_of_non_base_token_in_weth = token1_amount_adjusted / token0_amount_adjusted
                    elif token1_amount_adjusted > 1e-18:
                        logger.warning(f"Token0 amount is zero for trade where WETH is token1. Pool: {pool_address}, tx: {transaction_hash}")
                    else:
                        logger.warning(f"Both token amounts are zero for trade where WETH is token1. Pool: {pool_address}, tx: {transaction_hash}")


                # Calculate USD price using ETH price from trade data
                eth_price_usd = trade.data.get('calculated_eth_price', 0.0)
                price_of_non_base_token_usd = price_of_non_base_token_in_weth * eth_price_usd
                
                # Create processed trade entry
                processed_trade_entry = {
                    'timestamp': block_timestamp,
                    'tokens': {
                        token0_symbol: token0_amount_adjusted,
                        token1_symbol: token1_amount_adjusted,
                    },
                    'trade_amount_usd': trade_amount_usd,
                    'trade_type': trade_type_str,
                    'trade_price_usd': price_of_non_base_token_usd,
                    'token0_amount': token0_amount,
                    'token1_amount': token1_amount,
                    'transaction_hash': transaction_hash,
                }
                processed_trades.append(processed_trade_entry)

    return processed_trades

