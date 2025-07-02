from typing import Dict
from typing import Optional
from typing import Union
import asyncio

from redis import asyncio as aioredis
import json
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from computes.redis_keys import pool_metadata_key, active_pools_per_block_key
from snapshotter.utils.redis.redis_keys import base_snapshot_project_id
from ipfs_client.main import AsyncIPFSClient
from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_first_epoch
from snapshotter.utils.data_utils import get_project_latest_snapshot
from web3 import Web3
from computes.utils.models.message_models import UniswapPoolMetadata


class MetadataProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting metadata for Uniswap pairs.
    
    This class handles the processing and caching of metadata for Uniswap liquidity pools,
    including retrieving and storing pool information from various sources.
    """

    def __init__(self) -> None:
        """Initialize the MetadataProcessor with a logger instance."""
        self._logger = logger.bind(module="MetadataProcessor")
    
    async def get_pool_metadata(
        self, 
        pool_address: str, 
        redis_conn: aioredis.Redis, 
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str = 'metadata:{poolAddress}:{Namespace}',
    ) -> Optional[UniswapPoolMetadata]:
        """
        Retrieve metadata for a specific pool, checking cache first then fetching from chain.

        Args:
            pool_address (str): The address of the pool to get metadata for
            redis_conn (aioredis.Redis): Redis connection for cache operations
            anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data
            protocol_state_contract: Contract instance for protocol state
            task_type (str): Format string for project ID construction

        Returns:
            Optional[UniswapPoolMetadata]: Pool metadata if found, None otherwise
        """
        # Check Redis cache first for existing metadata
        cache_key = pool_metadata_key(pool_address)
        cached_data = await redis_conn.get(cache_key)
        if cached_data:
            self._logger.info(f"Found cached metadata for pool {pool_address}")
            return UniswapPoolMetadata(**json.loads(cached_data))

        try:
            # Get the latest snapshot from chain if not in cache
            project_id = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
            latest_snapshot = await get_project_latest_snapshot(
                redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id
            )
            if not latest_snapshot:
                self._logger.error(f"No latest snapshot found for pool {pool_address} while processing metadata")
                return None
            return UniswapPoolMetadata(**latest_snapshot)
        except Exception as e:
            self._logger.opt(exception=e).error(f"Error getting latest snapshot for pool {pool_address} while processing metadata")
            return None

    async def _process_pool(
        self,
        epoch: SnapshotProcessMessage,
        pool_address: str,
        task_type: str,
        redis_conn: aioredis.Redis,
        protocol_state_contract,
        anchor_rpc_helper: RpcHelper,
    ):
        """
        Process a single pool asynchronously, checking first epoch and cache.

        Args:
            epoch (SnapshotProcessMessage): Current epoch information
            pool_address (str): The pool address to process
            task_type (str): Format string for project ID construction
            redis_conn (aioredis.Redis): Redis connection for cache operations
            protocol_state_contract: Contract instance for protocol state
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain
            
        Returns:
            Optional[tuple]: Tuple of (project_id, pool_metadata) if successful, None otherwise
        """
        try:
            project_id = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
            
            # Get project's first epoch data
            project_first_epoch = await get_project_first_epoch(
                redis_conn, protocol_state_contract, anchor_rpc_helper, project_id,
            )

            if not project_first_epoch:
                # If no first epoch, check Redis cache for existing metadata
                cache_key = pool_metadata_key(pool_address)
                cached_data = await redis_conn.get(cache_key)

                if cached_data:
                    data = json.loads(cached_data)
                    return (project_id, UniswapPoolMetadata(**data))
            
            return None
        except Exception as e:
            self._logger.opt(exception=e).error(f"Error processing pool {pool_address}")
            return None

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str = None,
    ) -> Optional[Dict[str, Union[int, float]]]:
        """
        Compute metadata for all active Uniswap pools within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information
            redis_conn (aioredis.Redis): Redis connection for cache operations
            rpc_helper (RpcHelper): RPC helper for blockchain interactions
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data
            protocol_state_contract: Contract instance for protocol state
            task_type (str): Format string for project ID construction

        Returns:
            Optional[Dict[str, Union[int, float]]]: Dictionary of computed pool metadata snapshots
        """
        self._logger.info(f"Computing metadata for epoch {epoch.epochId}")
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        # Collect all active pool keys for the epoch range
        keys_to_fetch = []
        for block_number in range(min_chain_height, max_chain_height + 1):
            key = active_pools_per_block_key(block_number, settings.namespace)
            keys_to_fetch.append(key)

        # Get union of all active pools across the epoch
        pools = set()
        if keys_to_fetch:
            pools = await redis_conn.sunion(*keys_to_fetch)
        self._logger.info(f"Found {len(pools)} active pools in the epoch {min_chain_height} to {max_chain_height}")

        # Convert pool addresses to checksum format
        pools = map(lambda x: Web3.to_checksum_address(x.decode('utf-8')), pools)
        
        # Create tasks for parallel processing of all pools
        pool_tasks = []
        for pool_address in pools:
            task = self._process_pool(
                epoch=epoch,
                pool_address=pool_address,
                task_type=task_type,
                redis_conn=redis_conn,
                protocol_state_contract=protocol_state_contract,
                anchor_rpc_helper=anchor_rpc_helper,
            )
            pool_tasks.append(task)
        
        # Execute all pool processing tasks in parallel
        results = await asyncio.gather(*pool_tasks, return_exceptions=False)
        
        # Filter out failed tasks and collect valid snapshots
        snapshots = [result for result in results if result is not None]
        
        return snapshots
