from typing import Dict
from typing import Optional
from typing import Union
import asyncio

from redis import asyncio as aioredis
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient
from computes.utils.helpers import get_uniswap_v3_pool_metadata
from web3 import Web3


class MetadataProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting metadata for Uniswap pairs.
    
    This class handles the processing and caching of metadata for Uniswap liquidity pools,
    including retrieving and storing pool information from various sources.
    """

    def __init__(self) -> None:
        """Initialize the MetadataProcessor with a logger instance."""
        self._logger = logger.bind(module="MetadataProcessor")
    
    async def _process_pool(
        self,
        epoch: SnapshotProcessMessage,
        pool_address: str,
        task_type: str,
        redis_conn: aioredis.Redis,
        protocol_state_contract,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
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
            pool_metadata = await get_uniswap_v3_pool_metadata(
                pool_address, redis_conn, anchor_rpc_helper, ipfs_reader, protocol_state_contract,
            )
            if not pool_metadata:
                return None
            
            return (pool_address, pool_metadata)
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
            key = f"active_pools:{block_number}:{settings.namespace}"
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
                ipfs_reader=ipfs_reader,
            )
            pool_tasks.append(task)
        
        # Execute all pool processing tasks in parallel
        results = await asyncio.gather(*pool_tasks, return_exceptions=False)
        
        # Filter out failed tasks and collect valid snapshots
        snapshots = [result for result in results if result is not None]
        
        return snapshots
