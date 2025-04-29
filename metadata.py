from typing import Dict
from typing import Optional
from typing import Union
import asyncio

from redis import asyncio as aioredis
import json
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from computes.utils.models.message_models import UniswapPoolMetadata
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient
from snapshotter.utils.data_utils import get_project_first_epoch
from web3 import Web3


class MetadataProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="MetadataProcessor")

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
        Process a single pool asynchronously.

        Args:
            pool_address (str): The pool address to process
            task_type (str): The task type format string
            redis_conn (aioredis.Redis): Redis connection object
            protocol_state_contract: The protocol state contract
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain
            
        Returns:
            tuple: A tuple containing project_id and pool metadata snapshot if available
        """
        try:
            project_id = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
            
            # aggregate project first epoch
            project_first_epoch = await get_project_first_epoch(
                redis_conn, protocol_state_contract, anchor_rpc_helper, project_id,
            )

            if not project_first_epoch:
                # Check Redis cache first
                cache_key = f'pool_metadata:{pool_address}'
                cached_data = await redis_conn.get(cache_key)

                if cached_data:
                    data = json.loads(cached_data)
                    return (project_id, UniswapPoolMetadata(**data))
            
            return None
        except Exception as e:
            self._logger.opt(exception=e).error(f"Error processing pool {pool_address}")
            # Silently ignore any exceptions
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
        Compute the metadata for a Uniswap pair within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        Returns:
            Optional[Dict[str, Union[int, float]]]: Computed pair metadata snapshot.
        """
        self._logger.info(f"Computing metadata for epoch {epoch.epochId}")
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        keys_to_fetch = []
        # Collect all keys to fetch
        for block_number in range(min_chain_height, max_chain_height + 1):
            key = f"active_pools:{block_number}:{settings.namespace}"
            keys_to_fetch.append(key)

        # Use sunion to get the union of all sets at once
        pools = set()
        if keys_to_fetch:
            pools = await redis_conn.sunion(*keys_to_fetch)
        self._logger.info(f"Found {len(pools)} active pools in the epoch {min_chain_height} to {max_chain_height}")
        
        # Process all pools in parallel
        pool_tasks = []
        for pool_address in pools:
            pool_address = Web3.to_checksum_address(pool_address.decode('utf-8'))
            task = self._process_pool(
                epoch=epoch,
                pool_address=pool_address,
                task_type=task_type,
                redis_conn=redis_conn,
                protocol_state_contract=protocol_state_contract,
                anchor_rpc_helper=anchor_rpc_helper,
            )
            pool_tasks.append(task)
        
        # Gather results from all tasks, with return_exceptions=False
        # This will make asyncio.gather() ignore failed tasks and continue with the rest
        results = await asyncio.gather(*pool_tasks, return_exceptions=False)
        
        # Filter out None results and add valid snapshots
        snapshots = [result for result in results if result is not None]
        
        return snapshots
