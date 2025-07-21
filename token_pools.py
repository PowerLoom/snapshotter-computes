from typing import List, Optional, Tuple
import asyncio

from redis import asyncio as aioredis
import json
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient
from computes.utils.models.message_models import UniswapPoolMetadata, UniswapTokenPoolsSnapshot
from snapshotter.utils.data_utils import get_project_latest_snapshot
from computes.api.utils.data_utils import get_uniswap_v3_pool_metadata
from web3 import Web3


class TokenPoolsProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting token pools for Uniswap pairs.
    
    This class handles the processing and computation of token pool data for Uniswap pairs,
    including metadata retrieval, caching, and parallel processing of multiple pools.
    """

    def __init__(self) -> None:
        """
        Initialize the TokenPoolsProcessor with a bound logger.
        """
        self._logger = logger.bind(module="TokenPoolsProcessor")

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
        Process a single token pool asynchronously.

        This method handles the processing of a single pool, including:
        - Retrieving pool metadata from cache or protocol state
        - Processing token addresses and creating snapshots
        - Handling WETH pools and local pool metadata

        Args:
            epoch (SnapshotProcessMessage): The epoch information for processing
            pool_address (str): The pool address to process
            task_type (str): The task type format string for project ID generation
            redis_conn (aioredis.Redis): Redis connection for caching and data retrieval
            protocol_state_contract: The protocol state contract for on-chain data
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain interactions
            ipfs_reader (AsyncIPFSClient): IPFS client for data retrieval
            
        Returns:
            Optional[List[Tuple[str, UniswapTokenPoolsSnapshot]]]: List of project IDs and their corresponding snapshots,
            or None if processing fails
        """
        try:
            metadata_project_id = f"metadata:{pool_address}:{settings.namespace}"
            snapshots = []

            # Attempt to get pool metadata from protocol state
            pool_metadata = await get_uniswap_v3_pool_metadata(
                pool_address, redis_conn, anchor_rpc_helper, ipfs_reader, protocol_state_contract,
            )
            if not pool_metadata:
                self._logger.error(
                    "[Epoch {}-{}] Pool {} | No metadata found in cache or protocol state",
                    epoch.begin,
                    epoch.end,
                    pool_address
                )
                return None
            # Process token addresses and ensure checksum format
            token_addresses = [pool_metadata.token0.address, pool_metadata.token1.address]
            token_addresses = [Web3.to_checksum_address(token_address) for token_address in token_addresses]
            
            for token_address in token_addresses:
                project_id = task_type.format(tokenAddress=token_address, Namespace=settings.namespace)

                # Get existing token pools snapshot or create new one
                token_pools_snapshot = await get_project_latest_snapshot(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id,
                )

                if token_pools_snapshot:
                    if isinstance(token_pools_snapshot, str):
                        token_pools_snapshot = json.loads(token_pools_snapshot)
                    snapshot = UniswapTokenPoolsSnapshot(**token_pools_snapshot)
                else:
                    snapshot = UniswapTokenPoolsSnapshot(pools={})

                # Get and process local pools
                local_pools = await redis_conn.smembers(f"token_pools:{token_address}")
                if local_pools:
                    local_pools = [pool.decode('utf-8') for pool in local_pools]
                local_pools_with_metadata = {}
                
                # Process each local pool
                for pool in local_pools:
                    if pool in snapshot.pools:
                        continue
                    
                    pool_metadata = await get_uniswap_v3_pool_metadata(
                        pool, redis_conn, anchor_rpc_helper, ipfs_reader, protocol_state_contract,
                    )
                    if not pool_metadata:
                        self._logger.error(
                            "[Epoch {}-{}] Pool {} | No metadata found in cache or protocol state",
                            epoch.begin,
                            epoch.end,
                            pool
                        )
                        continue

                    local_pools_with_metadata[pool] = pool_metadata

                if not local_pools_with_metadata:
                    continue

                # Update snapshot with local pool metadata
                for pool in local_pools_with_metadata:
                    snapshot.pools[pool] = UniswapPoolMetadata(**local_pools_with_metadata[pool])
                
                snapshots.append((project_id, snapshot))

            return snapshots
        except Exception as e:
            self._logger.opt(exception=e).error(
                "[Epoch {}-{}] Token pools compute | Pool {} | Error processing pool metadata",
                epoch.begin,
                epoch.end,
                pool_address
            )
            return None

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> Optional[List[Tuple[str, UniswapTokenPoolsSnapshot]]]:
        """
        Compute token pool snapshots for the given epoch.

        This method orchestrates the parallel processing of multiple pools:
        1. Collects active pools for the epoch
        2. Processes pools in parallel using asyncio
        3. Aggregates results into snapshots

        Args:
            epoch (SnapshotProcessMessage): The epoch information for processing
            redis_conn (aioredis.Redis): Redis connection for data retrieval
            rpc_helper (RpcHelper): RPC helper for blockchain interactions
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain
            ipfs_reader (AsyncIPFSClient): IPFS client for data retrieval
            protocol_state_contract: The protocol state contract
            task_type (str): The task type format string

        Returns:
            Optional[List[Tuple[str, UniswapTokenPoolsSnapshot]]]: List of project IDs and their corresponding snapshots,
            or None if computation fails
        """
        self._logger.info(
            "[Epoch {}-{}] Token pools compute | Starting token pools computation",
            epoch.begin,
            epoch.end
        )
        
        # Get epoch boundaries
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        # Collect keys for active pools
        keys_to_fetch = []
        for block_number in range(min_chain_height, max_chain_height + 1):
            key = f"active_pools:{block_number}:{settings.namespace}"
            keys_to_fetch.append(key)

        # Get union of all active pools
        pools = set()
        if keys_to_fetch:
            pools = await redis_conn.sunion(*keys_to_fetch)
            
        self._logger.info(
            "[Epoch {}-{}] Token pools compute | Found {} active pools to process",
            min_chain_height,
            max_chain_height,
            len(pools)
        )
        
        # Create tasks for parallel processing
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
                ipfs_reader=ipfs_reader,
            )
            pool_tasks.append(task)
        
        # Gather results from all tasks, with return_exceptions=False
        # This will make asyncio.gather() ignore failed tasks and continue with the rest
        logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Gathering results from {len(pool_tasks)} tasks")
        snapshots = []
        results = await asyncio.gather(*pool_tasks, return_exceptions=False)
        for result in results:
            if result:
                snapshots.extend(result)
        logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Results gathered from {len(pool_tasks)} tasks")
        return snapshots
