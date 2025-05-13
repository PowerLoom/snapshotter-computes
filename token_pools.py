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
from web3 import Web3


class TokenPoolsProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting token pools for Uniswap pairs.
    """

    def __init__(self) -> None:
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
        Process a single token asynchronously.

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
            metadata_project_id = f"metadata:{pool_address}:{settings.namespace}"
            snapshots = []

            pool_metadata = await get_project_latest_snapshot(
                redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, metadata_project_id,
            )

            if not pool_metadata:
                # Check Redis cache first
                cache_key = f'pool_metadata:{pool_address}'
                cached_data = await redis_conn.get(cache_key)

                if cached_data:
                    pool_metadata = json.loads(cached_data)
                else:
                    self._logger.debug(
                        "[Epoch {}-{}] Pool {} | No metadata found in cache or first epoch",
                        epoch.begin,
                        epoch.end,
                        pool_address
                    )
                    return None
            
            token_addresses = [pool_metadata["token0"]["address"], pool_metadata["token1"]["address"]]
            token_addresses = [Web3.to_checksum_address(token_address) for token_address in token_addresses]

            for token_address in token_addresses:
                project_id = task_type.format(tokenAddress=token_address, Namespace=settings.namespace)

                token_pools_snapshot = await get_project_latest_snapshot(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id,
                )

                if token_pools_snapshot:
                    if isinstance(token_pools_snapshot, str):
                        token_pools_snapshot = json.loads(token_pools_snapshot)
                    snapshot = UniswapTokenPoolsSnapshot(**token_pools_snapshot)
                else:
                    snapshot = UniswapTokenPoolsSnapshot(
                        pools={}
                    )

                local_pools = await redis_conn.smembers(f"token_pools:{token_address}")
                if local_pools:
                    local_pools = [pool.decode('utf-8') for pool in local_pools]
                local_pools_with_metadata = {}
                for pool in local_pools:
                    if pool in snapshot.pools:
                        continue
                    # check if metadata is present in redis
                    cache_key = f'pool_metadata:{pool}'
                    cached_data = await redis_conn.get(cache_key)

                    if cached_data:
                        local_pools_with_metadata[pool] = json.loads(cached_data)
                    else:
                        pool_metadata = await get_project_latest_snapshot(
                            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, metadata_project_id,
                        )

                        if pool_metadata:
                            local_pools_with_metadata[pool] = pool_metadata
                        else:
                            self._logger.error(
                                "[Epoch {}-{}] Pool {} | No metadata found in cache or protocol state",
                                epoch.begin,
                                epoch.end,
                                pool_address
                            )
                            continue

                if not local_pools_with_metadata:
                    continue

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
        Compute the metadata for a Uniswap pair within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        Returns:
            Optional[Dict[str, Union[int, float]]]: Computed pair metadata snapshot.
        """
        self._logger.info(
            "[Epoch {}-{}] Token pools compute | Starting token pools computation",
            epoch.begin,
            epoch.end
        )
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
        self._logger.info(
            "[Epoch {}-{}] Token pools compute | Found {} active pools to process",
            min_chain_height,
            max_chain_height,
            len(pools)
        )
        
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
