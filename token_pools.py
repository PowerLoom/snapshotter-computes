from typing import Dict, List, Tuple
from typing import Optional
from typing import Union
import asyncio

from redis import asyncio as aioredis
import json
from computes.metadata import MetadataProcessor
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient
from computes.utils.models.message_models import UniswapPoolMetadata, UniswapTokenPoolsSnapshot
from snapshotter.utils.data_utils import get_project_first_epoch
from snapshotter.utils.data_utils import get_project_last_finalized_epoch
from snapshotter.utils.data_utils import get_project_epoch_snapshot
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
            snapshots = []
            metadata_helper = MetadataProcessor()
            pool_metadata: Optional[UniswapPoolMetadata] = await metadata_helper.get_pool_metadata(
                pool_address=pool_address,
                redis_conn=redis_conn,
                protocol_state_contract=protocol_state_contract,
                anchor_rpc_helper=anchor_rpc_helper,
                ipfs_reader=ipfs_reader,
            )
            if not pool_metadata:
                return None
            logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | Token pools compute | Processing token pools for Token0: {pool_metadata.token0.address} | Token1: {pool_metadata.token1.address}")
            token_addresses = [pool_metadata.token0.address, pool_metadata.token1.address]
            token_addresses = [Web3.to_checksum_address(token_address) for token_address in token_addresses]

            for token_address in token_addresses:
                project_id = task_type.format(tokenAddress=token_address, Namespace=settings.namespace)
                logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | Attempting to get project ID: {project_id} for token: {token_address}")
                # get the last finalized epoch
                last_finalized_epoch = await get_project_last_finalized_epoch(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, project_id,
                )
                logger.info(
                    f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | "
                    f"Token pools compute | Last finalized epoch: {last_finalized_epoch} for project ID: {project_id} "
                    f"for token: {token_address}"
                )
                if not last_finalized_epoch:
                    snapshot = UniswapTokenPoolsSnapshot(
                        pools={pool_address: pool_metadata}
                    )
                    logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | Token pools compute | No last finalized epoch found for project ID: {project_id} for token: {token_address}. Creating snapshot with pool metadata: {snapshot}")
                    snapshots.append((project_id, snapshot))
                else:
                    # get the snapshot for the last finalized epoch
                    snapshot = await get_project_epoch_snapshot(
                        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, last_finalized_epoch, project_id,
                    )
                    logger.info(
                        f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | "
                        f"Token pools compute | Snapshot for project ID: {project_id} for token: {token_address} at last finalized epoch: {last_finalized_epoch} found: {snapshot}"
                    )
                    if snapshot:
                        snapshot = UniswapTokenPoolsSnapshot(**snapshot)
                        if pool_address not in snapshot.pools:
                            snapshot.pools[pool_address] = pool_metadata
                    else:
                        snapshot = UniswapTokenPoolsSnapshot(
                            pools={}
                        )
                        snapshot.pools[pool_address] = pool_metadata
                    logger.info(f"Epoch {epoch.begin}-{epoch.end} | Token pools compute | Pool {pool_address} | Token {token_address} | Token pools compute | Token pools snapshot generated: {snapshot}")
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
