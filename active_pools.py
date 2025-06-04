from typing import List, Tuple

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.settings.config import settings
from computes.utils.models.message_models import ActivePoolsSnapshot, EpochBaseSnapshot
from ipfs_client.main import AsyncIPFSClient


class ActivePoolsProcessor(GenericProcessorSnapshot):
    """
    Processor for tracking and snapshotting active Uniswap V3 pools within a given epoch.
    
    This processor aggregates data about which pools were active during the specified epoch
    by analyzing Redis data that tracks pool activity per block. It creates snapshots that
    include the frequency of pool activity and the epoch range.
    """

    def __init__(self) -> None:
        """Initialize the processor with a module-specific logger."""
        self._logger = logger.bind(module="ActivePoolsProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> List[Tuple[str, ActivePoolsSnapshot]]:
        """
        Compute active pools snapshot for the given epoch.

        This method aggregates pool activity data from Redis for each block in the epoch,
        tracking how frequently each pool appears in the active pools set.

        Args:
            epoch (SnapshotProcessMessage): The epoch information containing begin and end block numbers
            redis_conn (aioredis.Redis): Redis connection for accessing pool activity data
            rpc_helper (RpcHelper): RPC helper for blockchain interactions
            anchor_rpc_helper (RpcHelper): Anchor chain RPC helper
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data
            protocol_state_contract: Protocol state contract instance
            task_type (str): Format string for task identification

        Returns:
            List[Tuple[str, ActivePoolsSnapshot]]: List containing a tuple of task identifier and snapshot
        """
        
        # Get epoch boundaries
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        # Initialize dictionary to track pool activity frequency
        active_pools = {}
        
        # Iterate through each block in the epoch
        for block_number in range(min_chain_height, max_chain_height + 1):
            # Construct Redis key for active pools in this block
            key = f"active_pools_per_block:{block_number}:{settings.namespace}"
            
            # Retrieve all pools and their activity scores for this block
            block_active_pools = await redis_conn.zrange(key, 0, -1, withscores=True)
            
            # Process each pool's activity data
            for pool_address, score in block_active_pools:
                # Decode and normalize pool address
                pool_address = pool_address.decode('utf-8')
                pool_address = Web3.to_checksum_address(pool_address)
                
                # Accumulate activity score for this pool
                if pool_address not in active_pools:
                    active_pools[pool_address] = 0
                active_pools[pool_address] += int(score)
        
        # Log the aggregated pool activity data
        self._logger.info(f"Active pools: {active_pools}")
        
        # Create snapshot with pool activity data and epoch information
        snapshot = ActivePoolsSnapshot(
            pools=active_pools,
            epoch=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
        )
        self._logger.info(f"Snapshot: {snapshot}")

        # Return task identifier and snapshot
        return [(task_type.format(Namespace=settings.namespace), snapshot)]