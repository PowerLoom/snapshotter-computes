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
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="ActivePoolsProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        # TODO: need clarity on this interface
        task_type: str,
    ) -> List[Tuple[str, ActivePoolsSnapshot]]:
        """
        Compute the total reserves for a Uniswap pair within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        Returns:
            Optional[Dict[str, Union[int, float]]]: Computed pair total reserves snapshot.
        """
        
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        # fetch all active pools for epoch from redis
        active_pools = {}
        for block_number in range(min_chain_height, max_chain_height + 1):
            # pipeline.zincrby(f"active_pools_per_block:{block_number}:{namespace}", 1, pool_address)
            key = f"active_pools_per_block:{block_number}:{settings.namespace}"
            # get all pools for block with score which is frequency of occurrence
            block_active_pools = await redis_conn.zrange(key, 0, -1, withscores=True)
            for pool_address, score in block_active_pools:
                pool_address = pool_address.decode('utf-8')
                pool_address = Web3.to_checksum_address(pool_address)
                if pool_address not in active_pools:
                    active_pools[pool_address] = 0
                active_pools[pool_address] += int(score)
        
        # sort active pools by score
        self._logger.info(f"Active pools: {active_pools}")
        snapshot = ActivePoolsSnapshot(
            pools=active_pools,
            epoch=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
        )
        self._logger.info(f"Snapshot: {snapshot}")

        return [(task_type.format(Namespace=settings.namespace), snapshot)]