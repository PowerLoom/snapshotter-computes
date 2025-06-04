from typing import List, Tuple

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.settings.config import settings
from computes.utils.models.message_models import ActiveTokensSnapshot, EpochBaseSnapshot
from ipfs_client.main import AsyncIPFSClient


class ActiveTokensProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="ActiveTokensProcessor")

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
    ) -> List[Tuple[str, ActiveTokensSnapshot]]:
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
        active_tokens = {}
        for block_number in range(min_chain_height, max_chain_height + 1):
            # pipeline.zincrby(f"active_tokens_per_block:{block_number}:{namespace}", 1, token_address)
            key = f"active_tokens_per_block:{block_number}:{settings.namespace}"
            # get all pools for block with score which is frequency of occurrence
            block_active_tokens = await redis_conn.zrange(key, 0, -1, withscores=True)
            for token_address, score in block_active_tokens:
                token_address = token_address.decode('utf-8')
                token_address = Web3.to_checksum_address(token_address)
                if token_address not in active_tokens:
                    active_tokens[token_address] = 0
                active_tokens[token_address] += int(score)
        
        # sort active pools by score
        self._logger.info(f"Active tokens: {active_tokens}")
        snapshot = ActiveTokensSnapshot(
            tokens=active_tokens,
            epoch=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
        )
        self._logger.info(f"Snapshot: {snapshot}")

        return [(task_type.format(Namespace=settings.namespace), snapshot)]