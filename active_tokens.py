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
    Processor for tracking and aggregating active token usage across blockchain blocks.
    
    This processor analyzes token activity within specified epochs by:
    1. Retrieving token usage data from Redis for each block in the epoch
    2. Aggregating token activity scores across blocks
    3. Creating snapshots of token activity for the epoch
    """

    def __init__(self) -> None:
        """Initialize the processor with a module-specific logger."""
        self._logger = logger.bind(module="ActiveTokensProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> List[Tuple[str, ActiveTokensSnapshot]]:
        """
        Compute active token usage statistics for the given epoch.

        Args:
            epoch (SnapshotProcessMessage): Epoch information containing begin and end block numbers
            redis_conn (aioredis.Redis): Redis connection for retrieving token activity data
            rpc_helper (RpcHelper): RPC helper for blockchain interactions
            anchor_rpc_helper (RpcHelper): Anchor RPC helper for additional blockchain interactions
            ipfs_reader (AsyncIPFSClient): IPFS client for data retrieval
            protocol_state_contract: Contract interface for protocol state queries
            task_type (str): Format string for task identification

        Returns:
            List[Tuple[str, ActiveTokensSnapshot]]: List containing a tuple of task identifier and token activity snapshot
        """
        
        # Extract epoch boundaries
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        # Initialize dictionary to store aggregated token activity
        active_tokens = {}
        
        # Process each block in the epoch
        for block_number in range(min_chain_height, max_chain_height + 1):
            # Construct Redis key for block's token activity
            key = f"active_tokens_per_block:{block_number}:{settings.namespace}"
            
            # Retrieve token activity data with scores (frequency of occurrence)
            block_active_tokens = await redis_conn.zrange(key, 0, -1, withscores=True)
            
            # Process each token's activity in the block
            for token_address, score in block_active_tokens:
                # Decode and normalize token address
                token_address = token_address.decode('utf-8')
                token_address = Web3.to_checksum_address(token_address)
                
                # Aggregate token activity scores
                if token_address not in active_tokens:
                    active_tokens[token_address] = 0
                active_tokens[token_address] += int(score)
        
        # Log aggregated token activity
        self._logger.info(f"Active tokens: {active_tokens}")
        
        # Create snapshot of token activity for the epoch
        snapshot = ActiveTokensSnapshot(
            tokens=active_tokens,
            epoch=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
        )
        self._logger.info(f"Snapshot: {snapshot}")

        # Return task identifier and snapshot
        return [(task_type.format(Namespace=settings.namespace, dataMarketAddress=settings.data_market), snapshot)]