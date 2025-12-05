from typing import List, Tuple
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper

from computes.utils.models.message_models import (
    EpochBaseSnapshot,
    AllUniswapTradesSnapshot
)
from snapshotter.settings.config import settings
from snapshotter.utils.models.message_models import CalculateAggregateMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.data_utils import get_submission_data_bulk
from computes.utils.models.message_models import UniswapTradesSnapshot


class AllTradesProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and storing trade volume for Uniswap pairs.
    
    This class handles the computation and storage of trade volume data for Uniswap V3 pools
    within a given epoch. It processes trade events, calculates volumes, and creates snapshots
    of trading activity for each active pool.
    """

    def __init__(self) -> None:
        """
        Initialize the processor with a logger instance.
        """
        self._logger = logger.bind(module="TradeVolumeProcessor")

    async def compute(
        self,
        msg_obj: CalculateAggregateMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> List[Tuple[str, AllUniswapTradesSnapshot]]:
        """
        Compute the trade volume for Uniswap pairs within the given epoch.

        This method processes trade events for all active pools in the epoch, calculates
        trade volumes, and creates snapshots of trading activity. It handles:
        - Fetching block details and active pools
        - Processing trade events for each pool
        - Calculating trade volumes and USD values
        - Creating snapshots of trading activity

        Args:
            epoch (SnapshotProcessMessage): The epoch information containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching and data storage.
            rpc_helper (RpcHelper): RPC helper for main blockchain interactions.
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Protocol state contract instance.
            task_type (str): The task type string for formatting the snapshot key.

        Returns:
            List[Tuple[str, UniswapTradesSnapshot]]: List of (snapshot_key, snapshot_data) pairs.
        """
        
        epoch_snapshot_model = EpochBaseSnapshot(
            begin=msg_obj.begin,
            end=msg_obj.end,
        )
        aggregate_project_id = task_type.format(Namespace=settings.namespace, dataMarketAddress=settings.data_market)
        
        all_uniswap_trades_snapshot = AllUniswapTradesSnapshot(
            epoch=epoch_snapshot_model,
            tradeData={},
            previousSnapshots=[]
        )

        snapshots = []
        # fetch all snapshots from ipfs
        all_cids = [cid for _, cid in msg_obj.processed_message.payload]
        all_project_ids = [project_id for project_id, _ in msg_obj.processed_message.payload]
        all_snapshot_data = await get_submission_data_bulk(
            redis_conn,
            all_cids,
            ipfs_reader,
            None,
            ensure_complete=False,  # Allow partial data to handle missing snapshots
        )

        parsed_snapshot_data = []
        successful_indices = []  # Track which indices have valid data

        for i, snapshot_data in enumerate(all_snapshot_data):
            if snapshot_data and snapshot_data != {}:  # Check for valid data
                try:
                    snapshot = UniswapTradesSnapshot(**snapshot_data)
                    snapshot.previousSnapshots = []
                    parsed_snapshot_data.append(snapshot)
                    successful_indices.append(i)
                except Exception as e:
                    self._logger.warning(f"Failed to parse snapshot data for index {i}: {e}")
            else:
                self._logger.warning(f"Missing or empty snapshot data for index {i}")

        # Only process successful snapshots
        for idx in successful_indices:
            project_id = all_project_ids[idx]
            snapshot_data = parsed_snapshot_data[successful_indices.index(idx)]
            pair_address = project_id.split(":")[1]
            all_uniswap_trades_snapshot.tradeData[pair_address] = snapshot_data

        self._logger.info(f"Successfully processed {len(successful_indices)} out of {len(all_cids)} pool snapshots")

        snapshots.append((aggregate_project_id, all_uniswap_trades_snapshot))
        return snapshots