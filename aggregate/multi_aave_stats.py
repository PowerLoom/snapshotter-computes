from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from computes.utils.models.message_models import AaveMarketStatsSnapshot
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot


class AggreagateMarketStatsProcessor(GenericProcessorAggregate):
    """
    Processor for aggregating market statistics across multiple Aave pools.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateMarketStatsProcessor')

    async def compute(
        self,
        msg_obj: PowerloomCalculateAggregateMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ):
        """
        Compute aggregated market statistics for Aave pools.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): Message object containing calculation details.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper for the source chain.
            anchor_rpc_helper (RpcHelper): RPC helper for the anchor chain.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract for accessing protocol state.
            project_id (str): ID of the project.

        Returns:
            AaveMarketStatsSnapshot: Aggregated market statistics snapshot.
        """
        self._logger.info(f'Calculating market stats for {msg_obj}')

        epoch_id = msg_obj.epochId
        complete = False

        snapshot_mapping = {}

        # Fetch snapshot data for all messages
        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        # Parse snapshot data
        for msg, data in zip(msg_obj.messages, snapshot_data):
            if not data:
                self._logger.debug(f'Retrieved snapshot with no data: {msg}')
                continue
            snapshot = AavePoolTotalAssetSnapshot.parse_obj(data)
            snapshot_mapping[msg.projectId] = snapshot

        # Initialize market data object
        stats_data = {
            'totalMarketSize': 0,
            'totalAvailable': 0,
            'totalBorrows': 0,
            'marketChange24h': 0,
            'availableChange24h': 0,
            'borrowChange24h': 0,
        }

        # Calculate total market statistics
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            max_epoch_block = snapshot.chainHeightRange.end

            stats_data['totalAvailable'] += snapshot.totalAToken[f'block{max_epoch_block}'].usd_supply - \
                snapshot.totalVariableDebt[f'block{max_epoch_block}'].usd_debt
            stats_data['totalBorrows'] += snapshot.totalVariableDebt[f'block{max_epoch_block}'].usd_debt
            stats_data['totalMarketSize'] += snapshot.totalAToken[f'block{max_epoch_block}'].usd_supply

        # Calculate 24-hour changes
        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )

        if not extrapolated_flag:
            previous_stats_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_stats_snapshot_data:
                previous_stats_snapshot = AaveMarketStatsSnapshot.parse_obj(previous_stats_snapshot_data)

                # Calculate percentage changes
                stats_data['marketChange24h'] = (stats_data['totalMarketSize'] - previous_stats_snapshot.totalMarketSize) / \
                    previous_stats_snapshot.totalMarketSize * 100

                stats_data['availableChange24h'] = (stats_data['totalAvailable'] - previous_stats_snapshot.totalAvailable) / \
                    previous_stats_snapshot.totalAvailable * 100

                stats_data['borrowChange24h'] = (stats_data['totalBorrows'] - previous_stats_snapshot.totalBorrows) / \
                    previous_stats_snapshot.totalBorrows * 100

                complete = True

        # Create and return the final snapshot
        aave_market_stats_snapshot = AaveMarketStatsSnapshot(
            epochId=epoch_id,
            totalMarketSize=stats_data['totalMarketSize'],
            totalAvailable=stats_data['totalAvailable'],
            totalBorrows=stats_data['totalBorrows'],
            marketChange24h=stats_data['marketChange24h'],
            availableChange24h=stats_data['availableChange24h'],
            borrowChange24h=stats_data['borrowChange24h'],
            complete=complete,
        )

        self._logger.info(f'Got market stats data: {aave_market_stats_snapshot}')

        return aave_market_stats_snapshot
