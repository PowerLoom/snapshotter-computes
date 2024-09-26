from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from computes.utils.models.message_models import UniswapPairTotalReservesSnapshot
from computes.utils.models.message_models import UniswapStatsSnapshot
from computes.utils.models.message_models import UniswapTradesAggregateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper


class AggreagateStatsProcessor(GenericProcessorAggregate):
    """
    A processor class for aggregating Uniswap statistics.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        """
        Initialize the AggreagateStatsProcessor.
        """
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateStatsProcessor')

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
        Compute aggregate Uniswap statistics.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): The message object containing calculation details.
            redis (aioredis.Redis): Redis client for caching and data retrieval.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper for specific blockchain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: The contract representing the protocol state.
            project_id (str): The ID of the project.

        Returns:
            UniswapStatsSnapshot: A snapshot of the computed Uniswap statistics.
        """
        self._logger.info(f'Calculating unswap stats for {msg_obj}')

        epoch_id = msg_obj.epochId

        snapshot_mapping = {}

        # Retrieve snapshot data in bulk
        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        complete_flags = []
        for msg, data in zip(msg_obj.messages, snapshot_data):
            if not data:
                continue
            if 'reserves' in msg.projectId:
                snapshot = UniswapPairTotalReservesSnapshot.parse_obj(data)
            elif 'volume' in msg.projectId:
                snapshot = UniswapTradesAggregateSnapshot.parse_obj(data)
                complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot

        # Initialize stats data
        stats_data = {
            'volume24h': 0,
            'tvl': 0,
            'fee24h': 0,
            'volumeChange24h': 0,
            'tvlChange24h': 0,
            'feeChange24h': 0,
        }

        # Iterate over all snapshots and generate stats data
        for snapshot_project_id, snapshot in snapshot_mapping.items():
            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end
                # Calculate Total Value Locked (TVL)
                stats_data['tvl'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}'] + \
                    snapshot.token1ReservesUSD[f'block{max_epoch_block}']
            elif 'volume' in snapshot_project_id:
                # Accumulate volume and fee data
                stats_data['volume24h'] += snapshot.totalTrade
                stats_data['fee24h'] += snapshot.totalFee

        # Retrieve the tail epoch for comparison
        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )

        if not extrapolated_flag:
            # Retrieve previous stats snapshot for comparison
            previous_stats_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_stats_snapshot_data:
                previous_stats_snapshot = UniswapStatsSnapshot.parse_obj(previous_stats_snapshot_data)

                # Calculate percentage changes
                stats_data['volumeChange24h'] = (stats_data['volume24h'] - previous_stats_snapshot.volume24h) / \
                    previous_stats_snapshot.volume24h * 100

                stats_data['tvlChange24h'] = (stats_data['tvl'] - previous_stats_snapshot.tvl) / \
                    previous_stats_snapshot.tvl * 100

                stats_data['feeChange24h'] = (stats_data['fee24h'] - previous_stats_snapshot.fee24h) / \
                    previous_stats_snapshot.fee24h * 100

        # Create the final stats snapshot
        stats_snapshot = UniswapStatsSnapshot(
            epochId=epoch_id,
            volume24h=stats_data['volume24h'],
            tvl=stats_data['tvl'],
            fee24h=stats_data['fee24h'],
            volumeChange24h=stats_data['volumeChange24h'],
            tvlChange24h=stats_data['tvlChange24h'],
            feeChange24h=stats_data['feeChange24h'],
        )

        # Set the complete flag based on all volume snapshots
        if not all(complete_flags):
            stats_snapshot.complete = False

        return stats_snapshot
