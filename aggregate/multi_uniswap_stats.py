from typing import List

from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import CalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from ..utils.models.message_models import UniswapPairTotalReservesSnapshot
from ..utils.models.message_models import UniswapStatsSnapshot
from ..utils.models.message_models import UniswapTradesAggregateSnapshot


class AggregateStatsProcessor(GenericProcessorAggregate):

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateStatsProcessor')

    async def compute(
        self,
        msg_obj: CalculateAggregateMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_ids: List[str],


    ):
        self._logger.info(f'Calculating unswap stats for {msg_obj}')

        epoch_id = msg_obj.epochId
        project_id = project_ids[0]
        snapshot_mapping = {}

        submitted_snapshots = []
        for msg in msg_obj.messages:
            for snapshot in msg.snapshotsSubmitted:
                submitted_snapshots.append(snapshot)

        snapshot_data = await get_submission_data_bulk(
            redis, [snapshot.snapshotCid for snapshot in submitted_snapshots], ipfs_reader,
            [snapshot.projectId for snapshot in submitted_snapshots],
        )

        complete_flags = []
        for msg, data in zip(submitted_snapshots, snapshot_data):
            if not data:
                continue
            if 'reserves' in msg.projectId:
                snapshot = UniswapPairTotalReservesSnapshot.parse_obj(data)
            elif 'volume' in msg.projectId:
                snapshot = UniswapTradesAggregateSnapshot.parse_obj(data)
                complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot

        stats_data = {
            'volume24h': 0,
            'tvl': 0,
            'fee24h': 0,
            'volumeChange24h': 0,
            'tvlChange24h': 0,
            'feeChange24h': 0,
        }
        # iterate over all snapshots and generate stats data
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]

            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end

                stats_data['tvl'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}'] + \
                    snapshot.token1ReservesUSD[f'block{max_epoch_block}']

            elif 'volume' in snapshot_project_id:
                stats_data['volume24h'] += snapshot.totalTrade
                stats_data['fee24h'] += snapshot.totalFee


        # source project tail epoch
        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )
        if not extrapolated_flag:
            previous_stats_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_stats_snapshot_data:
                previous_stats_snapshot = UniswapStatsSnapshot.parse_obj(previous_stats_snapshot_data)

                # calculate change in percentage
                stats_data['volumeChange24h'] = (stats_data['volume24h'] - previous_stats_snapshot.volume24h) / \
                    previous_stats_snapshot.volume24h * 100

                stats_data['tvlChange24h'] = (stats_data['tvl'] - previous_stats_snapshot.tvl) / \
                    previous_stats_snapshot.tvl * 100

                stats_data['feeChange24h'] = (stats_data['fee24h'] - previous_stats_snapshot.fee24h) / \
                    previous_stats_snapshot.fee24h * 100

        stats_snapshot = UniswapStatsSnapshot(
            epochId=epoch_id,
            volume24h=stats_data['volume24h'],
            tvl=stats_data['tvl'],
            fee24h=stats_data['fee24h'],
            volumeChange24h=stats_data['volumeChange24h'],
            tvlChange24h=stats_data['tvlChange24h'],
            feeChange24h=stats_data['feeChange24h'],
        )

        if not all(complete_flags):
            stats_snapshot.complete = False

        return [(project_id, stats_snapshot)]