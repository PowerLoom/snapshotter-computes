import asyncio
import time
from typing import List

import pydantic
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import ProjectTypeProcessingCompleteMessage
from snapshotter.utils.models.message_models import SnapshotSubmittedMessageLite
from snapshotter.utils.rpc import RpcHelper

from ..utils.models.message_models import UniswapTradesAggregateSnapshot


class AggregateTradeVolumeProcessor(GenericProcessorAggregate):

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateTradeVolumeProcessor7d')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesAggregateSnapshot,
    ):

        previous_aggregate_snapshot.totalTrade += current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee += current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume += current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume += current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD += current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD += current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    def _remove_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesAggregateSnapshot,
    ):

        previous_aggregate_snapshot.totalTrade -= current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee -= current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume -= current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume -= current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD -= current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD -= current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    async def _compute_single(
        self,
        submitted_snapshot: SnapshotSubmittedMessageLite,
        epoch_id: int,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ):
        self._logger.info(f'Building 7 day trade volume aggregate snapshot against {submitted_snapshot}')

        aggregate_snapshot = UniswapTradesAggregateSnapshot(
            epochId=epoch_id,
        )
        # 24h snapshots fetches
        snapshot_tasks = list()
        self._logger.debug('fetching 24hour aggregates spaced out by 1 day over 7 days...')
        count = 1
        self._logger.debug(
            'fetch # {}: queueing task for 24h aggregate snapshot for project ID {}'
            ' at currently received epoch ID {} with snasphot CID {}',
            count, submitted_snapshot.projectId, epoch_id, submitted_snapshot.snapshotCid,
        )

        snapshot_tasks.append(
            get_submission_data(
                redis, submitted_snapshot.snapshotCid, ipfs_reader, submitted_snapshot.projectId,
            ),
        )

        seek_stop_flag = False
        head_epoch = epoch_id
        # 2. if not extrapolated, attempt to seek further back
        while not seek_stop_flag and count < 7:
            tail_epoch_id, seek_stop_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, head_epoch, 86400, submitted_snapshot.projectId,
            )
            count += 1
            snapshot_tasks.append(
                get_project_epoch_snapshot(
                    redis, protocol_state_contract, anchor_rpc_helper,
                    ipfs_reader, tail_epoch_id, submitted_snapshot.projectId,
                ),
            )
            head_epoch = tail_epoch_id - 1

        all_snapshots = await asyncio.gather(*snapshot_tasks, return_exceptions=True)
        self._logger.debug(
            'for 7d aggregated trade volume calculations: fetched {} '
            '24h aggregated trade volume snapshots for project ID {}: {}',
            len(all_snapshots), submitted_snapshot.projectId, all_snapshots,
        )
        complete_flags = []
        for single_24h_snapshot in all_snapshots:
            if not isinstance(single_24h_snapshot, BaseException):
                try:
                    snapshot = UniswapTradesAggregateSnapshot.parse_obj(single_24h_snapshot)
                    complete_flags.append(snapshot.complete)
                except pydantic.ValidationError:
                    pass
                else:
                    aggregate_snapshot = self._add_aggregate_snapshot(aggregate_snapshot, snapshot)

        if not all(complete_flags) or count < 7:
            aggregate_snapshot.complete = False
        else:
            aggregate_snapshot.complete = True

        return aggregate_snapshot

    async def compute(
        self,
        msg_obj: ProjectTypeProcessingCompleteMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_ids: List[str],

    ):
        snapshots = []
        snapshot_tasks = []
        for submitted_snapshot, project_id in zip(msg_obj.snapshotsSubmitted, project_ids):

            snapshot_tasks.append(self._compute_single(
                submitted_snapshot, msg_obj.epochId, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            ))
        snapshots_generated = await asyncio.gather(*snapshot_tasks, return_exceptions=True)

        for data_source_contract_address, snapshot in zip(project_ids, snapshots_generated):
            if isinstance(snapshot, Exception):
                self._logger.error(f'Error while computing trade volume snapshot: {snapshot}')
                continue
            snapshots.append(
                (
                    data_source_contract_address,
                    snapshot,
                ),
            )
            
        self._logger.debug(f'trade volume aggregation, computation end time {time.time()}')
        return snapshots