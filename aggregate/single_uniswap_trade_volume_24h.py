import asyncio
import json
import time
from typing import List

from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot_bulk
from snapshotter.utils.data_utils import get_project_last_finalized_cid_and_epoch
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import ProjectTypeProcessingCompleteMessage
from snapshotter.utils.models.message_models import SnapshotSubmittedMessageLite
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.redis.redis_keys import submitted_base_snapshots_key
from snapshotter.utils.rpc import RpcHelper

from ..utils.models.message_models import UniswapTradesAggregateSnapshot
from ..utils.models.message_models import UniswapTradesSnapshot


class AggregateTradeVolumeProcessor(GenericProcessorAggregate):

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateTradeVolumeProcessor24h')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesSnapshot,
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
        current_snapshot: UniswapTradesSnapshot,
    ):

        previous_aggregate_snapshot.totalTrade -= current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee -= current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume -= current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume -= current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD -= current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD -= current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    async def _calculate_from_scratch(
        self,
        msg_obj: SnapshotSubmittedMessageLite,
        epoch_id: int,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ):
        calculate_from_scratch_in_progress = await redis.get(f'calculate_from_scratch:{project_id}')
        if calculate_from_scratch_in_progress:
            self._logger.info('calculate_from_scratch already in progress, skipping')
            return

        self._logger.info('building aggregate from scratch')
        await redis.set(
            name=f'calculate_from_scratch:{project_id}',
            value='true',
            ex=300,
        )
        # source project tail epoch
        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, epoch_id, 86400, msg_obj.projectId,
        )

        # for the first epoch, using submitted cid
        current_epoch_underlying_data = await get_submission_data(
            redis, msg_obj.snapshotCid, ipfs_reader, project_id,
        )

        snapshots_data = await get_project_epoch_snapshot_bulk(
            redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
            tail_epoch_id, epoch_id - 1, msg_obj.projectId,
        )

        aggregate_snapshot = UniswapTradesAggregateSnapshot.parse_obj({'epochId': epoch_id})
        if extrapolated_flag:
            aggregate_snapshot.complete = False
        if current_epoch_underlying_data:
            current_snapshot = UniswapTradesSnapshot.parse_obj(current_epoch_underlying_data)
            aggregate_snapshot = self._add_aggregate_snapshot(aggregate_snapshot, current_snapshot)

        for snapshot_data in snapshots_data:
            if snapshot_data:
                snapshot = UniswapTradesSnapshot.parse_obj(snapshot_data)
                aggregate_snapshot = self._add_aggregate_snapshot(aggregate_snapshot, snapshot)

        await redis.delete(f'calculate_from_scratch:{project_id}')

        return aggregate_snapshot

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
        self._logger.info(f'Building trade volume aggregate snapshot for {submitted_snapshot}')

        project_last_finalized_cid, project_last_finalized_epoch = await get_project_last_finalized_cid_and_epoch(
            redis, protocol_state_contract, anchor_rpc_helper, project_id,
        )

        if not project_last_finalized_cid:
            self._logger.info('project_last_finalized_cid is None, building aggregate from scratch')
            return await self._calculate_from_scratch(
                submitted_snapshot, epoch_id, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            )

        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, epoch_id, 86400, submitted_snapshot.projectId,
        )

        if extrapolated_flag:
            aggregate_complete_flag = False
        else:
            aggregate_complete_flag = True
        if project_last_finalized_epoch == 0:
            self._logger.info('project_last_finalized_epoch is 0, building aggregate from scratch')
            return await self._calculate_from_scratch(
                submitted_snapshot, epoch_id, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            )

        if project_last_finalized_epoch <= tail_epoch_id:
            self._logger.error('last finalized epoch is too old, building aggregate from scratch')
            return await self._calculate_from_scratch(
                submitted_snapshot, epoch_id, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            )

        project_last_finalized_data = await get_submission_data(
            redis, project_last_finalized_cid, ipfs_reader, project_id,
        )

        if not project_last_finalized_data:
            self._logger.info('project_last_finalized_data is None, building aggregate from scratch')
            return await self._calculate_from_scratch(
                submitted_snapshot, epoch_id, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            )

        aggregate_snapshot = UniswapTradesAggregateSnapshot.parse_obj(project_last_finalized_data)
        # updating epochId to current epoch
        aggregate_snapshot.epochId = epoch_id

        _, base_project_last_finalized_epoch = await get_project_last_finalized_cid_and_epoch(
            redis, protocol_state_contract, anchor_rpc_helper, submitted_snapshot.projectId,
        )

        if base_project_last_finalized_epoch and project_last_finalized_epoch < base_project_last_finalized_epoch:
            # fetch base finalized snapshots if they exist and are within 5 epochs of current epoch
            base_finalized_snapshot_range = (
                project_last_finalized_epoch + 1,
                base_project_last_finalized_epoch,
            )

            base_finalized_snapshots = await get_project_epoch_snapshot_bulk(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
                base_finalized_snapshot_range[0], base_finalized_snapshot_range[1], submitted_snapshot.projectId,
            )
        else:
            base_finalized_snapshots = []
            base_finalized_snapshot_range = (0, project_last_finalized_epoch)

        base_unfinalized_tasks = []
        for epoch_id in range(base_finalized_snapshot_range[1] + 1, epoch_id + 1):
            base_unfinalized_tasks.append(
                redis.get(submitted_base_snapshots_key(epoch_id=epoch_id, project_id=submitted_snapshot.projectId)),
            )

        base_unfinalized_snapshots_raw = await asyncio.gather(*base_unfinalized_tasks, return_exceptions=True)

        base_unfinalized_snapshots = []
        for snapshot_data in base_unfinalized_snapshots_raw:
            # check if not exception and not None
            if not isinstance(snapshot_data, Exception) and snapshot_data:
                base_unfinalized_snapshots.append(
                    json.loads(snapshot_data),
                )
            else:
                self._logger.error(
                    f'Error fetching base unfinalized snapshot, cancelling aggregation for epoch {epoch_id}',
                )
                return

        base_snapshots = base_finalized_snapshots + base_unfinalized_snapshots

        for snapshot_data in base_snapshots:
            if snapshot_data:
                snapshot = UniswapTradesSnapshot.parse_obj(snapshot_data)
                aggregate_snapshot = self._add_aggregate_snapshot(aggregate_snapshot, snapshot)

        # Remove from tail if needed
        tail_epochs_to_remove = []
        for epoch_id in range(project_last_finalized_epoch, epoch_id):
            tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, epoch_id, 86400, submitted_snapshot.projectId,
            )
            if not extrapolated_flag:
                tail_epochs_to_remove.append(tail_epoch_id)
        if tail_epochs_to_remove:
            tail_epoch_snapshots = await get_project_epoch_snapshot_bulk(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
                tail_epochs_to_remove[0], tail_epochs_to_remove[-1], submitted_snapshot.projectId,
            )

            for snapshot_data in tail_epoch_snapshots:
                if snapshot_data:
                    snapshot = UniswapTradesSnapshot.parse_obj(snapshot_data)
                    aggregate_snapshot = self._remove_aggregate_snapshot(aggregate_snapshot, snapshot)

        if aggregate_complete_flag:
            aggregate_snapshot.complete = True
        else:
            aggregate_snapshot.complete = False
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