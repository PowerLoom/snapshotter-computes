import asyncio

import pydantic
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.rpc import RpcHelper
from ..utils.helpers import truncate
from ..utils.models.message_models import AaveAprAggregateSnapshot


class AggreagateSingleAprProcessor(GenericProcessorAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateSingleAprProcessor24hr')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: AaveAprAggregateSnapshot,
        current_snapshot: AaveAprAggregateSnapshot,
        sample_size,
    ):

        # increment rolling averages
        previous_aggregate_snapshot.avgLiquidityRate = sample_size * \
            previous_aggregate_snapshot.avgLiquidityRate + current_snapshot.avgLiquidityRate
        previous_aggregate_snapshot.avgLiquidityRate /= sample_size + 1
        previous_aggregate_snapshot.avgLiquidityRate = truncate(
            previous_aggregate_snapshot.avgLiquidityRate, 
            5,
        )

        previous_aggregate_snapshot.avgVariableRate = sample_size * \
            previous_aggregate_snapshot.avgVariableRate + current_snapshot.avgVariableRate
        previous_aggregate_snapshot.avgVariableRate /= sample_size + 1
        previous_aggregate_snapshot.avgVariableRate = truncate(
            previous_aggregate_snapshot.avgVariableRate,
            5,
        )

        previous_aggregate_snapshot.avgStableRate = sample_size * \
            previous_aggregate_snapshot.avgStableRate + current_snapshot.avgStableRate
        previous_aggregate_snapshot.avgStableRate /= sample_size + 1
        previous_aggregate_snapshot.avgStableRate = truncate(
            previous_aggregate_snapshot.avgStableRate, 
            5,
        )

        previous_aggregate_snapshot.avgUtilizationRate = sample_size * \
            previous_aggregate_snapshot.avgUtilizationRate + current_snapshot.avgUtilizationRate
        previous_aggregate_snapshot.avgUtilizationRate /= sample_size + 1
        previous_aggregate_snapshot.avgUtilizationRate = truncate(
            previous_aggregate_snapshot.avgUtilizationRate,
            5,
        )

        sample_size += 1

        return previous_aggregate_snapshot, sample_size

    def _remove_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: AaveAprAggregateSnapshot,
        current_snapshot: AaveAprAggregateSnapshot,
        sample_size,
    ):

        # decrement rolling averages
        previous_aggregate_snapshot.avgLiquidityRate = sample_size * \
            previous_aggregate_snapshot.avgLiquidityRate - current_snapshot.avgLiquidityRate
        previous_aggregate_snapshot.avgLiquidityRate /= sample_size - 1
        previous_aggregate_snapshot.avgLiquidityRate = truncate(
            previous_aggregate_snapshot.avgLiquidityRate, 
            5,
        )

        previous_aggregate_snapshot.avgVariableRate = sample_size * \
            previous_aggregate_snapshot.avgVariableRate - current_snapshot.avgVariableRate
        previous_aggregate_snapshot.avgVariableRate /= sample_size - 1
        previous_aggregate_snapshot.avgVariableRate = truncate(
            previous_aggregate_snapshot.avgVariableRate,
            5,
        )

        previous_aggregate_snapshot.avgStableRate = sample_size * \
            previous_aggregate_snapshot.avgStableRate - current_snapshot.avgStableRate
        previous_aggregate_snapshot.avgStableRate /= sample_size - 1
        previous_aggregate_snapshot.avgStableRate = truncate(
            previous_aggregate_snapshot.avgStableRate, 
            5,
        )

        previous_aggregate_snapshot.avgUtilizationRate = sample_size * \
            previous_aggregate_snapshot.avgUtilizationRate - current_snapshot.avgUtilizationRate
        previous_aggregate_snapshot.avgUtilizationRate /= sample_size - 1
        previous_aggregate_snapshot.avgUtilizationRate = truncate(
            previous_aggregate_snapshot.avgUtilizationRate,
            5,
        )

        sample_size -= 1

        return previous_aggregate_snapshot, sample_size

    async def compute(
        self,
        msg_obj: PowerloomSnapshotSubmittedMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,

    ):
        self._logger.info(f'Building 24hr apr average aggregate snapshot against {msg_obj}')

        aggregate_snapshot = AaveAprAggregateSnapshot(
            epochId=msg_obj.epochId,
        )
        # 6h snapshots fetches
        snapshot_tasks = list()
        self._logger.debug('fetching 4 6hr aggregates spaced out by 6hrs over 1 day...')
        count = 1
        self._logger.debug(
            'fetch # {}: queueing task for 6hr aggregate snapshot for project ID {}'
            ' at currently received epoch ID {} with snasphot CID {}',
            count, msg_obj.projectId, msg_obj.epochId, msg_obj.snapshotCid,
        )

        snapshot_tasks.append(
            get_submission_data(
                redis, msg_obj.snapshotCid, ipfs_reader, msg_obj.projectId,
            ),
        )

        seek_stop_flag = False
        head_epoch = msg_obj.epochId
        # 2. if not extrapolated, attempt to seek further back
        while not seek_stop_flag and count < 4:
            tail_epoch_id, seek_stop_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, head_epoch, 21600, msg_obj.projectId,
            )
            count += 1
            snapshot_tasks.append(
                get_project_epoch_snapshot(
                    redis, protocol_state_contract, anchor_rpc_helper,
                    ipfs_reader, tail_epoch_id, msg_obj.projectId,
                ),
            )
            head_epoch = tail_epoch_id - 1

        all_snapshots = await asyncio.gather(*snapshot_tasks, return_exceptions=True)
        self._logger.debug(
            'for 24hr aggregated apr avg calculations: fetched {} '
            '6hr aggregated apr avg snapshots for project ID {}: {}',
            len(all_snapshots), msg_obj.projectId, all_snapshots,
        )

        complete_flags = []
        sample_size = 0
        for single_6h_snapshot in all_snapshots:
            if not isinstance(single_6h_snapshot, BaseException):
                try:
                    snapshot = AaveAprAggregateSnapshot.parse_obj(single_6h_snapshot)
                    complete_flags.append(snapshot.complete)
                except pydantic.ValidationError:
                    pass
                else:
                    aggregate_snapshot, sample_size = self._add_aggregate_snapshot(
                        aggregate_snapshot, snapshot, sample_size,
                    )
                    if snapshot.epochId == msg_obj.epochId:
                        aggregate_snapshot.timestamp = snapshot.timestamp

        self._logger.debug(f'Final sample size: {sample_size} for project: {project_id}')

        if not all(complete_flags) or count < 4:
            aggregate_snapshot.complete = False
        else:
            aggregate_snapshot.complete = True
        return aggregate_snapshot
