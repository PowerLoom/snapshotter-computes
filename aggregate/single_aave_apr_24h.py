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
    """
    Processor for aggregating APR data for a single Aave pool over a 24-hour period.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateSingleAprProcessor24hr')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: AaveAprAggregateSnapshot,
        current_snapshot: AaveAprAggregateSnapshot,
        sample_size: int,
    ) -> tuple[AaveAprAggregateSnapshot, int]:
        """
        Add a new snapshot to the aggregate and update rolling averages.

        Args:
            previous_aggregate_snapshot (AaveAprAggregateSnapshot): The previous aggregate snapshot.
            current_snapshot (AaveAprAggregateSnapshot): The current snapshot to add.
            sample_size (int): The current sample size.

        Returns:
            tuple[AaveAprAggregateSnapshot, int]: Updated aggregate snapshot and new sample size.
        """
        # Update rolling averages
        previous_aggregate_snapshot.avgLiquidityRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgLiquidityRate, current_snapshot.avgLiquidityRate, sample_size)
        previous_aggregate_snapshot.avgVariableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgVariableRate, current_snapshot.avgVariableRate, sample_size)
        previous_aggregate_snapshot.avgStableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgStableRate, current_snapshot.avgStableRate, sample_size)
        previous_aggregate_snapshot.avgUtilizationRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgUtilizationRate, current_snapshot.avgUtilizationRate, sample_size)

        sample_size += 1

        return previous_aggregate_snapshot, sample_size

    def _update_rolling_average(self, current_avg: float, new_value: float, sample_size: int) -> float:
        """
        Update a rolling average with a new value.

        Args:
            current_avg (float): The current average.
            new_value (float): The new value to add to the average.
            sample_size (int): The current sample size.

        Returns:
            float: The updated rolling average.
        """
        updated_avg = (current_avg * sample_size + new_value) / (sample_size + 1)
        return truncate(updated_avg, 5)

    async def compute(
        self,
        msg_obj: PowerloomSnapshotSubmittedMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ) -> AaveAprAggregateSnapshot:
        """
        Compute the aggregate APR snapshot for a 24-hour period.

        Args:
            msg_obj (PowerloomSnapshotSubmittedMessage): The snapshot submission message.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper for the source chain.
            anchor_rpc_helper (RpcHelper): RPC helper for the anchor chain.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract for accessing protocol state.
            project_id (str): ID of the project.

        Returns:
            AaveAprAggregateSnapshot: The computed aggregate APR snapshot.
        """
        self._logger.info(f'Building 24hr apr average aggregate snapshot against {msg_obj}')

        aggregate_snapshot = AaveAprAggregateSnapshot(
            epochId=msg_obj.epochId,
        )

        # Fetch 6-hour snapshots
        snapshot_tasks = list()
        self._logger.debug('fetching 4 6hr aggregates spaced out by 6hrs over 1 day...')
        count = 1
        self._logger.debug(
            'fetch # {}: queueing task for 6hr aggregate snapshot for project ID {}'
            ' at currently received epoch ID {} with snapshot CID {}',
            count, msg_obj.projectId, msg_obj.epochId, msg_obj.snapshotCid,
        )

        # Fetch the current snapshot
        snapshot_tasks.append(
            get_submission_data(
                redis, msg_obj.snapshotCid, ipfs_reader, msg_obj.projectId,
            ),
        )

        seek_stop_flag = False
        head_epoch = msg_obj.epochId
        # Fetch previous snapshots
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
        # Process fetched snapshots
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
