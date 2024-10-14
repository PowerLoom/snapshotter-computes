import asyncio
import json

from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot_bulk
from snapshotter.utils.data_utils import get_project_first_epoch
from snapshotter.utils.data_utils import get_source_chain_block_time
from snapshotter.utils.data_utils import get_source_chain_epoch_size
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.redis.redis_keys import submitted_base_snapshots_key
from snapshotter.utils.rpc import RpcHelper
from computes.utils.helpers import truncate
from computes.utils.constants import RAY
from computes.utils.models.message_models import AaveAprAggregateSnapshot
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot


class AggreagateSingleAprProcessor(GenericProcessorAggregate):
    """
    Processor for aggregating APR data for a single Aave pool over a 6-hour period.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateSingleAprProcessor')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: AaveAprAggregateSnapshot,
        current_snapshot: AavePoolTotalAssetSnapshot,
        sample_size: int,
    ) -> tuple[AaveAprAggregateSnapshot, int]:
        """
        Add a new snapshot to the aggregate and update rolling averages.

        Args:
            previous_aggregate_snapshot (AaveAprAggregateSnapshot): The previous aggregate snapshot.
            current_snapshot (AavePoolTotalAssetSnapshot): The current snapshot to add.
            sample_size (int): The current sample size.

        Returns:
            tuple[AaveAprAggregateSnapshot, int]: Updated aggregate snapshot and new sample size.
        """
        # Build averages and normalize values
        current_liq_avg = sum(current_snapshot.liquidityRate.values()) / len(current_snapshot.liquidityRate.values())
        current_liq_avg /= RAY

        current_variable_avg = sum(current_snapshot.variableBorrowRate.values()) / \
            len(current_snapshot.variableBorrowRate.values())
        current_variable_avg /= RAY

        current_stable_avg = sum(current_snapshot.stableBorrowRate.values()) / \
            len(current_snapshot.stableBorrowRate.values())
        current_stable_avg /= RAY

        current_util_avg = sum(
            [details.utilRate for details in current_snapshot.rateDetails.values()],
        ) / len(current_snapshot.rateDetails.values())

        # Increment rolling averages
        previous_aggregate_snapshot.avgLiquidityRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgLiquidityRate, current_liq_avg, sample_size)
        previous_aggregate_snapshot.avgVariableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgVariableRate, current_variable_avg, sample_size)
        previous_aggregate_snapshot.avgStableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgStableRate, current_stable_avg, sample_size)
        previous_aggregate_snapshot.avgUtilizationRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgUtilizationRate, current_util_avg, sample_size)

        sample_size += 1

        return previous_aggregate_snapshot, sample_size

    def _remove_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: AaveAprAggregateSnapshot,
        current_snapshot: AavePoolTotalAssetSnapshot,
        sample_size: int,
    ) -> tuple[AaveAprAggregateSnapshot, int]:
        """
        Remove a snapshot from the aggregate and update rolling averages.

        Args:
            previous_aggregate_snapshot (AaveAprAggregateSnapshot): The previous aggregate snapshot.
            current_snapshot (AavePoolTotalAssetSnapshot): The snapshot to remove.
            sample_size (int): The current sample size.

        Returns:
            tuple[AaveAprAggregateSnapshot, int]: Updated aggregate snapshot and new sample size.
        """
        # Build averages and normalize values
        current_liq_avg = sum(current_snapshot.liquidityRate.values()) / len(current_snapshot.liquidityRate.values())
        current_liq_avg /= RAY

        current_variable_avg = sum(current_snapshot.variableBorrowRate.values()) / \
            len(current_snapshot.variableBorrowRate.values())
        current_variable_avg /= RAY

        current_stable_avg = sum(current_snapshot.stableBorrowRate.values()) / \
            len(current_snapshot.stableBorrowRate.values())
        current_stable_avg /= RAY

        current_util_avg = sum(
            [details.utilRate for details in current_snapshot.rateDetails.values()],
        ) / len(current_snapshot.rateDetails.values())

        # Decrement rolling averages
        previous_aggregate_snapshot.avgLiquidityRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgLiquidityRate, -current_liq_avg, sample_size - 1)
        previous_aggregate_snapshot.avgVariableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgVariableRate, -current_variable_avg, sample_size - 1)
        previous_aggregate_snapshot.avgStableRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgStableRate, -current_stable_avg, sample_size - 1)
        previous_aggregate_snapshot.avgUtilizationRate = self._update_rolling_average(
            previous_aggregate_snapshot.avgUtilizationRate, -current_util_avg, sample_size - 1)

        sample_size -= 1

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

    async def _calculate_from_scratch(
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
        Calculate the aggregate APR snapshot from scratch.

        Args:
            msg_obj (PowerloomSnapshotSubmittedMessage): The snapshot submission message.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper for the source chain.
            anchor_rpc_helper (RpcHelper): RPC helper for the anchor chain.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract for accessing protocol state.
            project_id (str): ID of the project.

        Returns:
            AaveAprAggregateSnapshot: The calculated aggregate APR snapshot.
        """
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
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 21600, msg_obj.projectId,
        )

        # for the first epoch, using submitted cid
        current_epoch_underlying_data = await get_submission_data(
            redis, msg_obj.snapshotCid, ipfs_reader, project_id,
        )

        snapshots_data = await get_project_epoch_snapshot_bulk(
            redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
            tail_epoch_id, msg_obj.epochId - 1, msg_obj.projectId,
        )

        aggregate_snapshot = AaveAprAggregateSnapshot.parse_obj({'epochId': msg_obj.epochId})

        sample_size = 0
        if extrapolated_flag:
            aggregate_snapshot.complete = False

        for snapshot_data in snapshots_data:
            if snapshot_data:
                snapshot = AavePoolTotalAssetSnapshot.parse_obj(snapshot_data)
                aggregate_snapshot, sample_size = self._add_aggregate_snapshot(
                    aggregate_snapshot, snapshot, sample_size,
                )
                aggregate_snapshot.timestamp = snapshot.timestamp

        if current_epoch_underlying_data:
            current_snapshot = AavePoolTotalAssetSnapshot.parse_obj(current_epoch_underlying_data)
            aggregate_snapshot, sample_size = self._add_aggregate_snapshot(
                aggregate_snapshot, current_snapshot, sample_size,
            )
            aggregate_snapshot.timestamp = current_snapshot.timestamp

        self._logger.debug(f'calculate_from_scratch sample size: {sample_size}')
        await redis.delete(f'calculate_from_scratch:{project_id}')

        return aggregate_snapshot

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
        Compute the aggregate APR snapshot for a 6-hour period.

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
        self._logger.info(f'Calculating 6h asset apr data for {msg_obj}')

        # Get the first epoch for the project
        project_first_epoch = await get_project_first_epoch(
            redis, protocol_state_contract, anchor_rpc_helper, project_id,
        )

        self._logger.debug(f'Project first epoch is {project_first_epoch}')

        # If no past snapshots exist, then aggregate will be calculated from scratch
        if project_first_epoch == 0:
            return await self._calculate_from_scratch(
                msg_obj, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
            )
        else:
            self._logger.info('project_first_epoch is not 0, building aggregate from previous aggregate')

            # Fetch the most recent finalized data from Redis
            project_last_finalized = await redis.zrevrangebyscore(
                project_finalized_data_zset(project_id),
                max='+inf',
                min='-inf',
                withscores=True,
                start=0,
                num=1,
            )

            if project_last_finalized:
                # Extract the CID and epoch of the last finalized data
                project_last_finalized_cid, project_last_finalized_epoch = project_last_finalized[0]
                project_last_finalized_epoch = int(project_last_finalized_epoch)
                project_last_finalized_cid = project_last_finalized_cid.decode('utf-8')
            else:
                self._logger.info('project_last_finalized is None, trying to fetch from contract')
                return await self._calculate_from_scratch(
                    msg_obj, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
                )

            # Calculate the tail epoch (6 hours ago)
            tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 21600, msg_obj.projectId,
            )

            if extrapolated_flag:
                aggregate_complete_flag = False
            else:
                aggregate_complete_flag = True

            # If the last finalized epoch is older than the tail epoch, recalculate from scratch
            if project_last_finalized_epoch <= tail_epoch_id:
                self._logger.error('last finalized epoch is too old, building aggregate from scratch')
                return await self._calculate_from_scratch(
                    msg_obj, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
                )

            # Fetch the last finalized data
            project_last_finalized_data = await get_submission_data(
                redis, project_last_finalized_cid, ipfs_reader, project_id,
            )

            if not project_last_finalized_data:
                self._logger.info('project_last_finalized_data is None, building aggregate from scratch')
                return await self._calculate_from_scratch(
                    msg_obj, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
                )

            # Initialize the aggregate snapshot with the last finalized data
            aggregate_snapshot = AaveAprAggregateSnapshot.parse_obj(project_last_finalized_data)
            # Update the epoch ID to the current epoch
            aggregate_snapshot.epochId = msg_obj.epochId

            # Fetch the last finalized epoch for the base project
            base_project_last_finalized = await redis.zrevrangebyscore(
                project_finalized_data_zset(msg_obj.projectId),
                max='+inf',
                min='-inf',
                withscores=True,
                start=0,
                num=1,
            )

            if base_project_last_finalized:
                _, base_project_last_finalized_epoch_ = base_project_last_finalized[0]
                base_project_last_finalized_epoch = int(base_project_last_finalized_epoch_)
            else:
                base_project_last_finalized_epoch = 0

            # Fetch and process new snapshots since the last finalized data
            if base_project_last_finalized_epoch and project_last_finalized_epoch < base_project_last_finalized_epoch:
                # Fetch finalized snapshots between last processed and last available
                base_finalized_snapshot_range = (
                    project_last_finalized_epoch + 1,
                    base_project_last_finalized_epoch,
                )

                base_finalized_snapshots = await get_project_epoch_snapshot_bulk(
                    redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
                    base_finalized_snapshot_range[0], base_finalized_snapshot_range[1], msg_obj.projectId,
                )
            else:
                base_finalized_snapshots = []
                base_finalized_snapshot_range = (0, project_last_finalized_epoch)

            # Fetch unfinalized snapshots
            base_unfinalized_tasks = []
            for epoch_id in range(base_finalized_snapshot_range[1] + 1, msg_obj.epochId + 1):
                base_unfinalized_tasks.append(
                    redis.get(submitted_base_snapshots_key(epoch_id=epoch_id, project_id=msg_obj.projectId)),
                )

            base_unfinalized_snapshots_raw = await asyncio.gather(*base_unfinalized_tasks, return_exceptions=True)

            # Process unfinalized snapshots
            base_unfinalized_snapshots = []
            for snapshot_data in base_unfinalized_snapshots_raw:
                # check if not exception and not None
                if not isinstance(snapshot_data, Exception) and snapshot_data:
                    base_unfinalized_snapshots.append(
                        json.loads(snapshot_data),
                    )
                else:
                    self._logger.error(
                        f'Error fetching base unfinalized snapshot, building aggregate from scratch',
                    )
                    return await self._calculate_from_scratch(
                        msg_obj, redis, rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract, project_id,
                    )

            base_snapshots = base_finalized_snapshots + base_unfinalized_snapshots

            last_finalized_tail, last_finalized_extrapolated_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, project_last_finalized_epoch, 21600, project_id,
            )

            source_chain_epoch_size = await get_source_chain_epoch_size(redis, protocol_state_contract, anchor_rpc_helper)
            source_chain_block_time = await get_source_chain_block_time(redis, protocol_state_contract, anchor_rpc_helper)
            epoch_time = source_chain_block_time * source_chain_epoch_size

            # Determine the sample size based on the last finalized data
            if last_finalized_extrapolated_flag:
                # Use derived sample size
                sample_size = project_last_finalized_epoch - last_finalized_tail + 1
                self._logger.debug(f'Using derived sample_size {sample_size} for {msg_obj.projectId}')
            else:
                # Use full sample size (6 hours worth of epochs)
                sample_size = int(21600 / epoch_time) + 1
                self._logger.debug(f'Using base sample_size {sample_size} for {msg_obj.projectId}')

            # Process new snapshots and update the aggregate
            for snapshot_data in base_snapshots:
                if snapshot_data:
                    snapshot = AavePoolTotalAssetSnapshot.parse_obj(snapshot_data)
                    aggregate_snapshot, sample_size = self._add_aggregate_snapshot(
                        aggregate_snapshot, snapshot, sample_size,
                    )
                    aggregate_snapshot.timestamp = snapshot.timestamp
                    self._logger.debug(f'Added 1 to sample_size: {sample_size}')

            # Remove outdated snapshots from the tail if needed
            tail_epochs_to_remove = []
            for epoch_id in range(project_last_finalized_epoch, msg_obj.epochId):
                tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
                    redis, protocol_state_contract, anchor_rpc_helper, epoch_id, 21600, msg_obj.projectId,
                )
                if not extrapolated_flag:
                    tail_epochs_to_remove.append(tail_epoch_id)
            
            if tail_epochs_to_remove:
                tail_epoch_snapshots = await get_project_epoch_snapshot_bulk(
                    redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader,
                    tail_epochs_to_remove[0], tail_epochs_to_remove[-1], msg_obj.projectId,
                )

                for snapshot_data in tail_epoch_snapshots:
                    if snapshot_data:
                        snapshot = AavePoolTotalAssetSnapshot.parse_obj(snapshot_data)
                        aggregate_snapshot, sample_size = self._remove_aggregate_snapshot(
                            aggregate_snapshot, snapshot, sample_size,
                        )
                        self._logger.debug(f'Removed 1 from sample_size: {sample_size}')

            self._logger.debug(f'Final sample size for {project_id}: {sample_size}')

            if aggregate_complete_flag:
                aggregate_snapshot.complete = True
            else:
                aggregate_snapshot.complete = False

            return aggregate_snapshot