import asyncio

import pydantic
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from ..utils.helpers import truncate
from ..utils.models.message_models import UniswapTradesAggregateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.rpc import RpcHelper


class AggregateTradeVolumeProcessor(GenericProcessorAggregate):
    """
    A processor class for aggregating Uniswap trade volume data over a 7-day period.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        """
        Initialize the AggregateTradeVolumeProcessor.
        """
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateTradeVolumeProcessor7d')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesAggregateSnapshot,
    ) -> UniswapTradesAggregateSnapshot:
        """
        Add the current snapshot data to the previous aggregate snapshot.

        Args:
            previous_aggregate_snapshot (UniswapTradesAggregateSnapshot): The existing aggregate snapshot.
            current_snapshot (UniswapTradesAggregateSnapshot): The new snapshot to be added.

        Returns:
            UniswapTradesAggregateSnapshot: The updated aggregate snapshot.
        """
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
    ) -> UniswapTradesAggregateSnapshot:
        """
        Remove the current snapshot data from the previous aggregate snapshot.

        Args:
            previous_aggregate_snapshot (UniswapTradesAggregateSnapshot): The existing aggregate snapshot.
            current_snapshot (UniswapTradesAggregateSnapshot): The snapshot to be removed.

        Returns:
            UniswapTradesAggregateSnapshot: The updated aggregate snapshot.
        """
        previous_aggregate_snapshot.totalTrade -= current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee -= current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume -= current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume -= current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD -= current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD -= current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    def _truncate_snapshot(self, snapshot: UniswapTradesAggregateSnapshot) -> UniswapTradesAggregateSnapshot:
        """
        Truncate the numeric values in the snapshot to a fixed number of decimal places.

        Args:
            snapshot (UniswapTradesAggregateSnapshot): The snapshot to be truncated.

        Returns:
            UniswapTradesAggregateSnapshot: The truncated snapshot.
        """
        snapshot.totalTrade = truncate(snapshot.totalTrade)
        snapshot.totalFee = truncate(snapshot.totalFee)
        snapshot.token0TradeVolume = truncate(snapshot.token0TradeVolume)
        snapshot.token1TradeVolume = truncate(snapshot.token1TradeVolume)
        snapshot.token0TradeVolumeUSD = truncate(snapshot.token0TradeVolumeUSD)
        snapshot.token1TradeVolumeUSD = truncate(snapshot.token1TradeVolumeUSD)
        return snapshot

    async def compute(
        self,
        msg_obj: PowerloomSnapshotSubmittedMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ) -> UniswapTradesAggregateSnapshot:
        """
        Compute the 7-day aggregate trade volume snapshot.

        Args:
            msg_obj (PowerloomSnapshotSubmittedMessage): The message object containing snapshot information.
            redis (aioredis.Redis): Redis client for caching.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper for blockchain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data.
            protocol_state_contract: The protocol state contract.
            project_id (str): The ID of the project.

        Returns:
            UniswapTradesAggregateSnapshot: The computed 7-day aggregate snapshot.
        """
        self._logger.info(f'Building 7 day trade volume aggregate snapshot against {msg_obj}')

        contract = project_id.split(':')[-2]

        aggregate_snapshot = UniswapTradesAggregateSnapshot(
            epochId=msg_obj.epochId,
        )
        
        # Fetch 24h snapshots
        snapshot_tasks = list()
        self._logger.debug('fetching 24hour aggregates spaced out by 1 day over 7 days...')
        count = 1
        self._logger.debug(
            'fetch # {}: queueing task for 24h aggregate snapshot for project ID {}'
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
        # If not extrapolated, attempt to seek further back
        while not seek_stop_flag and count < 7:
            tail_epoch_id, seek_stop_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, head_epoch, 86400, msg_obj.projectId,
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
            'for 7d aggregated trade volume calculations: fetched {} '
            '24h aggregated trade volume snapshots for project ID {}: {}',
            len(all_snapshots), msg_obj.projectId, all_snapshots,
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

        # Set the completeness flag based on all snapshots being complete and having 7 days of data
        aggregate_snapshot.complete = all(complete_flags) and count == 7

        return self._truncate_snapshot(aggregate_snapshot)
