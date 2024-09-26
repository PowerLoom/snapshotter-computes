from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from computes.utils.helpers import get_pair_metadata
from computes.utils.models.message_models import UniswapTopPair7dSnapshot
from computes.utils.models.message_models import UniswapTopPairs7dSnapshot
from computes.utils.models.message_models import UniswapTradesAggregateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper


class AggreagateTopPairsProcessor(GenericProcessorAggregate):
    """
    Processor class for aggregating top Uniswap pairs data over a 7-day period.
    """

    def __init__(self) -> None:
        """
        Initialize the AggreagateTopPairsProcessor.
        """
        self._logger = logger.bind(module='AggregateTopPairsProcessor')

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
        Compute the top Uniswap pairs based on 7-day trading volume.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): Message object containing computation details.
            redis (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Protocol state contract.
            project_id (str): Project identifier.

        Returns:
            UniswapTopPairs7dSnapshot: Snapshot of top Uniswap pairs over 7 days.
        """
        self._logger.info(f'Calculating 7d top pairs trade volume data for {msg_obj}')

        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        all_pair_metadata = {}

        # Fetch snapshot data for all messages
        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        complete_flags = []
        for msg, data in zip(msg_obj.messages, snapshot_data):
            if not data:
                continue
            snapshot = UniswapTradesAggregateSnapshot.parse_obj(data)
            complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot

            # Fetch pair metadata if not already cached
            contract_address = msg.projectId.split(':')[-2]
            if contract_address not in all_pair_metadata:
                pair_metadata = await get_pair_metadata(
                    contract_address,
                    redis_conn=redis,
                    rpc_helper=rpc_helper,
                )
                all_pair_metadata[contract_address] = pair_metadata

        # Aggregate pair data
        pair_data = {}
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            contract = snapshot_project_id.split(':')[-2]
            pair_metadata = all_pair_metadata[contract]

            if contract not in pair_data:
                pair_data[contract] = {
                    'address': contract,
                    'name': pair_metadata['pair']['symbol'],
                    'volume7d': 0,
                    'fee7d': 0,
                }

            pair_data[contract]['volume7d'] += snapshot.totalTrade
            pair_data[contract]['fee7d'] += snapshot.totalFee

        # Create and sort top pairs list
        top_pairs = []
        for pair in pair_data.values():
            top_pairs.append(UniswapTopPair7dSnapshot.parse_obj(pair))

        top_pairs = sorted(top_pairs, key=lambda x: x.volume7d, reverse=True)

        # Create final snapshot
        top_pairs_snapshot = UniswapTopPairs7dSnapshot(
            epochId=epoch_id,
            pairs=top_pairs,
        )

        # Set complete flag based on all individual snapshots
        if not all(complete_flags):
            top_pairs_snapshot.complete = False

        return top_pairs_snapshot
