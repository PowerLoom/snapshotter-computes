from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from computes.utils.helpers import get_pair_metadata
from computes.utils.models.message_models import UniswapPairTotalReservesSnapshot
from computes.utils.models.message_models import UniswapTopPair24hSnapshot
from computes.utils.models.message_models import UniswapTopPairs24hSnapshot
from computes.utils.models.message_models import UniswapTradesAggregateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper


class AggregateTopPairsProcessor(GenericProcessorAggregate):
    """
    Processor for aggregating top Uniswap pairs data over a 24-hour period.
    
    This class calculates trade volume, reserves, and other metrics for Uniswap pairs,
    and generates a snapshot of the top pairs based on liquidity.
    """

    def __init__(self) -> None:
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
        Compute the 24-hour top pairs trade volume and reserves data.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): The message object containing calculation details.
            redis (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: The protocol state contract.
            project_id (str): The project ID.

        Returns:
            UniswapTopPairs24hSnapshot: A snapshot of the top Uniswap pairs over 24 hours.
        """
        self._logger.info(f'Calculating 24h top pairs trade volume and reserves data for {msg_obj}')

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
            
            # Parse snapshot based on project ID
            if 'reserves' in msg.projectId:
                snapshot = UniswapPairTotalReservesSnapshot.parse_obj(data)
            elif 'volume' in msg.projectId:
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

        # Process snapshots and generate pair data
        pair_data = {}
        for snapshot_project_id, snapshot in snapshot_mapping.items():
            contract = snapshot_project_id.split(':')[-2]
            pair_metadata = all_pair_metadata[contract]

            # Initialize pair data if it doesn't exist
            if contract not in pair_data:
                pair_data[contract] = {
                    'address': contract,
                    'name': pair_metadata['pair']['symbol'],
                    'liquidity': 0,
                    'volume24h': 0,
                    'fee24h': 0,
                }

            # Update pair data based on snapshot type
            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end
                pair_data[contract]['liquidity'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}'] + \
                    snapshot.token1ReservesUSD[f'block{max_epoch_block}']
            elif 'volume' in snapshot_project_id:
                pair_data[contract]['volume24h'] += snapshot.totalTrade
                pair_data[contract]['fee24h'] += snapshot.totalFee

        # Create and sort top pairs list
        top_pairs = [UniswapTopPair24hSnapshot.parse_obj(pair) for pair in pair_data.values()]
        top_pairs = sorted(top_pairs, key=lambda x: x.liquidity, reverse=True)

        # Create final snapshot
        top_pairs_snapshot = UniswapTopPairs24hSnapshot(
            epochId=epoch_id,
            pairs=top_pairs,
        )

        # Set complete flag based on all base snapshots' statuses
        if not all(complete_flags):
            top_pairs_snapshot.complete = False

        return top_pairs_snapshot
