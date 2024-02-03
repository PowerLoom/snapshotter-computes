import asyncio
from typing import List

from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import CalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from ..utils.helpers import get_pair_metadata
from ..utils.models.message_models import UniswapPairTotalReservesSnapshot
from ..utils.models.message_models import UniswapTopPair24hSnapshot
from ..utils.models.message_models import UniswapTopPairs24hSnapshot
from ..utils.models.message_models import UniswapTradesAggregateSnapshot


class AggregateTopPairsProcessor(GenericProcessorAggregate):

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateTopPairsProcessor')

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
        self._logger.info(f'Calculating 24h top pairs trade volume and reserves data for {msg_obj}')

        epoch_id = msg_obj.epochId
        project_id = 'aggregate_24h_top_pairs:uniswapv3'
        snapshot_mapping = {}
        all_pair_metadata = {}

        submitted_snapshots = []
        for msg in msg_obj.messages:
            for snapshot in msg.snapshotsSubmitted:
                submitted_snapshots.append(snapshot)

        snapshot_data = await get_submission_data_bulk(
            redis, [snapshot.snapshotCid for snapshot in submitted_snapshots], ipfs_reader,
            [snapshot.projectId for snapshot in submitted_snapshots],
        )
        pair_metadata_tasks = {}
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

            contract_address = msg.projectId.split(':')[-2]
            if contract_address not in all_pair_metadata:
                all_pair_metadata[contract_address] = await get_pair_metadata(
                    pair_address=contract_address,
                    rpc_helper=rpc_helper,
                    redis_conn=redis,
                )
        
        # iterate over all snapshots and generate pair data
        pair_data = {}
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            contract = snapshot_project_id.split(':')[-2]
            pair_metadata = all_pair_metadata[contract]

            if contract not in pair_data:
                pair_data[contract] = {
                    'address': contract,
                    'name': pair_metadata['pair']['symbol'],
                    'liquidity': 0,
                    'volume24h': 0,
                    'fee24h': 0,
                }

            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end
                pair_data[contract]['liquidity'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}'] + \
                    snapshot.token1ReservesUSD[f'block{max_epoch_block}']

            elif 'volume' in snapshot_project_id:
                pair_data[contract]['volume24h'] += snapshot.totalTrade
                pair_data[contract]['fee24h'] += snapshot.totalFee

        top_pairs = []
        for pair in pair_data.values():
            top_pairs.append(UniswapTopPair24hSnapshot.parse_obj(pair))

        top_pairs = sorted(top_pairs, key=lambda x: x.liquidity, reverse=True)

        top_pairs_snapshot = UniswapTopPairs24hSnapshot(
            epochId=epoch_id,
            pairs=top_pairs,
        )

        if not all(complete_flags):
            top_pairs_snapshot.complete = False

        return [(project_id, top_pairs_snapshot)]