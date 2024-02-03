import asyncio
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

from ..utils.helpers import get_pair_metadata
from ..utils.models.message_models import UniswapPairTotalReservesSnapshot
from ..utils.models.message_models import UniswapTopTokenSnapshot
from ..utils.models.message_models import UniswapTopTokensSnapshot
from ..utils.models.message_models import UniswapTradesAggregateSnapshot


class AggregateTopTokensProcessor(GenericProcessorAggregate):

    def __init__(self) -> None:
        self._logger = logger.bind(module='AggregateTopTokensProcessor')

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

        self._logger.info(f'Calculating top tokens data for {msg_obj}')
        epoch_id = msg_obj.epochId
        project_id = project_ids[0]
        snapshot_mapping = {}
        projects_metadata = {}

        submitted_snapshots = []
        for msg in msg_obj.messages:
            for snapshot in msg.snapshotsSubmitted:
                submitted_snapshots.append(snapshot)

        snapshot_data = await get_submission_data_bulk(
            redis, [snapshot.snapshotCid for snapshot in submitted_snapshots], ipfs_reader,
            [snapshot.projectId for snapshot in submitted_snapshots],
        )
        pair_metadata_tasks = []
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
            pair_metadata_tasks.append(get_pair_metadata(
                pair_address=contract_address,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            ))
        pair_metadata_list = await asyncio.gather(*pair_metadata_tasks) 
        for msg, pair_metadata in zip(submitted_snapshots, pair_metadata_list):
            contract_address = msg.projectId.split(':')[-2]
            projects_metadata[contract_address] = pair_metadata


        # iterate over all snapshots and generate token data
        token_data = {}
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            contract_address = snapshot_project_id.split(':')[-2]
            project_metadata = projects_metadata[contract_address]

            token0 = project_metadata['token0']
            token1 = project_metadata['token1']

            if token0['address'] not in token_data:
                token_data[token0['address']] = {
                    'address': token0['address'],
                    'name': token0['name'],
                    'symbol': token0['symbol'],
                    'decimals': token0['decimals'],
                    'price': 0,
                    'volume24h': 0,
                    'liquidity': 0,
                    'priceChange24h': 0,
                }

            if token1['address'] not in token_data:
                token_data[token1['address']] = {
                    'address': token1['address'],
                    'name': token1['name'],
                    'symbol': token1['symbol'],
                    'decimals': token1['decimals'],
                    'price': 0,
                    'volume24h': 0,
                    'liquidity': 0,
                    'priceChange24h': 0,
                }

            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end

                token_data[token0['address']]['price'] = snapshot.token0Prices[f'block{max_epoch_block}']
                token_data[token1['address']]['price'] = snapshot.token1Prices[f'block{max_epoch_block}']

                token_data[token0['address']]['liquidity'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}']
                token_data[token1['address']]['liquidity'] += snapshot.token1ReservesUSD[f'block{max_epoch_block}']

            elif 'volume' in snapshot_project_id:

                token_data[token0['address']]['volume24h'] += snapshot.token0TradeVolumeUSD
                token_data[token1['address']]['volume24h'] += snapshot.token1TradeVolumeUSD

        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )

        if not extrapolated_flag:
            previous_top_tokens_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_top_tokens_snapshot_data:
                previous_top_tokens_snapshot = UniswapTopTokensSnapshot.parse_obj(previous_top_tokens_snapshot_data)
                for token in previous_top_tokens_snapshot.tokens:
                    if token.address in token_data:
                        price_before_24h = token.price

                        if price_before_24h > 0:
                            token_data[token.address]['priceChange24h'] = (
                                token_data[token.address]['price'] - price_before_24h
                            ) / price_before_24h * 100

        top_tokens = []
        for token in token_data.values():
            top_tokens.append(UniswapTopTokenSnapshot.parse_obj(token))

        top_tokens = sorted(top_tokens, key=lambda x: x.volume24h, reverse=True)

        top_tokens_snapshot = UniswapTopTokensSnapshot(
            epochId=epoch_id,
            tokens=top_tokens,
        )
        if not all(complete_flags):
            self._logger.debug(f'Not all snapshots are complete for {project_id}')

            top_tokens_snapshot.complete = False

        return [(project_id, top_tokens_snapshot)]