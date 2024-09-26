from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from computes.utils.helpers import get_pair_metadata
from computes.utils.models.message_models import UniswapPairTotalReservesSnapshot
from computes.utils.models.message_models import UniswapTopTokenSnapshot
from computes.utils.models.message_models import UniswapTopTokensSnapshot
from computes.utils.models.message_models import UniswapTradesAggregateSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper


class AggreagateTopTokensProcessor(GenericProcessorAggregate):
    """
    A processor class for aggregating top tokens data from Uniswap.
    """

    def __init__(self) -> None:
        """
        Initialize the AggreagateTopTokensProcessor.
        """
        self._logger = logger.bind(module='AggregateTopTokensProcessor')

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
        Compute the top tokens data for Uniswap.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): The message object containing calculation details.
            redis (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper for blockchain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: The protocol state contract.
            project_id (str): The ID of the project.

        Returns:
            UniswapTopTokensSnapshot: A snapshot of the top tokens data.
        """

        self._logger.info(f'Calculating top tokens data for {msg_obj}')
        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        projects_metadata = {}

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
            
            # Parse snapshot based on project type
            if 'reserves' in msg.projectId:
                snapshot = UniswapPairTotalReservesSnapshot.parse_obj(data)
            elif 'volume' in msg.projectId:
                snapshot = UniswapTradesAggregateSnapshot.parse_obj(data)
                complete_flags.append(snapshot.complete)
            
            snapshot_mapping[msg.projectId] = snapshot

            # Fetch and store pair metadata
            contract_address = msg.projectId.split(':')[-2]
            pair_metadata = await get_pair_metadata(
                contract_address,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            )
            projects_metadata[msg.projectId] = pair_metadata

        # Process snapshots to generate token data
        token_data = {}
        for snapshot_project_id, snapshot in snapshot_mapping.items():
            project_metadata = projects_metadata[snapshot_project_id]

            token0 = project_metadata['token0']
            token1 = project_metadata['token1']

            # Initialize token data if not present
            for token in [token0, token1]:
                if token['address'] not in token_data:
                    token_data[token['address']] = {
                        'address': token['address'],
                        'name': token['name'],
                        'symbol': token['symbol'],
                        'decimals': token['decimals'],
                        'price': 0,
                        'volume24h': 0,
                        'liquidity': 0,
                        'priceChange24h': 0,
                    }

            if 'reserves' in snapshot_project_id:
                # Update price and liquidity for reserve snapshots
                max_epoch_block = snapshot.chainHeightRange.end

                token_data[token0['address']]['price'] = snapshot.token0Prices[f'block{max_epoch_block}']
                token_data[token1['address']]['price'] = snapshot.token1Prices[f'block{max_epoch_block}']

                token_data[token0['address']]['liquidity'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}']
                token_data[token1['address']]['liquidity'] += snapshot.token1ReservesUSD[f'block{max_epoch_block}']

            elif 'volume' in snapshot_project_id:
                # Update volume for volume snapshots
                token_data[token0['address']]['volume24h'] += snapshot.token0TradeVolumeUSD
                token_data[token1['address']]['volume24h'] += snapshot.token1TradeVolumeUSD

        # Calculate price change over 24 hours
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

        # Create and sort top tokens list
        top_tokens = [UniswapTopTokenSnapshot.parse_obj(token) for token in token_data.values()]
        top_tokens = sorted(top_tokens, key=lambda x: x.liquidity, reverse=True)

        # Create final snapshot
        top_tokens_snapshot = UniswapTopTokensSnapshot(
            epochId=epoch_id,
            tokens=top_tokens,
        )
        if not all(complete_flags):
            top_tokens_snapshot.complete = False

        return top_tokens_snapshot
