import time

from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .redis_keys import uniswap_v2_monitored_pairs
from .utils.core import get_pair_reserves
from .utils.models.message_models import EpochBaseSnapshot
from .utils.models.message_models import UniswapPairTotalReservesSnapshot


class PairTotalReservesProcessor(GenericProcessor):

    def __init__(self) -> None:
        self._logger = logger.bind(module='PairTotalReservesProcessor')

    async def _compute_single(
        self,
        data_source_contract_address: str,
        min_chain_height: int,
        max_chain_height: int,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        epoch_reserves_snapshot_map_token0 = dict()
        epoch_prices_snapshot_map_token0 = dict()
        epoch_prices_snapshot_map_token1 = dict()
        epoch_reserves_snapshot_map_token1 = dict()
        epoch_usd_reserves_snapshot_map_token0 = dict()
        epoch_usd_reserves_snapshot_map_token1 = dict()

        pair_reserve_total = await get_pair_reserves(
            pair_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            block_pair_total_reserves = pair_reserve_total.get(block_num)

            epoch_reserves_snapshot_map_token0[
                f'block{block_num}'
            ] = block_pair_total_reserves['token0']
            epoch_reserves_snapshot_map_token1[
                f'block{block_num}'
            ] = block_pair_total_reserves['token1']
            epoch_usd_reserves_snapshot_map_token0[
                f'block{block_num}'
            ] = block_pair_total_reserves['token0USD']
            epoch_usd_reserves_snapshot_map_token1[
                f'block{block_num}'
            ] = block_pair_total_reserves['token1USD']

            epoch_prices_snapshot_map_token0[
                f'block{block_num}'
            ] = block_pair_total_reserves['token0Price']

            epoch_prices_snapshot_map_token1[
                f'block{block_num}'
            ] = block_pair_total_reserves['token1Price']

        pair_total_reserves_snapshot = UniswapPairTotalReservesSnapshot(
            **{
                'token0Reserves': epoch_reserves_snapshot_map_token0,
                'token1Reserves': epoch_reserves_snapshot_map_token1,
                'token0ReservesUSD': epoch_usd_reserves_snapshot_map_token0,
                'token1ReservesUSD': epoch_usd_reserves_snapshot_map_token1,
                'token0Prices': epoch_prices_snapshot_map_token0,
                'token1Prices': epoch_prices_snapshot_map_token1,
                'epoch': EpochBaseSnapshot(
                    begin=min_chain_height, end=max_chain_height,
                ),
                'contract': data_source_contract_address,
            },
        )
        return pair_total_reserves_snapshot

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
    ):

        min_chain_height = msg_obj.begin
        max_chain_height = msg_obj.end

        # get monitored pairs from redis
        monitored_pairs = await redis.smembers(uniswap_v2_monitored_pairs)
        if monitored_pairs:
            monitored_pairs = set([pair.decode() for pair in monitored_pairs])
        snapshots = list()
        self._logger.debug(f'pair reserves computation init time {time.time()}')

        for data_source_contract_address in monitored_pairs:
            snapshot = await self._compute_single(
                data_source_contract_address=data_source_contract_address,
                min_chain_height=min_chain_height,
                max_chain_height=max_chain_height,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            )
            if snapshot:
                snapshots.append(
                    (
                        data_source_contract_address,
                        snapshot,
                    ),
                )

        self._logger.debug(f'pair reserves, computation end time {time.time()}')

        return snapshots
