import time

from redis import asyncio as aioredis

from .utils.core import get_pair_reserves
from .utils.models.message_models import EpochBaseSnapshot
from .utils.models.message_models import UniswapPairTotalReservesSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper


class PairTotalReservesProcessor(GenericProcessorSnapshot):

    def __init__(self) -> None:
        self._logger = logger.bind(module='PairTotalReservesProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ):

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        # TODO: make it dynamic later, just a placeholder to clean things up for now
        data_source_contract_address = "0xaae5f80bac0c7fa0cad6c2481771a3b17af21455"

        epoch_reserves_snapshot_map_token0 = dict()
        epoch_prices_snapshot_map_token0 = dict()
        epoch_prices_snapshot_map_token1 = dict()
        epoch_reserves_snapshot_map_token1 = dict()
        epoch_usd_reserves_snapshot_map_token0 = dict()
        epoch_usd_reserves_snapshot_map_token1 = dict()

        self._logger.debug(f'pair reserves {data_source_contract_address} computation init time {time.time()}')
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
        self._logger.debug(f'pair reserves {data_source_contract_address}, computation end time {time.time()}')

        return pair_total_reserves_snapshot
