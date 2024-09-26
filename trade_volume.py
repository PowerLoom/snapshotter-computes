import time

from redis import asyncio as aioredis

from computes.utils.core import get_pair_trade_volume
from computes.utils.models.message_models import EpochBaseSnapshot
from computes.utils.models.message_models import UniswapTradesSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper


class TradeVolumeProcessor(GenericProcessorSnapshot):
    """
    A processor class for computing and transforming trade volume data.

    This class extends GenericProcessorSnapshot and implements methods to compute
    trade volume for a given epoch and transform the processed data into a
    structured snapshot.
    """

    def __init__(self) -> None:
        """
        Initialize the TradeVolumeProcessor and a logger.
        """
        self._logger = logger.bind(module='TradeVolumeProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Compute trade volume for the given epoch.

        Args:
            epoch (PowerloomSnapshotProcessMessage): The epoch data for which to compute trade volume.
            redis_conn (aioredis.Redis): Redis connection for caching and data retrieval.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        Returns:
            dict: The computed trade volume data.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        data_source_contract_address = epoch.data_source

        self._logger.debug(f'trade volume {data_source_contract_address}, computation init time {time.time()}')
        snapshot = await get_pair_trade_volume(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        self._logger.debug(f'trade volume {data_source_contract_address}, computation end time {time.time()}')

        # Extract trade volume data from the snapshot
        total_trades_in_usd = snapshot['Trades']['totalTradesUSD']
        total_fee_in_usd = snapshot['Trades']['totalFeeUSD']
        total_token0_vol = snapshot['Trades']['token0TradeVolume']
        total_token1_vol = snapshot['Trades']['token1TradeVolume']
        total_token0_vol_usd = snapshot['Trades']['token0TradeVolumeUSD']
        total_token1_vol_usd = snapshot['Trades']['token1TradeVolumeUSD']

        # Get the max block timestamp and remove it from the snapshot
        max_block_timestamp = snapshot.get('timestamp')
        snapshot.pop('timestamp', None)

        # Create and return a structured UniswapTradesSnapshot
        trade_volume_snapshot = UniswapTradesSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(begin=min_chain_height, end=max_chain_height),
            timestamp=max_block_timestamp,
            totalTrade=float(f'{total_trades_in_usd: .6f}'),
            totalFee=float(f'{total_fee_in_usd: .6f}'),
            token0TradeVolume=float(f'{total_token0_vol: .6f}'),
            token1TradeVolume=float(f'{total_token1_vol: .6f}'),
            token0TradeVolumeUSD=float(f'{total_token0_vol_usd: .6f}'),
            token1TradeVolumeUSD=float(f'{total_token1_vol_usd: .6f}'),
            events=snapshot,
        )
        return trade_volume_snapshot