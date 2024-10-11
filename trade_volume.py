import time

from redis import asyncio as aioredis

from computes.utils.core import get_pair_trade_volume
from computes.utils.models.message_models import EpochBaseSnapshot
from computes.utils.models.message_models import UniswapTradesSnapshot
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper


class TradeVolumeProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and storing trade volume for Uniswap pairs.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = [
            self.transform_processed_epoch_to_trade_volume,
        ]
        self._logger = logger.bind(module="TradeVolumeProcessor")

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Compute the trade volume for a Uniswap pair within the given epoch.

        Args:
            epoch (PowerloomSnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        Returns:
            dict: Computed trade volume data.
        """
        
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        data_source_contract_address = epoch.data_source

        self._logger.debug(
            f"trade volume {data_source_contract_address}, computation init time {time.time()}"
        )
        result = await get_pair_trade_volume(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        self._logger.debug(
            f"trade volume {data_source_contract_address}, computation end time {time.time()}"
        )
        return result

    def transform_processed_epoch_to_trade_volume(
        self,
        snapshot,
        data_source_contract_address,
        epoch_begin,
        epoch_end,
    ):
        """
        Transform the processed epoch data into a trade volume snapshot.

        Args:
            snapshot (dict): The processed snapshot data.
            data_source_contract_address (str): The address of the data source contract.
            epoch_begin (int): The beginning of the epoch.
            epoch_end (int): The end of the epoch.

        Returns:
            UniswapTradesSnapshot: The transformed trade volume snapshot.
        """
        self._logger.debug(
            "Trade volume processed snapshot: {}",
            snapshot,
        )

        # Extract trade volume data from the snapshot
        total_trades_in_usd = snapshot["Trades"]["totalTradesUSD"]
        total_fee_in_usd = snapshot["Trades"]["totalFeeUSD"]
        total_token0_vol = snapshot["Trades"]["token0TradeVolume"]
        total_token1_vol = snapshot["Trades"]["token1TradeVolume"]
        total_token0_vol_usd = snapshot["Trades"]["token0TradeVolumeUSD"]
        total_token1_vol_usd = snapshot["Trades"]["token1TradeVolumeUSD"]

        max_block_timestamp = snapshot.get("timestamp")
        snapshot.pop("timestamp", None)

        # Create the UniswapTradesSnapshot object
        trade_volume_snapshot = UniswapTradesSnapshot(
            contract=data_source_contract_address,
            epoch=EpochBaseSnapshot(begin=epoch_begin, end=epoch_end),
            timestamp=max_block_timestamp,
            totalTrade=float(f"{total_trades_in_usd: .6f}"),
            totalFee=float(f"{total_fee_in_usd: .6f}"),
            token0TradeVolume=float(f"{total_token0_vol: .6f}"),
            token1TradeVolume=float(f"{total_token1_vol: .6f}"),
            token0TradeVolumeUSD=float(f"{total_token0_vol_usd: .6f}"),
            token1TradeVolumeUSD=float(f"{total_token1_vol_usd: .6f}"),
            events=snapshot,
        )
        return trade_volume_snapshot