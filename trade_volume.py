import time

from redis import asyncio as aioredis

from .utils.core import get_pair_trade_volume
from .utils.models.message_models import EpochBaseSnapshot
from .utils.models.message_models import UniswapTradesSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper
from .redis_keys import uniswap_v2_monitored_pairs


class TradeVolumeProcessor(GenericProcessorSnapshot):

    def __init__(self) -> None:
        self._logger = logger.bind(module='TradeVolumeProcessor')

    async def _compute_single(
        self,
        data_source_contract_address: str,
        min_chain_height: int,
        max_chain_height: int,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        result = await get_pair_trade_volume(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        self._logger.debug(f'trade volume {data_source_contract_address}, computation end time {time.time()}')

        # Set effective trade volume at top level
        total_trades_in_usd = result['Trades'][
            'totalTradesUSD'
        ]
        total_fee_in_usd = result['Trades']['totalFeeUSD']
        total_token0_vol = result['Trades'][
            'token0TradeVolume'
        ]
        total_token1_vol = result['Trades'][
            'token1TradeVolume'
        ]
        total_token0_vol_usd = result['Trades'][
            'token0TradeVolumeUSD'
        ]
        total_token1_vol_usd = result['Trades'][
            'token1TradeVolumeUSD'
        ]

        trade_volume_snapshot = UniswapTradesSnapshot(
            contract=data_source_contract_address,
            epoch=EpochBaseSnapshot(begin=min_chain_height, end=max_chain_height),
            totalTrade=float(f'{total_trades_in_usd: .6f}'),
            totalFee=float(f'{total_fee_in_usd: .6f}'),
            token0TradeVolume=float(f'{total_token0_vol: .6f}'),
            token1TradeVolume=float(f'{total_token1_vol: .6f}'),
            token0TradeVolumeUSD=float(f'{total_token0_vol_usd: .6f}'),
            token1TradeVolumeUSD=float(f'{total_token1_vol_usd: .6f}'),
            events=result,
        )

        return trade_volume_snapshot

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        # get monitored pairs from redis
        monitored_pairs = await redis_conn.smembers(uniswap_v2_monitored_pairs)
        if monitored_pairs:
            monitored_pairs = set([pair.decode() for pair in monitored_pairs])
        snapshots = list()

        self._logger.debug(f'trade volume, computation init time {time.time()}')

        for data_source_contract_address in monitored_pairs:

            snapshot = await self._compute_single(
                data_source_contract_address=data_source_contract_address,
                min_chain_height=min_chain_height,
                max_chain_height=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
            if snapshot:
                snapshots.append(
                    (
                        data_source_contract_address,
                        snapshot,
                    ),
                )

        return snapshots
