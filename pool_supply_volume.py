import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .utils.core import get_asset_trade_volume
from .utils.models.message_models import AaveSupplyVolumeSnapshot
from .utils.models.message_models import EpochBaseSnapshot


class AssetSupplyVolumeProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AssetSupplyVolumeProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        asset_address = epoch.data_source

        max_block_timestamp = int(time.time())

        self._logger.debug(f'supply volume {asset_address}, computation init time {time.time()}')
        result = await get_asset_trade_volume(
            asset_address=asset_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        self._logger.debug(f'supply volume {asset_address}, computation end time {time.time()}')

        if result.get('timestamp', 0) > 0:
            max_block_timestamp = result['timestamp']

        result.pop('timestamp', None)

        events = []
        for key in result.keys():
            if result[key]['logs']:
                for log in result[key]['logs']:
                    events.append(log)

        supply_volume_snapshot = AaveSupplyVolumeSnapshot(
            contract=asset_address,
            chainHeightRange=EpochBaseSnapshot(begin=epoch.begin, end=epoch.end),
            timestamp=max_block_timestamp,
            borrow=result['borrow']['totals'],
            repay=result['repay']['totals'],
            supply=result['supply']['totals'],
            withdraw=result['withdraw']['totals'],
            events=events,
        )

        return supply_volume_snapshot
