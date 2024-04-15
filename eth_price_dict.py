from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .utils.core import get_eth_price_dict
from .utils.models.data_models import EthPriceDict
from .utils.models.message_models import EthUsdPriceSnapshot

class EthPriceDictProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='EthPriceDictProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        price_dict_snapshot_prices_map = dict()

        epoch_block_details: EthPriceDict = await get_eth_price_dict(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            block_price_data = epoch_block_details.blockPrices.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False
            
            price_dict_snapshot_prices_map[f"block{block_num}"] = block_price_data.blockPrice

            if fetch_ts:
                if not block_price_data.timestamp:
                    self._logger.error(
                        (
                            'Could not fetch timestamp against max block'
                            ' height in epoch {} - {} for get block details'
                            ' Using current timestamp for snapshot construction'
                        ),
                        min_chain_height,
                        max_chain_height,
                    )
                else:
                    max_block_timestamp = block_price_data.timestamp

        epoch_block_details_snapshot = EthUsdPriceSnapshot(
            chainHeightRange={
                'begin': min_chain_height,
                'end': max_chain_height,
            },
            blockEthUsdPrices=price_dict_snapshot_prices_map,
            timestamp=max_block_timestamp,
        )

        return epoch_block_details_snapshot