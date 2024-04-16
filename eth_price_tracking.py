from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.models.message_models import EthPriceSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_eth_price_usd


class EthPriceProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='EthPriceProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, EthPriceSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        token_price_dict = await get_eth_price_usd(
                from_block=epoch.begin, to_block=epoch.end,
                redis_conn=redis_conn, rpc_helper=rpc_helper,
            )

        snapshot = EthPriceSnapshot(
            block=epoch.begin,
            price=token_price_dict[epoch.begin]
        )
        return [(f"eth-price-{epoch.begin}", snapshot)]