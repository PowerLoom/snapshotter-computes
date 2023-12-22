import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis

from .utils.core import get_liquidity_depth, get_pair_reserves
from .utils.models.message_models import LiquidityDepthSnapshot, UniswapPairTotalReservesSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBaseSnapshot
from snapshotter.utils.rpc import RpcHelper


class LiquidityDepthProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module="LiquidityDepthProcessor")

    async def compute(
        self,
        min_chain_height: int,
        max_chain_height: int,
        data_source_contract_address: str,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ) -> Optional[Dict[str, Union[int, float]]]:


        self._logger.debug(
            f"liquidity  depth {data_source_contract_address} computation init time {time.time()}"
        )

        liquidity_depth_dict = await get_liquidity_depth(
            pair_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            fetch_timestamp=True,
        )
        
        liquidity_depth_snapshot: LiquidityDepthSnapshot = LiquidityDepthSnapshot(ticks_by_block=liquidity_depth_dict)

        self._logger.debug(
            f"liquidity depth dict {liquidity_depth_snapshot[max_chain_height]}, computation end time {time.time()}"
        )
        self._logger.debug(
            f"liquidity depth {data_source_contract_address}, computation end time {time.time()}"
        )

        return liquidity_depth_snapshot