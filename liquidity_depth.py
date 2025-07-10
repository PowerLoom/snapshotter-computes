import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper

from computes.utils.core import get_liquidity_depth
from computes.utils.models.message_models import EpochBaseSnapshot, LiquidityDepthSnapshot
from snapshotter.utils.callback_helpers import SnapshotProcessMessage
from snapshotter.utils.default_logger import logger
from ipfs_client.main import AsyncIPFSClient


class LiquidityDepthProcessor(SnapshotProcessMessage):

    def __init__(self) -> None:
        self._logger = logger.bind(module="LiquidityDepthProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
    ) -> Optional[Dict[str, Union[int, float]]]:


        self._logger.debug(
            f"liquidity depth {epoch.data_source} computation init time {time.time()}"
        )

        liquidity_depth_dict = await get_liquidity_depth(
            pair_address=epoch.data_source,
            from_block=epoch.begin,
            to_block=epoch.end,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )
        liquidity_depth_snapshot: LiquidityDepthSnapshot = LiquidityDepthSnapshot(
            ticks_by_block=liquidity_depth_dict,
            contract=epoch.primary_data_source,
            chainHeightRange=EpochBaseSnapshot(
                begin=epoch.begin,
                end=epoch.end,
            ),
            timestamp=int(time.time())
        )
        self._logger.debug(
            f"liquidity depth dict {liquidity_depth_snapshot.ticks_by_block}, computation end time {time.time()}"
        )
        self._logger.debug(
            f"liquidity depth {epoch.data_source}, computation end time {time.time()}"
        )

        return liquidity_depth_snapshot
