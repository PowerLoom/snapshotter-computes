from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
import json
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from computes.utils.models.message_models import EpochBaseSnapshot
from computes.utils.models.message_models import UniswapEthPriceSnapshot
from computes.redis_keys import uniswap_eth_usd_price_zset
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient


class EthPriceProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="EthPriceProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str = None,
    ) -> Optional[Dict[str, Union[int, float]]]:
        """
        Compute the total reserves for a Uniswap pair within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        Returns:
            Optional[Dict[str, Union[int, float]]]: Computed pair total reserves snapshot.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        # price:ETH:{Namespace}

        project_id = task_type.format(Namespace=settings.namespace)

        cached_price_dict = await redis_conn.zrangebyscore(
            name=uniswap_eth_usd_price_zset,
            min=int(min_chain_height),
            max=int(max_chain_height),
        )
        # If all prices are cached, return them
        price_dict = {
            json.loads(price.decode('utf-8'))['blockHeight']:
            json.loads(price.decode('utf-8'))['price']
            for price in cached_price_dict
        }
        if price_dict:
            eth_price_snapshot = UniswapEthPriceSnapshot(
                **{
                    "ethPrice": price_dict,
                    "chainHeightRange": EpochBaseSnapshot(
                        begin=min_chain_height,
                        end=max_chain_height,
                    ),
                },
            )

        return [(project_id, eth_price_snapshot)]