from typing import List, Tuple
from typing import Optional

from redis import asyncio as aioredis
import json
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import RpcHelper
from computes.utils.models.message_models import EpochBaseSnapshot
from computes.utils.models.message_models import UniswapEthPriceSnapshot
from computes.redis_keys import uniswap_eth_usd_price_zset_key
from snapshotter.settings.config import settings
from ipfs_client.main import AsyncIPFSClient


class EthPriceProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting ETH/USD prices from Uniswap.
    
    This processor retrieves cached ETH/USD prices from Redis for a given epoch range
    and formats them into a snapshot structure for further processing.
    """

    def __init__(self) -> None:
        """
        Initialize the EthPriceProcessor with a logger instance.
        """
        self._logger = logger.bind(module="EthPriceProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> Optional[List[Tuple[str, UniswapEthPriceSnapshot]]]:
        """
        Compute ETH/USD prices for a given epoch range by retrieving cached data from Redis.

        Args:
            epoch (SnapshotProcessMessage): The epoch information containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for accessing cached price data.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions (unused in this implementation).
            anchor_rpc_helper (RpcHelper): Anchor RPC helper (unused in this implementation).
            ipfs_reader (AsyncIPFSClient): IPFS client for data retrieval (unused in this implementation).
            protocol_state_contract: Protocol state contract instance (unused in this implementation).
            task_type (str): Task type identifier with a {Namespace} placeholder.

        Returns:
            Optional[List[Tuple[str, UniswapEthPriceSnapshot]]]: A list containing a tuple of project ID and 
            ETH price snapshot, or None if no prices are found.
        """
        # Extract epoch boundaries
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        # Format project ID with namespace
        project_id = task_type.format(Namespace=settings.namespace)

        # Retrieve cached prices from Redis for the given epoch range
        cached_price_dict = await redis_conn.zrangebyscore(
            name=uniswap_eth_usd_price_zset_key(settings.namespace),
            min=int(min_chain_height),
            max=int(max_chain_height),
        )

        # Convert cached data into a dictionary mapping block heights to prices
        price_dict = {
            str(json.loads(price.decode('utf-8'))['blockHeight']):
            json.loads(price.decode('utf-8'))['price']
            for price in cached_price_dict
        }

        # Create snapshot if prices are found
        if price_dict:
            eth_price_snapshot = UniswapEthPriceSnapshot(
                **{
                    "ethPrice": price_dict,
                    "epoch": EpochBaseSnapshot(
                        begin=min_chain_height,
                        end=max_chain_height,
                    ),
                },
            )

        return [(project_id, eth_price_snapshot)]