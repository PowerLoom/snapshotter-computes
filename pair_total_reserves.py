import time
from typing import List, Tuple
from typing import Optional

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from computes.utils.core import get_pair_reserves, get_block_details_in_block_range
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.settings.config import settings
from computes.utils.models.message_models import UniswapBaseSnapshot
from ipfs_client.main import AsyncIPFSClient


class PairTotalReservesProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="PairTotalReservesProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        # TODO: need clarity on this interface
        task_type: str,
    ) -> List[Tuple[str, UniswapBaseSnapshot]]:
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
        snapshots = list()
        # Fetch block details once for the entire epoch
        try:
            block_details_dict = await get_block_details_in_block_range(
                min_chain_height,
                max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
            self._logger.debug(
                "[Epoch {}-{}] Block details fetched successfully for {} blocks",
                min_chain_height,
                max_chain_height,
                len(block_details_dict)
            )
        except Exception as err:
            self._logger.opt(exception=True).error(
                "[Epoch {}-{}] Failed to fetch block details: {}",
                min_chain_height,
                max_chain_height,
                err
            )
            block_details_dict = dict()

        # find list of active pools for the epoch
        active_pool_set_keys_to_fetch = []
        for block_number in range(min_chain_height, max_chain_height + 1):
            key = f"active_pools:{block_number}:{settings.namespace}"
            active_pool_set_keys_to_fetch.append(key)
        # Use sunion to get the union of all sets at once
        active_pool_addresses = set()
        if active_pool_set_keys_to_fetch:
            active_pool_addresses = await redis_conn.sunion(*active_pool_set_keys_to_fetch)
        self._logger.info(
            "[Epoch {}-{}] Starting token pair reserves computation for {} active pools",
            min_chain_height,
            max_chain_height,
            len(active_pool_addresses)
        )
        active_pool_addresses = map(lambda x: x.decode('utf-8'), active_pool_addresses)
        active_pool_addresses = map(lambda x: Web3.to_checksum_address(x), active_pool_addresses)
        
        # for each Uniswap V3 pool, fetch reserves of token0 and token1 within them
        for pool_address in active_pool_addresses:
            self._logger.debug(
                "[Epoch {}-{}] Processing pool {} | Starting computation",
                min_chain_height,
                max_chain_height,
                pool_address
            )

            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Starting token pair reserves computation (will return UniswapBaseSnapshot) | Wall time: {}",
                min_chain_height,
                max_chain_height,
                pool_address,
                time.time()
            )
            
            # Fetch pair reserves for the entire block range of the epoch
            # get_pair_reserves now returns a UniswapBaseSnapshot object directly
            base_snapshot_data: Optional[UniswapBaseSnapshot] = await get_pair_reserves(
                pair_address=pool_address,
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                ipfs_reader=ipfs_reader,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                block_details_dict=block_details_dict,  # Pass the pre-fetched block details
            )

            if not base_snapshot_data:
                self._logger.error(
                    "[Epoch {}-{}] Pool {} | No UniswapBaseSnapshot data returned by 'get_pair_reserves()'",
                    min_chain_height,
                    max_chain_height,
                    pool_address
                )
                continue

            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Computation completed (UniswapBaseSnapshot received) | Wall time: {}",
                min_chain_height,
                max_chain_height,
                pool_address,
                time.time()
            )
            snapshots.append((task_type.format(poolAddress=pool_address, Namespace=settings.namespace), base_snapshot_data))

        return snapshots