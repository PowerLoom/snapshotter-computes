import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper

from computes.utils.core import get_pair_reserves
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.settings.config import settings
from computes.utils.models.message_models import EpochBaseSnapshot
from computes.utils.models.message_models import UniswapPairTotalReservesSnapshot
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
        task_type: str = "baseSnapshot:{poolAddress}:{Namespace}",
    ) -> Optional[UniswapPairTotalReservesSnapshot]:
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
        
        # for each Uniswap V3 pool, fetch reserves of token0 and token1 within them
        for pool_address in active_pool_addresses:
            self._logger.debug(
                "[Epoch {}-{}] Processing pool {} | Starting computation",
                min_chain_height,
                max_chain_height,
                pool_address
            )
            
            # fetch reserves of token0 and token1 within the pool
            # fetch prices of token0 and token1 within the pool
            # fetch timestamp of the pool
            # store the data in the snapshot
            project_id = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
            # Initialize dictionaries to store reserve and price data for each block
            epoch_reserves_snapshot_map_token0 = dict()
            epoch_prices_snapshot_map_token0 = dict()
            epoch_prices_snapshot_map_token1 = dict()
            epoch_reserves_snapshot_map_token1 = dict()
            epoch_usd_reserves_snapshot_map_token0 = dict()
            epoch_usd_reserves_snapshot_map_token1 = dict()
            max_block_timestamp = int(time.time())

            self._logger.debug(
                f"pair reserves {pool_address} computation init time {time.time()}"
            )
            
            # Fetch pair reserves for the entire block range of the epoch
            pair_reserve_total = await get_pair_reserves(
                pair_address=pool_address,
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                ipfs_reader=ipfs_reader,
                protocol_state_contract=protocol_state_contract,
            )

            # Process reserve data for each block in the epoch
            for block_num in range(min_chain_height, max_chain_height + 1):
                block_pair_total_reserves = pair_reserve_total.get(block_num)
                if not block_pair_total_reserves:
                    self._logger.error(
                        "[Epoch {}-{}] Pool {} | No token pair reserves data returned by 'get_pair_reserves()' for block {}",
                        min_chain_height,
                        max_chain_height,
                        pool_address,
                        block_num
                    )
                    continue
                # Store reserve and price data for each token
                epoch_reserves_snapshot_map_token0[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token0"]
                epoch_reserves_snapshot_map_token1[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token1"]
                epoch_usd_reserves_snapshot_map_token0[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token0USD"]
                epoch_usd_reserves_snapshot_map_token1[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token1USD"]
                epoch_prices_snapshot_map_token0[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token0Price"]
                epoch_prices_snapshot_map_token1[
                    f"block{block_num}"
                ] = block_pair_total_reserves["token1Price"]

                if not block_pair_total_reserves.get("timestamp", None):
                    self._logger.error(
                        "[Epoch {}-{}] Pool {} | Could not fetch timestamp for max block height. Using current timestamp",
                        min_chain_height,
                        max_chain_height,
                        pool_address
                    )
                else:
                    max_block_timestamp = block_pair_total_reserves.get(
                        "timestamp",
                    )

            # Create the final snapshot object
            pair_total_reserves_snapshot = UniswapPairTotalReservesSnapshot(
                **{
                    "token0Reserves": epoch_reserves_snapshot_map_token0,
                    "token1Reserves": epoch_reserves_snapshot_map_token1,
                    "token0ReservesUSD": epoch_usd_reserves_snapshot_map_token0,
                    "token1ReservesUSD": epoch_usd_reserves_snapshot_map_token1,
                    "token0Prices": epoch_prices_snapshot_map_token0,
                    "token1Prices": epoch_prices_snapshot_map_token1,
                    "chainHeightRange": EpochBaseSnapshot(
                        begin=min_chain_height,
                        end=max_chain_height,
                    ),
                    "timestamp": max_block_timestamp,
                    "contract": pool_address,
                },
            )
            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Computation completed",
                min_chain_height,
                max_chain_height,
                pool_address
            )

        return pair_total_reserves_snapshot