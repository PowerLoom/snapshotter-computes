from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper

from computes.utils.pricing import get_all_asset_prices


class BulkPricePreloader(GenericPreloader):
    """
    A preloader class for fetching bulk price data for assets in the Aave protocol.
    
    This class is responsible for retrieving USD prices for all blocks in a given range
    and for all assets in the Aave pool.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkPricingPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and store bulk price data for the given epoch.

        Args:
            epoch (EpochBase): The epoch object containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching data.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        Raises:
            Exception: If there's an error during the bulk price data retrieval process.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        try:
            # Get USD prices for all blocks in range for all assets in the Aave pool
            # Returns a dict of dicts {asset_address: {block_height: usd_price, ...} ...}
            await get_all_asset_prices(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Price preloader: {e}')
        finally:
            # Ensure Redis connection is closed even if an exception occurs
            await redis_conn.close()

    async def cleanup(self):
        """Perform any necessary cleanup operations."""
        pass
