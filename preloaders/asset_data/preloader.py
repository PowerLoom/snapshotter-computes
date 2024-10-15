from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper

from computes.utils.helpers import get_bulk_asset_data


class BulkAssetDataPreloader(GenericPreloader):
    """
    A preloader class for fetching bulk asset data for Aave protocol.
    
    This class is responsible for retrieving asset reserve data for all blocks
    in a given range and for all assets in the Aave pool.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkAssetDataPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and store bulk asset data for the given epoch.

        Args:
            epoch (EpochBase): The epoch object containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching data.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        Raises:
            Exception: If there's an error during the bulk asset data retrieval process.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        try:
            # Get asset reserve data for all blocks in range for all assets in the Aave pool
            await get_bulk_asset_data(
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                from_block=min_chain_height,
                to_block=max_chain_height,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Asset Data preloader: {e}')
            raise e
        finally:
            # Ensure Redis connection is closed even if an exception occurs
            await redis_conn.close()

    async def cleanup(self):
        """Perform any necessary cleanup operations."""
        pass
