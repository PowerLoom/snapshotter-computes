from redis import asyncio as aioredis
from computes.utils.helpers import get_pool_supply_events
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper


class AaveBulkVolumeEventsPreloader(GenericPreloader):
    """
    A preloader class for fetching bulk volume events data for the Aave protocol.
    
    This class is responsible for retrieving core Aave supply events for all blocks
    in a given range and for all assets in the Aave pool.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkVolumeEventsPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and store bulk volume events data for the given epoch.

        Args:
            epoch (EpochBase): The epoch object containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching data.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        Raises:
            Exception: If there's an error during the bulk volume events data retrieval process.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        try:
            # Get core Aave supply events for all blocks in range for all assets in the Aave pool
            await get_pool_supply_events(
                rpc_helper=rpc_helper,
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Volume Event preloader: {e}')
            raise e
        finally:
            # Ensure Redis connection is closed even if an exception occurs
            await redis_conn.close()

    async def cleanup(self):
        """Perform any necessary cleanup operations."""
        pass
