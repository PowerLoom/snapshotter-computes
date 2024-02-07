from redis import asyncio as aioredis
from snapshotter.modules.computes.utils.helpers import get_pool_supply_events
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper


class AaveBulkVolumeEventsPreloader(GenericPreloader):
    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkVolumeEventsPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,

    ):
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        # get core aave supply eventsfor all blocks in range for all assets in the Aave pool
        try:
            await get_pool_supply_events(
                rpc_helper=rpc_helper,
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Volume Event preloader: {e}')
        finally:
            await redis_conn.close()

    async def cleanup(self):
        pass
