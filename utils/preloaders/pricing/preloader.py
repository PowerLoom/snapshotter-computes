from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper

from ....utils.pricing import get_all_asset_prices


class BulkPricePreloader(GenericPreloader):
    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkPricingPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,

    ):
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        # get usd prices for all blocks in range for all assets in the Aave pool
        # return dict of dicts {asset_address: {block_height: usd_price, ...} ...}
        try:
            await get_all_asset_prices(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Price preloader: {e}')
        finally:
            await redis_conn.close()

    async def cleanup(self):
        pass
