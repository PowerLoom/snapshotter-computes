from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper

from ....utils.helpers import get_bulk_asset_data


class BulkAssetDataPreloader(GenericPreloader):
    def __init__(self) -> None:
        self._logger = logger.bind(module='AaveBulkAssetDataPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,

    ):
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        # get asset reserve data for all blocks in range for all assets in the Aave pool
        # return dict of dicts {asset_address: {block_height: data, ...} ...}
        try:
            await get_bulk_asset_data(
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                from_block=min_chain_height,
                to_block=max_chain_height,
            )
        except Exception as e:
            self._logger.error(f'Error in Bulk Asset Data preloader: {e}')
        finally:
            await redis_conn.close()

    async def cleanup(self):
        pass
