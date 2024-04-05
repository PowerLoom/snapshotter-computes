from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .utils.core import get_block_details
from .utils.models.data_models import BlockDetails
from .utils.models.message_models import BlockDetailsSnapshot


class BlockDetailsProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='BlockDetailsProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        
        block_details_snapshot_map_timestamp = dict()
        block_details_snapshot_map_transactions = dict()

        epoch_block_details: BlockDetails = await get_block_details(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            details = epoch_block_details.details.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False
            
            block_details_snapshot_map_timestamp[block_num] = details.timestamp
            block_details_snapshot_map_transactions[block_num] = details.transactions

            if fetch_ts:
                if not details.timestamp:
                    self._logger.error(
                        (
                            'Could not fetch timestamp against max block'
                            ' height in epoch {} - {} for get block details'
                            ' Using current timestamp for snapshot construction'
                        ),
                        min_chain_height,
                        max_chain_height,
                    )
                else:
                    max_block_timestamp = details.timestamp

        epoch_block_details_snapshot = BlockDetailsSnapshot(
            chainHeightRange={
                'begin': min_chain_height,
                'end': max_chain_height,
            },
            blockTimestamps=block_details_snapshot_map_timestamp,
            blockTransactions=block_details_snapshot_map_transactions,
            timestamp=max_block_timestamp,
        )

        return epoch_block_details_snapshot