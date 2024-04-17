import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.modules.computes.utils.core import get_erc1155_transfers
from snapshotter.modules.computes.utils.models.data_models import EpochNftData
from snapshotter.modules.computes.utils.models.message_models import EpochBaseSnapshot
from snapshotter.modules.computes.utils.models.message_models import NftTransfersSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper


class ERC1155TransferProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='ERC1155TransferProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        data_source_contract_address = epoch.data_source

        self._logger.debug(f'ERC1155 transfer {data_source_contract_address} computation init time {time.time()}')

        transfer_data: EpochNftData = await get_erc1155_transfers(
            data_source_contract_address=data_source_contract_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
        )

        self._logger.debug(f'ERC1155 transfer {data_source_contract_address} computation end time {time.time()}')

        total_minted = transfer_data.totalMinted
        total_unique_minters = transfer_data.totalUniqueMinters
        data_by_block = transfer_data.dataByBlock
        max_block_timestamp = transfer_data.timestamp

        mint_data_snapshot = NftTransfersSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
            timestamp=max_block_timestamp,
            totalMinted=total_minted,
            totalUniqueMinters=total_unique_minters,
            dataByBlock=data_by_block,
        )

        return mint_data_snapshot
