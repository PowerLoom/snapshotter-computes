import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.modules.computes.utils.core import get_nft_mints
from snapshotter.modules.computes.utils.models.data_models import EpochERC721Data
from snapshotter.modules.computes.utils.models.message_models import EpochBaseSnapshot
from snapshotter.modules.computes.utils.models.message_models import ERC721TransfersSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper


class NftMintProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='NftMintProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        data_source_contract_address = epoch.data_source

        self._logger.debug(f'nft mint {data_source_contract_address} computation init time {time.time()}')

        mint_data: EpochERC721Data = await get_nft_mints(
            data_source_contract_address=data_source_contract_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
        )

        self._logger.debug(f'nft mint {data_source_contract_address} computation end time {time.time()}')

        total_minted = mint_data.totalMinted
        total_unique_minters = mint_data.totalUniqueMinters
        data_by_block = mint_data.dataByBlock
        max_block_timestamp = mint_data.timestamp
        collection_name = mint_data.name
        collection_symbol = mint_data.symbol

        mint_data_snapshot = ERC721TransfersSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(
                begin=min_chain_height,
                end=max_chain_height,
            ),
            timestamp=max_block_timestamp,
            totalMinted=total_minted,
            totalUniqueMinters=total_unique_minters,
            dataByBlock=data_by_block,
            name=collection_name,
            symbol=collection_symbol,
        )

        return mint_data_snapshot
