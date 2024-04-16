from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from .constants import ERC721_EVENT_SIGS
from .constants import ERC721_EVENTS_ABI
from .constants import ZERO_ADDRESS
from .models.data_models import BlockMintData
from .models.data_models import EpochMintData
from .models.data_models import MintData

core_logger = logger.bind(module='PowerLoom|BaseSnapshots|Core')


async def get_nft_mints(
    data_source_contract_address,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    fetch_timestamp=True,
):
    """
    Fetches new mints for the provided NFT contract.
    """
    data_source_contract_address = Web3.to_checksum_address(
        data_source_contract_address,
    )
    block_details_dict = dict()

    if fetch_timestamp:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block=from_block,
                to_block=to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                (
                    'Error attempting to get block details of to_block {}:'
                    ' {}, retrying again'
                ),
                to_block,
                err,
            )
            raise err

    # fetch logs for transfers
    event_sig, event_abi = get_event_sig_and_abi(
        ERC721_EVENT_SIGS,
        ERC721_EVENTS_ABI,
    )

    events_log = await rpc_helper.get_events_logs(
        **{
            'contract_address': data_source_contract_address,
            'to_block': to_block,
            'from_block': from_block,
            'topics': [event_sig],
            'event_abi': event_abi,
            'redis_conn': redis_conn,
        },
    )

    # new mints are transfer events coming from the zero address
    mint_events = filter(lambda event: event['args']['from'] == ZERO_ADDRESS, events_log)

    mints_by_block = {}
    for event in mint_events:
        block_number = event['blockNumber']
        if block_number not in mints_by_block:
            mints_by_block[block_number] = []
        mints_by_block[block_number].append(event)

    epoch_mint_data = EpochMintData(
        mintsByBlock={},
        totalMinted=0,
        totalUniqueMinters=0,
        timestamp=0,
    )

    # iterate over the transfer events and create the mint data
    total_mints = 0
    minters = set()
    for block_number, mint_events in mints_by_block.items():
        block_details = block_details_dict.get(block_number, {})
        block_timestamp = block_details.get('timestamp', None)
        block_mint_data = BlockMintData(
            mints=[],
            timestamp=block_timestamp,
        )

        for mint_event in mint_events:
            mint_data = MintData(
                minter=mint_event['args']['to'],
                tokenId=mint_event['args']['tokenId'],
                transactionHash=mint_event['transactionHash'].hex(),
            )
            block_mint_data.mints.append(mint_data)
            total_mints += 1
            minters.add(mint_data.minter)

        epoch_mint_data.mintsByBlock[block_number] = block_mint_data

    max_block_details = block_details_dict.get(to_block, dict())
    max_block_timestamp = max_block_details.get('timestamp', None)

    epoch_mint_data.totalMinted = total_mints
    epoch_mint_data.totalUniqueMinters = len(minters)
    epoch_mint_data.timestamp = max_block_timestamp

    return epoch_mint_data
