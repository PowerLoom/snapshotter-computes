from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from .constants import ERC1155_EVENT_SIGS
from .constants import ERC1155_EVENTS_ABI
from .constants import ERC721_EVENT_SIGS
from .constants import ERC721_EVENTS_ABI
from .constants import ZERO_ADDRESS
from .helpers import get_collection_metadata
from .models.data_models import BlockNftData
from .models.data_models import EpochERC721Data
from .models.data_models import EpochNftData
from .models.data_models import MintData
from .models.data_models import NftTransferTypes
from .models.data_models import TransferData

core_logger = logger.bind(module='PowerLoom|NftDataSnapshots|Core')


async def get_erc721_transfers(
    data_source_contract_address,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    fetch_timestamp=True,
):
    """
    Fetches new transfers and mints for the provided ERC721 contract.
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

    collection_metadata = await get_collection_metadata(
        contract_address=data_source_contract_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

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

    transfers_by_block = {}
    for event in events_log:
        block_number = event['blockNumber']
        if block_number not in transfers_by_block:
            transfers_by_block[block_number] = []
        transfers_by_block[block_number].append(event)

    epoch_data = EpochERC721Data(
        dataByBlock={},
        totalMinted=0,
        totalUniqueMinters=0,
        name=collection_metadata['name'],
        symbol=collection_metadata['symbol'],
        timestamp=0,
    )

    # iterate over the transfer events and create the mint data
    total_mints = 0
    minters = set()
    for block_number, transfer_events in transfers_by_block.items():
        block_details = block_details_dict.get(block_number, {})
        block_timestamp = block_details.get('timestamp', None)
        block_data = BlockNftData(
            mints=[],
            transfers=[],
            timestamp=block_timestamp,
        )

        for event in transfer_events:

            # Mint is a transfer event from the zero address
            if event['args']['from'] == ZERO_ADDRESS:
                mint_data = MintData(
                    minterAddress=event['args']['to'],
                    tokenId=event['args']['tokenId'],
                    transactionHash=event['transactionHash'].hex(),
                )
                block_data.mints.append(mint_data)
                total_mints += 1
                minters.add(mint_data.minterAddress)

            # Transfer is from a non-zero address
            else:
                transfer_data = TransferData(
                    fromAddress=event['args']['from'],
                    toAddress=event['args']['to'],
                    tokenId=event['args']['tokenId'],
                    transactionHash=event['transactionHash'].hex(),
                )
                block_data.transfers.append(transfer_data)

        epoch_data.dataByBlock[block_number] = block_data

    max_block_details = block_details_dict.get(to_block, dict())
    max_block_timestamp = max_block_details.get('timestamp', None)

    epoch_data.totalMinted = total_mints
    epoch_data.totalUniqueMinters = len(minters)
    epoch_data.timestamp = max_block_timestamp

    return epoch_data


async def get_erc1155_transfers(
    data_source_contract_address,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    fetch_timestamp=True,
):
    """
    Fetches new transfers and mints for the provided ERC1155 contract.
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
        ERC1155_EVENT_SIGS,
        ERC1155_EVENTS_ABI,
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

    transfers_by_block = {}
    for event in events_log:
        block_number = event['blockNumber']
        if block_number not in transfers_by_block:
            transfers_by_block[block_number] = []
        transfers_by_block[block_number].append(event)

    epoch_data = EpochNftData(
        dataByBlock={},
        totalMinted=0,
        totalUniqueMinters=0,
        timestamp=0,
    )

    # iterate over the transfer events and create the mint data
    total_mints = 0
    minters = set()
    for block_number, transfer_events in transfers_by_block.items():
        block_details = block_details_dict.get(block_number, {})
        block_timestamp = block_details.get('timestamp', None)
        block_data = BlockNftData(
            mints=[],
            transfers=[],
            timestamp=block_timestamp,
        )

        for event in transfer_events:

            if event['event'] == NftTransferTypes.TRANSFER_SINGLE.value:
                # For ERC1155, a value greater than 1 indicates that the transferred token is not an NFT
                if event['args']['value'] != 1:
                    continue

                # A mint for ERC1155 is a transfer event from the zero address
                if event['args']['from'] == ZERO_ADDRESS:
                    mint_data = MintData(
                        minterAddress=event['args']['to'],
                        tokenId=event['args']['id'],
                        transactionHash=event['transactionHash'].hex(),
                    )
                    block_data.mints.append(mint_data)
                    total_mints += 1
                    minters.add(mint_data.minterAddress)
                else:
                    transfer_data = TransferData(
                        fromAddress=event['args']['from'],
                        toAddress=event['args']['to'],
                        tokenId=event['args']['id'],
                        transactionHash=event['transactionHash'].hex(),
                    )
                    block_data.transfers.append(transfer_data)

            # Event is a TransferBatch event
            else:
                token_ids = event['args']['ids']
                token_values = event['args']['values']

                for token_id, value in zip(token_ids, token_values):
                    if value != 1:
                        continue
                    if event['args']['from'] == ZERO_ADDRESS:
                        mint_data = MintData(
                            minterAddress=event['args']['to'],
                            tokenId=token_id,
                            transactionHash=event['transactionHash'].hex(),
                        )
                        block_data.mints.append(mint_data)
                        total_mints += 1
                        minters.add(mint_data.minterAddress)
                    else:
                        transfer_data = TransferData(
                            fromAddress=event['args']['from'],
                            toAddress=event['args']['to'],
                            tokenId=token_id,
                            transactionHash=event['transactionHash'].hex(),
                        )
                        block_data.transfers.append(transfer_data)

        epoch_data.dataByBlock[block_number] = block_data

    max_block_details = block_details_dict.get(to_block, dict())
    max_block_timestamp = max_block_details.get('timestamp', None)

    epoch_data.totalMinted = total_mints
    epoch_data.totalUniqueMinters = len(minters)
    epoch_data.timestamp = max_block_timestamp

    return epoch_data
