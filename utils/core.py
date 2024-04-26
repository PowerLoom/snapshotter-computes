from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from .constants import aggregator_contract_abi
from .helpers import get_oracle_metadata
from .models.data_models import LatestRoundData


core_logger = logger.bind(module='PowerLoom|ChainlinkOracleSnapshots|Core')


async def get_oracle_answers(
    oracle_contract_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    fetch_timestamp: bool = True,
):

    core_logger.debug(
        f'Starting chainlink oracle query for: {oracle_contract_address}',
    )
    oracle_address = Web3.toChecksumAddress(oracle_contract_address)

    if fetch_timestamp:
        # get block details for the range in order to get the timestamps
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
    else:
        block_details_dict = dict()

    # get the decimals and description of the oracle
    oracle_metadata = await get_oracle_metadata(
        oracle_address,
        redis_conn,
        rpc_helper,
    )

    # create dictionary of ABI {function_name -> {signature, abi, input, output}}
    oracle_abi_dict = get_contract_abi_dict(aggregator_contract_abi)

    # get the oracle's latest data for each block in the epoch
    oracle_data_array = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=oracle_abi_dict,
        function_name='latestRoundData',
        contract_address=oracle_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    oracle_decimals = oracle_metadata['decimals']
    oracle_answers_dict = dict()

    # iterate over the batch data response and construct data for each block
    for idx, block_num in enumerate(range(from_block, to_block + 1)):
        current_block_details = block_details_dict.get(block_num, None)
        oracle_data = oracle_data_array[idx]

        answer = oracle_data[1] / 10 ** int(oracle_decimals)

        timestamp = (
            current_block_details.get(
                'timestamp',
                0,
            )
            if current_block_details
            else 0
        )

        oracle_answers_dict[block_num] = LatestRoundData(
            answer=answer,
            roundId=oracle_data[0],
            startedAt=oracle_data[2],
            updatedAt=oracle_data[3],
            answeredInRound=oracle_data[4],
            blockTimestamp=timestamp,
        )

    core_logger.debug(
        (
            'Finished chainlink oracle query for epoch-range:'
            f' {from_block} - {to_block} | oracle_contract: {oracle_address}'
        ),
    )

    return oracle_answers_dict
