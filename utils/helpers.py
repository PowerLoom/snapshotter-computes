import math

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import oracle_metadata_map
from .constants import aggregator_contract_abi
from .constants import current_node


helper_logger = logger.bind(module='PowerLoom|ChainlinkOracleSnapshots|Helpers')


async def get_oracle_metadata(
    oracle_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    returns information on the nft collection located at the contract address - name, symbol
    """
    oracle_address = Web3.toChecksumAddress(oracle_address)

    oracle_address_cache = await redis_conn.hgetall(
        oracle_metadata_map.format(oracle_address),
    )

    if oracle_address_cache:
        oracle_decimals = oracle_address_cache[b'decimals'].decode('utf-8')
        oracle_description = oracle_address_cache[b'description'].decode('utf-8')

        oracle_decimals = int(oracle_decimals)

        return {
            'decimals': oracle_decimals,
            'description': oracle_description,
        }

    oracle_contract_obj = current_node['web3_client'].eth.contract(
        address=Web3.toChecksumAddress(oracle_address),
        abi=aggregator_contract_abi,
    )

    oracle_decimals, oracle_description = await rpc_helper.web3_call(
        tasks=[
            oracle_contract_obj.functions.decimals(),
            oracle_contract_obj.functions.description(),
        ],
        redis_conn=redis_conn,
    )

    oracle_decimals = int(oracle_decimals)

    await redis_conn.hset(
        name=oracle_metadata_map.format(
            oracle_address,
        ),
        mapping={
            'decimals': oracle_decimals,
            'description': oracle_description,
        },
    )

    return {
        'decimals': oracle_decimals,
        'description': oracle_description,
    }


def truncate(number, decimals=5):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError('decimal places must be an integer.')
    elif decimals < 0:
        raise ValueError('decimal places has to be 0 or more.')
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor
