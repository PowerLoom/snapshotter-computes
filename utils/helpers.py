import math

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import collection_metadata_map
from .constants import current_node
from .constants import erc721_abi


helper_logger = logger.bind(module='PowerLoom|NftDataSnapshots|Helpers')


async def get_collection_metadata(
    contract_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    returns information on the nft collection located at the contract address - name, symbol
    """
    collection_address = Web3.toChecksumAddress(contract_address)

    collection_address_cache = await redis_conn.hgetall(
        collection_metadata_map.format(collection_address),
    )

    if collection_address_cache:
        collection_name = collection_address_cache[b'name'].decode('utf-8')
        collection_symbol = collection_address_cache[b'symbol'].decode('utf-8')

        return {
            'name': collection_name,
            'symbol': collection_symbol,
        }

    collection_contract_obj = current_node['web3_client'].eth.contract(
        address=Web3.toChecksumAddress(collection_address),
        abi=erc721_abi,
    )

    collection_name, collection_symbol = await rpc_helper.web3_call(
        tasks=[
            collection_contract_obj.functions.name().call(),
            collection_contract_obj.functions.symbol().call(),
        ],
        redis_conn=redis_conn,
    )

    await redis_conn.hset(
        name=collection_metadata_map.format(
            collection_address,
        ),
        mapping={
            'name': collection_name,
            'symbol': collection_symbol,
        },
    )

    return {
        'name': collection_name,
        'symbol': collection_symbol,
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
