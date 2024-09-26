import asyncio

from redis import asyncio as aioredis
from web3 import Web3
import math
from computes.redis_keys import uniswap_pair_contract_tokens_addresses
from computes.redis_keys import uniswap_pair_contract_tokens_data
from computes.redis_keys import uniswap_tokens_pair_map
from computes.settings.config import settings as worker_settings
from computes.utils.constants import current_node
from computes.utils.constants import erc20_abi
from computes.utils.constants import pair_contract_abi
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper


helper_logger = logger.bind(module='PowerLoom|Uniswap|Helpers')


def get_maker_pair_data(prop):
    """
    Get Maker token data based on the given property.

    Args:
        prop (str): The property to retrieve ('name', 'symbol', or any other).

    Returns:
        str: The corresponding Maker token data.
    """
    prop = prop.lower()
    if prop == 'name':
        return 'Maker'
    elif prop == 'symbol':
        return 'MKR'
    else:
        return 'Maker'


async def get_pair(
    factory_contract_obj,
    token0,
    token1,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the pair address for two tokens from the Uniswap factory contract.

    Args:
        factory_contract_obj: The Uniswap factory contract object.
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for making Web3 calls.

    Returns:
        str: The pair address.
    """
    # Check if pair cache exists
    pair_address_cache = await redis_conn.hget(
        uniswap_tokens_pair_map,
        f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}',
    )
    if pair_address_cache:
        pair_address_cache = pair_address_cache.decode('utf-8')
        return Web3.to_checksum_address(pair_address_cache)

    # If not in cache, fetch from the contract
    tasks = [
        ('getPair', [Web3.to_checksum_address(token0), Web3.to_checksum_address(token1)])
    ]

    result = await rpc_helper.web3_call(
        tasks=tasks,
        contract_addr=factory_contract_obj.address,
        abi=factory_contract_obj.abi,
    )
    pair = result[0]

    # Cache the pair address
    await redis_conn.hset(
        name=uniswap_tokens_pair_map,
        mapping={
            f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}': Web3.to_checksum_address(
                pair,
            ),
        },
    )

    return pair


async def get_pair_metadata(
    pair_address,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get metadata for a Uniswap pair.

    This function retrieves information on the tokens contained within a pair contract,
    including name, symbol, and decimals of token0 and token1. It also returns the pair
    symbol by concatenating {token0Symbol}-{token1Symbol}.

    Args:
        pair_address (str): The address of the pair contract.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for making Web3 calls.

    Returns:
        dict: A dictionary containing metadata for token0, token1, and the pair.

    Raises:
        Exception: If there's an error fetching the metadata.
    """
    try:
        pair_address = Web3.to_checksum_address(pair_address)

        # Check if cache exists
        (
            pair_token_addresses_cache,
            pair_tokens_data_cache,
        ) = await asyncio.gather(
            redis_conn.hgetall(
                uniswap_pair_contract_tokens_addresses.format(pair_address),
            ),
            redis_conn.hgetall(
                uniswap_pair_contract_tokens_data.format(pair_address),
            ),
        )

        # Parse addresses cache or call eth rpc
        token0Addr = None
        token1Addr = None
        if pair_token_addresses_cache:
            token0Addr = Web3.to_checksum_address(
                pair_token_addresses_cache[b'token0Addr'].decode('utf-8'),
            )
            token1Addr = Web3.to_checksum_address(
                pair_token_addresses_cache[b'token1Addr'].decode('utf-8'),
            )
        else:
            # Fetch token addresses from the pair contract
            pair_contract_obj = current_node['web3_client'].eth.contract(
                address=Web3.to_checksum_address(pair_address),
                abi=pair_contract_abi,
            )
            token0Addr, token1Addr = await rpc_helper.web3_call(
                tasks=[
                    ('token0', []),
                    ('token1', [])
                ],
                contract_addr=pair_contract_obj.address,
                abi=pair_contract_obj.abi,
            )

            # Cache the token addresses
            await redis_conn.hset(
                name=uniswap_pair_contract_tokens_addresses.format(
                    pair_address,
                ),
                mapping={
                    'token0Addr': token0Addr,
                    'token1Addr': token1Addr,
                },
            )

        # Create token contract objects
        token0 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token0Addr),
            abi=erc20_abi,
        )
        token1 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token1Addr),
            abi=erc20_abi,
        )

        # Parse token data cache or call eth rpc
        if pair_tokens_data_cache:
            token0_decimals = pair_tokens_data_cache[b'token0_decimals'].decode('utf-8')
            token1_decimals = pair_tokens_data_cache[b'token1_decimals'].decode('utf-8')
            token0_symbol = pair_tokens_data_cache[b'token0_symbol'].decode('utf-8')
            token1_symbol = pair_tokens_data_cache[b'token1_symbol'].decode('utf-8')
            token0_name = pair_tokens_data_cache[b'token0_name'].decode('utf-8')
            token1_name = pair_tokens_data_cache[b'token1_name'].decode('utf-8')
        else:
            # Prepare tasks for fetching token data
            token0_tasks = []
            token1_tasks = []

            # Special case to handle Maker token
            maker_token0 = None
            maker_token1 = None
            if Web3.to_checksum_address(worker_settings.contract_addresses.MAKER) == Web3.to_checksum_address(token0Addr):
                token0_name = get_maker_pair_data('name')
                token0_symbol = get_maker_pair_data('symbol')
                maker_token0 = True
            else:
                token0_tasks.extend([('name', []), ('symbol', [])])
            token0_tasks.append(('decimals', []))

            if Web3.to_checksum_address(worker_settings.contract_addresses.MAKER) == Web3.to_checksum_address(token1Addr):
                token1_name = get_maker_pair_data('name')
                token1_symbol = get_maker_pair_data('symbol')
                maker_token1 = True
            else:
                token1_tasks.extend([('name', []), ('symbol', [])])
            token1_tasks.append(('decimals', []))

            # Fetch token data
            if maker_token1:
                token0_name, token0_symbol, token0_decimals = await rpc_helper.web3_call(
                    token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
                [token1_decimals] = await rpc_helper.web3_call(
                    token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )
            elif maker_token0:
                token1_name, token1_symbol, token1_decimals = await rpc_helper.web3_call(
                    token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )
                [token0_decimals] = await rpc_helper.web3_call(
                    token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
            else:
                token0_name, token0_symbol, token0_decimals = await rpc_helper.web3_call(
                    token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
                token1_name, token1_symbol, token1_decimals = await rpc_helper.web3_call(
                    token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )

            # Cache the token data
            await redis_conn.hset(
                name=uniswap_pair_contract_tokens_data.format(pair_address),
                mapping={
                    'token0_name': token0_name,
                    'token0_symbol': token0_symbol,
                    'token0_decimals': token0_decimals,
                    'token1_name': token1_name,
                    'token1_symbol': token1_symbol,
                    'token1_decimals': token1_decimals,
                    'pair_symbol': f'{token0_symbol}-{token1_symbol}',
                },
            )

        # Return the metadata
        return {
            'token0': {
                'address': token0Addr,
                'name': token0_name,
                'symbol': token0_symbol,
                'decimals': token0_decimals,
            },
            'token1': {
                'address': token1Addr,
                'name': token1_name,
                'symbol': token1_symbol,
                'decimals': token1_decimals,
            },
            'pair': {
                'symbol': f'{token0_symbol}-{token1_symbol}',
            },
        }
    except Exception as err:
        # Log the error and raise it for retry in the next cycle
        helper_logger.opt(exception=True).error(
            f'RPC error while fetching metadata for pair {pair_address}, error_msg:{err}'
        )
        raise err


def truncate(number, decimals=5):
    """
    Truncate a number to a specific number of decimal places.

    Args:
        number (float): The number to truncate.
        decimals (int, optional): The number of decimal places to keep. Defaults to 5.

    Returns:
        float: The truncated number.

    Raises:
        TypeError: If decimals is not an integer.
        ValueError: If decimals is negative.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor
