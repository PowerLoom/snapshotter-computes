import asyncio
import json
import math

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from computes.redis_keys import uniswap_cached_block_height_token_eth_price
from computes.redis_keys import uniswap_pair_contract_tokens_addresses
from computes.redis_keys import uniswap_pair_contract_tokens_data
from computes.redis_keys import uniswap_tokens_pair_map
from computes.redis_keys import uniswap_v3_best_pair_map
from computes.redis_keys import uniswap_v3_token_stable_pair_map
from computes.settings.config import settings as worker_settings
from computes.utils.constants import current_node
from computes.utils.constants import erc20_abi
from computes.utils.constants import factory_contract_obj
from computes.utils.constants import pair_contract_abi
from computes.utils.constants import STABLE_TOKENS_LIST
from computes.utils.constants import TOKENS_DECIMALS
from computes.utils.constants import ZER0_ADDRESS
from computes.preloaders.eth_price.preloader import eth_price_preloader

helper_logger = logger.bind(module='PowerLoom|Uniswap|Helpers')


def get_maker_pair_data(prop):
    """
    Get Maker token data based on the given property.
    
    Args:
        prop (str): The property to retrieve ('name', 'symbol', or other).
    
    Returns:
        str: The requested Maker token data.
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
    fee,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the pair address for two tokens and the given fee, using redis cache when available.

    Args:
        factory_contract_obj: The factory contract object.
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        fee (int): The fee for the pair.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The pair address.
    """
    # check if pair cache exists
    pair_address_cache = await redis_conn.hget(
        uniswap_tokens_pair_map,
        f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}|{fee}',
    )
    if pair_address_cache:
        pair_address_cache = pair_address_cache.decode('utf-8')
        return Web3.to_checksum_address(pair_address_cache)

    tasks = [
        ('getPool', [Web3.to_checksum_address(token0), Web3.to_checksum_address(token1), fee]),
    ]

    result = await rpc_helper.web3_call(
        tasks=tasks,
        contract_addr=factory_contract_obj.address,
        abi=factory_contract_obj.abi,
    )
    pair = result[0]
    # cache the pair address
    await redis_conn.hset(
        name=uniswap_tokens_pair_map,
        mapping={
            f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}|{fee}': Web3.to_checksum_address(
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
    Get information on the tokens contained within a pair contract.

    This function retrieves the name, symbol, and decimals of token0 and token1,
    as well as the pair symbol by concatenating {token0Symbol}-{token1Symbol}.

    Args:
        pair_address (str): The address of the pair contract.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary containing token and pair information.

    Raises:
        Exception: If there's an error fetching metadata for the pair.
    """
    try:
        pair_address = Web3.to_checksum_address(pair_address)

        # check if cache exist
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

        # parse addresses cache or call eth rpc
        token0Addr = None
        token1Addr = None
        if pair_token_addresses_cache:
            token0Addr = Web3.to_checksum_address(
                pair_token_addresses_cache[b'token0Addr'].decode('utf-8'),
            )
            token1Addr = Web3.to_checksum_address(
                pair_token_addresses_cache[b'token1Addr'].decode('utf-8'),
            )
            fee = pair_token_addresses_cache[b'fee'].decode('utf-8')
        else:
            # TODO: use rpc_helper batch_web3_call
            token0Addr, token1Addr, fee = await rpc_helper.web3_call(
                tasks=[
                    ('token0', []),
                    ('token1', []),
                    ('fee', []),
                ],
                contract_addr=pair_address,
                abi=pair_contract_abi,
            )

            await redis_conn.hset(
                name=uniswap_pair_contract_tokens_addresses.format(
                    pair_address,
                ),
                mapping={
                    'token0Addr': token0Addr,
                    'token1Addr': token1Addr,
                    'fee': fee,
                },
            )

        # token0 contract
        token0 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token0Addr),
            abi=erc20_abi,
        )
        # token1 contract
        token1 = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(token1Addr),
            abi=erc20_abi,
        )

        # parse token data cache or call eth rpc
        if pair_tokens_data_cache:
            token0_decimals = pair_tokens_data_cache[b'token0_decimals'].decode('utf-8')
            token1_decimals = pair_tokens_data_cache[b'token1_decimals'].decode('utf-8')
            token0_symbol = pair_tokens_data_cache[b'token0_symbol'].decode('utf-8')
            token1_symbol = pair_tokens_data_cache[b'token1_symbol'].decode('utf-8')
            token0_name = pair_tokens_data_cache[b'token0_name'].decode('utf-8')
            token1_name = pair_tokens_data_cache[b'token1_name'].decode('utf-8')
        else:
            # prepare tasks for fetching token data
            token0_tasks = []
            token1_tasks = []

            # special case to handle maker token
            maker_token0 = None
            maker_token1 = None

            if Web3.to_checksum_address(token0Addr) == Web3.to_checksum_address(worker_settings.contract_addresses.MAKER):
                token0_name = get_maker_pair_data('name')
                token0_symbol = get_maker_pair_data('symbol')
                maker_token0 = True
            else:
                token0_tasks.extend([('name', []), ('symbol', [])])
            token0_tasks.append(('decimals', []))

            if Web3.to_checksum_address(token1Addr) == Web3.to_checksum_address(worker_settings.contract_addresses.MAKER):
                token1_name = get_maker_pair_data('name')
                token1_symbol = get_maker_pair_data('symbol')
                maker_token1 = True
            else:
                token1_tasks.extend([('name', []), ('symbol', [])])
            token1_tasks.append(('decimals', []))

            if maker_token1:
                [token0_name, token0_symbol, token0_decimals] = await rpc_helper.web3_call(
                    tasks=token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
                [token1_decimals] = await rpc_helper.web3_call(
                    token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )
            elif maker_token0:
                [token1_name, token1_symbol, token1_decimals] = await rpc_helper.web3_call(
                    tasks=token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )
                [token0_decimals] = await rpc_helper.web3_call(
                    tasks=token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
            else:
                [token0_name, token0_symbol, token0_decimals] = await rpc_helper.web3_call(
                    tasks=token0_tasks,
                    contract_addr=token0.address,
                    abi=token0.abi,
                )
                [token1_name, token1_symbol, token1_decimals] = await rpc_helper.web3_call(
                    tasks=token1_tasks,
                    contract_addr=token1.address,
                    abi=token1.abi,
                )

            # cache the token data
            await redis_conn.hset(
                name=uniswap_pair_contract_tokens_data.format(pair_address),
                mapping={
                    'token0_name': token0_name,
                    'token0_symbol': token0_symbol,
                    'token0_decimals': token0_decimals,
                    'token1_name': token1_name,
                    'token1_symbol': token1_symbol,
                    'token1_decimals': token1_decimals,
                    'pair_symbol': f'{token0_symbol}-{token1_symbol}|{fee}',
                },
            )

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
                'symbol': f'{token0_symbol}-{token1_symbol}|{fee}',
                'address': pair_address,
                'fee': fee,
            },
        }
    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetcing metadata for pair {pair_address},'
                f' error_msg:{err}'
            ),
        )
        raise err


async def get_token_eth_price_dict(
    token_address: str,
    token_decimals: int,
    from_block,
    to_block,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Get a dictionary of token prices in ETH for each block and store it in Redis.

    Args:
        token_address (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn: Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary mapping block numbers to token prices in ETH.

    Raises:
        Exception: If there's an error fetching token prices.
    """

    token_address = Web3.to_checksum_address(token_address)
    # check if cache exists
    token_eth_price_dict = dict()
    cached_token_price_dict = await redis_conn.zrangebyscore(
        name=uniswap_cached_block_height_token_eth_price.format(token_address),
        min=from_block,
        max=to_block,
    )
    if len(cached_token_price_dict) > 0:
        token_eth_price_dict = {
            int(json.loads(price)['blockHeight']): json.loads(price)['price']
            for price in cached_token_price_dict
        }

        return token_eth_price_dict

    # get token price function takes care of its own rate limit
    # TODO repetitious refactor
    try:

        token_eth_quote = await get_token_eth_quote_from_uniswap(
            token_address=token_address,
            token_decimals=token_decimals,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )

        block_counter = 0
        # parse token_eth_quote and store in dict
        if len(token_eth_quote) > 0:
            token_eth_quote = [quote[0] for quote in token_eth_quote]
            for block_num in range(from_block, to_block + 1):
                token_eth_price_dict[block_num] = token_eth_quote[block_counter]
                block_counter += 1

                # cache price at height
        if len(token_eth_price_dict) > 0:

            redis_cache_mapping = {
                json.dumps({'blockHeight': height, 'price': price}): int(
                    height,
                )
                for height, price in token_eth_price_dict.items()
            }

            await redis_conn.zadd(
                name=uniswap_cached_block_height_token_eth_price.format(
                    Web3.to_checksum_address(token_address),
                ),
                mapping=redis_cache_mapping,  # timestamp so zset do not ignore same height on multiple heights
            )

            return token_eth_price_dict

        else:
            return token_eth_price_dict

    except Exception as e:
        # TODO BETTER ERROR HANDLING
        helper_logger.debug(f'error while fetching token price for {token_address}, error_msg:{e}')
        raise e


async def get_token_pair_address_with_fees(
    token0: str,
    token1: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the best pair address for two tokens based on liquidity across different fee tiers.

    Args:
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The address of the best pair contract.
    """

    # check if pair cache exists
    pair_address_cache = await redis_conn.hget(
        uniswap_v3_best_pair_map,
        f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}',
    )
    if pair_address_cache:
        pair_address_cache = pair_address_cache.decode('utf-8')
        return Web3.to_checksum_address(pair_address_cache)

    tasks = [
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(10000), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(3000), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(500), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
        get_pair(
            factory_contract_obj=factory_contract_obj, token0=token0, token1=token1,
            fee=int(100), redis_conn=redis_conn, rpc_helper=rpc_helper,
        ),
    ]
    pair_address_list = await asyncio.gather(*tasks)
    pair_address_list = [pair for pair in pair_address_list if pair != ZER0_ADDRESS]

    if len(pair_address_list) > 0:
        pair_contracts = [
            current_node['web3_client'].eth.contract(
                address=Web3.to_checksum_address(pair),
                abi=pair_contract_abi,
            ) for pair in pair_address_list
        ]

        tasks = [
            asyncio.create_task(
                rpc_helper.web3_call(
                    tasks=[('liquidity', [])],
                    contract_addr=pair_contract.address,
                    abi=pair_contract.abi,
                )
            ) for pair_contract in pair_contracts
        ]

        liquidity_list = await asyncio.gather(*tasks)

        pair_liquidity_dict = dict(zip(pair_address_list, liquidity_list))
        best_pair = max(pair_liquidity_dict, key=pair_liquidity_dict.get)

    else:
        best_pair = ZER0_ADDRESS

    # cache the pair address
    await redis_conn.hset(
        name=uniswap_v3_best_pair_map,
        mapping={
            f'{Web3.to_checksum_address(token0)}-{Web3.to_checksum_address(token1)}':
                best_pair,
        },
    )

    return best_pair


async def get_token_stable_pair_data(
    token: str,
    token_decimals: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the stable pair data for a given token.

    This function attempts to find a pair between the given token and a stable token.

    Args:
        token (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        dict: A dictionary containing token pair data.
    """
    # check if pair cache exists
    token_stable_pair_data_cache = await redis_conn.hgetall(
        uniswap_v3_token_stable_pair_map.format(Web3.to_checksum_address(token)),
    )
    if token_stable_pair_data_cache:
        token0 = token_stable_pair_data_cache[b'token0'].decode(
            'utf-8',
        )
        token1 = token_stable_pair_data_cache[b'token1'].decode(
            'utf-8',
        )
        token0_decimals = token_stable_pair_data_cache[b'token0_decimals'].decode(
            'utf-8',
        )
        token1_decimals = token_stable_pair_data_cache[b'token1_decimals'].decode(
            'utf-8',
        )
        pair = token_stable_pair_data_cache[b'pair'].decode(
            'utf-8',
        )

        data = {
            'token0': token0,
            'token1': token1,
            'token0_decimals': int(token0_decimals),
            'token1_decimals': int(token1_decimals),
            'pair': pair,
        }

        return data

    token_stable_pair = ZER0_ADDRESS
    token0 = token
    token1 = ZER0_ADDRESS
    token0_decimals = token_decimals
    token1_decimals = 0
    for stable_token in STABLE_TOKENS_LIST:

        if int(token, 16) < int(stable_token, 16):
            token0, token1 = token, stable_token
            token0_decimals, token1_decimals = token_decimals, TOKENS_DECIMALS.get(stable_token, 0)
        else:
            token0, token1 = stable_token, token
            token0_decimals, token1_decimals = TOKENS_DECIMALS.get(stable_token, 0), token_decimals

        pair = await get_token_pair_address_with_fees(
            token0=token0,
            token1=token1,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )

        if pair != ZER0_ADDRESS:
            token_stable_pair = pair
            break

    # cache the token-stable pair data
    await redis_conn.hset(
        name=uniswap_v3_token_stable_pair_map.format(Web3.to_checksum_address(token)),
        mapping={
            'token0': token0,
            'token1': token1,
            'token0_decimals': token0_decimals,
            'token1_decimals': token1_decimals,
            'pair': token_stable_pair,
        },
    )

    return {
        'token0': token0,
        'token1': token1,
        'token0_decimals': int(token0_decimals),
        'token1_decimals': int(token1_decimals),
        'pair': token_stable_pair,
    }


async def get_token_eth_quote_from_uniswap(
    token_address,
    token_decimals,
    from_block,
    to_block,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Get the ETH quote for a token from Uniswap.

    This function first attempts to price from a token-WETH pool. If that fails,
    it tries to find a token-stable coin pool and calculates the ETH price.

    Args:
        token_address (str): The address of the token.
        token_decimals (int): The number of decimals for the token.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn: Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        list: A list of tuples containing token prices in ETH for each block.

    Raises:
        Exception: If there's an error fetching token prices.
    """

    token0 = token_address
    token1 = worker_settings.contract_addresses.WETH
    token0_decimals = token_decimals
    token1_decimals = TOKENS_DECIMALS.get(worker_settings.contract_addresses.WETH, 18)
    if int(token1, 16) < int(token0, 16):
        token0, token1 = token1, token0
        token0_decimals, token1_decimals = token1_decimals, token0_decimals

    # first attempt to price from a token weth pool
    try:
        token_weth_pair = await get_token_pair_address_with_fees(
            token0=token0,
            token1=token1,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )

        token_eth_quote = []

        if token_weth_pair != ZER0_ADDRESS:
            response = await rpc_helper.batch_eth_call_on_block_range(
                abi_dict=get_contract_abi_dict(
                    abi=pair_contract_abi,
                ),
                contract_address=token_weth_pair,
                from_block=from_block,
                to_block=to_block,
                function_name='slot0',
                params=[],
            )
            sqrtP_list = [slot0[0] for slot0 in response]
            for sqrtP in sqrtP_list:
                price0, price1 = eth_price_preloader.sqrtPriceX96ToTokenPrices(
                    sqrtP,
                    token0_decimals,
                    token1_decimals,
                )

                if token0.lower() == token_address.lower():
                    token_eth_quote.append((price0,))
                else:
                    token_eth_quote.append((price1,))

            return token_eth_quote
        else:
            # since we couldnt find a token/weth pool, attempt to find a token/stable pool
            #  TODO -- rewrite with multicall
            token_stable_pair_data = await get_token_stable_pair_data(
                token=token_address,
                token_decimals=token_decimals,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )

            if token_stable_pair_data['pair'] != ZER0_ADDRESS:
                response = await rpc_helper.batch_eth_call_on_block_range(
                    abi_dict=get_contract_abi_dict(
                        abi=pair_contract_abi,
                    ),
                    contract_address=token_stable_pair_data['pair'],
                    from_block=from_block,
                    to_block=to_block,
                    function_name='slot0',
                    params=[],
                )

                eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
                    from_block=from_block,
                    to_block=to_block,
                    redis_conn=redis_conn,
                    rpc_helper=rpc_helper,
                )

                token0_decimals = token_stable_pair_data['token0_decimals']
                token1_decimals = token_stable_pair_data['token1_decimals']

                sqrtP_list = [slot0[0] for slot0 in response]
                sqrtP_eth_list = [block_price for block_price in eth_usd_price_dict.values()]
                token_eth_quote = []

                for i in range(len(sqrtP_list)):
                    sqrtP = sqrtP_list[i]
                    eth_price = sqrtP_eth_list[i]
                    price0, price1 = eth_price_preloader.sqrtPriceX96ToTokenPrices(
                        sqrtP,
                        token0_decimals,
                        token1_decimals,
                    )
                    if token0.lower() == token_address.lower():
                        token_eth_quote.append((price0 / eth_price,))
                    else:
                        token_eth_quote.append((price1 / eth_price,))

                return token_eth_quote
            else:
                return [(0,) for _ in range(from_block, to_block + 1)]
    except Exception as e:
        helper_logger.debug(f'error while fetching token price for {token_address}, error_msg:{e}')
        raise e


def truncate(number, decimals=5):
    """
    Truncate a number to a specific number of decimal places.

    Args:
        number (float): The number to truncate.
        decimals (int): The number of decimal places to keep (default: 5).

    Returns:
        float: The truncated number.

    Raises:
        TypeError: If decimals is not an integer.
        ValueError: If decimals is negative.
    """
    if not isinstance(decimals, int):
        raise TypeError('decimal places must be an integer.')
    elif decimals < 0:
        raise ValueError('decimal places has to be 0 or more.')
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor