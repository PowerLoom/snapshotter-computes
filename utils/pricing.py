import asyncio
import json

from redis import asyncio as aioredis
from web3 import Web3

from computes.redis_keys import (
    uniswap_pair_cached_block_height_token_price,
)
from computes.redis_keys import (
    uniswap_token_derived_eth_cached_block_height,
)
from computes.settings.config import settings as worker_settings
from computes.utils.constants import factory_contract_obj
from computes.utils.constants import pair_contract_abi
from computes.utils.constants import router_contract_abi
from computes.utils.constants import tokens_decimals
from computes.utils.helpers import get_pair
from computes.utils.helpers import get_pair_metadata
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from computes.preloaders.eth_price.preloader import eth_price_preloader

pricing_logger = logger.bind(module='PowerLoom|Uniswap|Pricing')


async def get_token_pair_price_and_white_token_reserves(
    pair_address,
    from_block,
    to_block,
    pair_metadata,
    white_token,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Get token price based on pair reserves and whitelisted token reserves.

    This function calculates the token price and whitelisted token reserves for a given pair
    over a range of blocks.

    Args:
        pair_address (str): The address of the token pair.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        pair_metadata (dict): Metadata about the token pair.
        white_token (str): The address of the whitelisted token.
        redis_conn: Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for making blockchain calls.

    Returns:
        tuple: A tuple containing two dictionaries:
            - token_price_dict: Block number to token price mapping.
            - white_token_reserves_dict: Block number to white token reserves mapping.

    Raises:
        Exception: If unable to get pair price and white token reserves.
    """
    token_price_dict = dict()
    white_token_reserves_dict = dict()

    # Get pair contract ABI and fetch reserves for the block range
    pair_abi_dict = get_contract_abi_dict(pair_contract_abi)
    pair_reserves_list = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=pair_abi_dict,
        function_name='getReserves',
        contract_address=pair_address,
        from_block=from_block,
        to_block=to_block,
    )

    if len(pair_reserves_list) < to_block - (from_block - 1):
        pricing_logger.trace(
            (
                'Unable to get pair price and white token reserves'
                'from_block: {}, to_block: {}, pair_reserves_list: {}'
            ),
            from_block,
            to_block,
            pair_reserves_list,
        )

        raise Exception(
            'Unable to get pair price and white token reserves'
            f'from_block: {from_block}, to_block: {to_block}, '
            f'got result: {pair_reserves_list}',
        )

    # Calculate token price and white token reserves for each block
    for index, block_num in enumerate(range(from_block, to_block + 1)):
        token_price = 0

        # Adjust reserves based on token decimals
        pair_reserve_token0 = pair_reserves_list[index][0] / 10 ** int(
            pair_metadata['token0']['decimals'],
        )
        pair_reserve_token1 = pair_reserves_list[index][1] / 10 ** int(
            pair_metadata['token1']['decimals'],
        )

        # Handle cases where reserves are zero
        if float(pair_reserve_token0) == float(0) or float(pair_reserve_token1) == float(0):
            token_price_dict[block_num] = token_price
            white_token_reserves_dict[block_num] = 0
        elif Web3.to_checksum_address(pair_metadata['token0']['address']) == white_token:
            token_price_dict[block_num] = float(pair_reserve_token0 / pair_reserve_token1)
            white_token_reserves_dict[block_num] = pair_reserve_token0
        else:
            token_price_dict[block_num] = float(pair_reserve_token1 / pair_reserve_token0)
            white_token_reserves_dict[block_num] = pair_reserve_token1

    return token_price_dict, white_token_reserves_dict


async def get_token_derived_eth(
    from_block,
    to_block,
    token_metadata,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Get the token's derived ETH value over a range of blocks.

    This function calculates how much ETH a token is worth for each block in the given range.

    Args:
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        token_metadata (dict): Metadata about the token.
        redis_conn: Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for making blockchain calls.

    Returns:
        dict: A dictionary mapping block numbers to derived ETH values.

    Raises:
        Exception: If unable to get token derived ETH values.
    """
    token_derived_eth_dict = dict()
    token_address = Web3.to_checksum_address(token_metadata['address'])

    # If the token is WETH, set derived ETH as 1 for all blocks
    if token_address == Web3.to_checksum_address(worker_settings.contract_addresses.WETH):
        return {block_num: 1 for block_num in range(from_block, to_block + 1)}

    # Check cache for derived ETH values
    cached_derived_eth_dict = await redis_conn.zrangebyscore(
        name=uniswap_token_derived_eth_cached_block_height.format(token_address),
        min=int(from_block),
        max=int(to_block),
    )
    if cached_derived_eth_dict and len(cached_derived_eth_dict) == to_block - (from_block - 1):
        return {
            json.loads(price.decode('utf-8'))['blockHeight']: json.loads(price.decode('utf-8'))['price']
            for price in cached_derived_eth_dict
        }

    # If not in cache, calculate derived ETH values
    router_abi_dict = get_contract_abi_dict(router_contract_abi)
    token_derived_eth_list = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=router_abi_dict,
        function_name='getAmountsOut',
        contract_address=worker_settings.contract_addresses.iuniswap_v2_router,
        from_block=from_block,
        to_block=to_block,
        params=[
            10 ** int(token_metadata['decimals']),
            [
                Web3.to_checksum_address(token_metadata['address']),
                Web3.to_checksum_address(worker_settings.contract_addresses.WETH),
            ],
        ],
    )

    if len(token_derived_eth_list) < to_block - (from_block - 1):
        pricing_logger.trace(
            (
                'Unable to get token derived eth'
                'from_block: {}, to_block: {}, token_derived_eth_list: {}'
            ),
            from_block,
            to_block,
            token_derived_eth_list,
        )

        raise Exception(
            'Unable to get token derived eth'
            f'from_block: {from_block}, to_block: {to_block}, '
            f'got result: {token_derived_eth_list}',
        )

    # Process derived ETH values
    for index, block_num in enumerate(range(from_block, to_block + 1)):
        if not token_derived_eth_list[index]:
            token_derived_eth_dict[block_num] = 0
        else:
            _, derivedEth = token_derived_eth_list[index][0]
            token_derived_eth_dict[block_num] = (
                derivedEth / 10 ** tokens_decimals['WETH'] if derivedEth != 0 else 0
            )

    # Cache the results
    if token_derived_eth_dict:
        redis_cache_mapping = {
            json.dumps({'blockHeight': height, 'price': price}): int(height)
            for height, price in token_derived_eth_dict.items()
        }
        source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))
        await asyncio.gather(
            redis_conn.zadd(
                name=uniswap_token_derived_eth_cached_block_height.format(token_address),
                mapping=redis_cache_mapping,
            ),
            redis_conn.zremrangebyscore(
                name=uniswap_token_derived_eth_cached_block_height.format(token_address),
                min=0,
                max=int(from_block) - source_chain_epoch_size * 4,
            ),
        )

    return token_derived_eth_dict


async def get_token_price_in_block_range(
    token_metadata,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    debug_log=True,
):
    """
    Get the price of a token in USD for a given block range.

    This function calculates the USD price of a token for each block in the given range.
    It uses caching to improve performance and falls back to on-chain calculations when necessary.

    Args:
        token_metadata (dict): Metadata about the token.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for making blockchain calls.
        debug_log (bool): Whether to log debug information.

    Returns:
        dict: A dictionary mapping block numbers to token prices in USD.

    Raises:
        Exception: If an error occurs during price calculation.
    """
    try:
        token_price_dict = dict()
        token_address = Web3.to_checksum_address(token_metadata['address'])

        # Check cache for token prices
        cached_price_dict = await redis_conn.zrangebyscore(
            name=uniswap_pair_cached_block_height_token_price.format(token_address),
            min=int(from_block),
            max=int(to_block),
        )
        if cached_price_dict and len(cached_price_dict) == to_block - (from_block - 1):
            return {
                json.loads(price.decode('utf-8'))['blockHeight']: json.loads(price.decode('utf-8'))['price']
                for price in cached_price_dict
            }

        # If token is WETH, use ETH price in USD
        if token_address == Web3.to_checksum_address(worker_settings.contract_addresses.WETH):
            token_price_dict = await eth_price_preloader.get_eth_price_usd(
                from_block=from_block, to_block=to_block,
                redis_conn=redis_conn, rpc_helper=rpc_helper,
            )
        else:
            # Calculate token price using whitelisted token pairs
            token_eth_price_dict = dict()

            for white_token in worker_settings.uniswap_v2_whitelist:
                white_token = Web3.to_checksum_address(white_token)
                pairAddress = await get_pair(
                    factory_contract_obj, white_token, token_metadata['address'],
                    redis_conn, rpc_helper,
                )
                if pairAddress != '0x0000000000000000000000000000000000000000':
                    new_pair_metadata = await get_pair_metadata(
                        pair_address=pairAddress,
                        redis_conn=redis_conn,
                        rpc_helper=rpc_helper,
                    )
                    white_token_metadata = new_pair_metadata['token0'] if white_token == new_pair_metadata['token0']['address'] else new_pair_metadata['token1']

                    white_token_price_dict, white_token_reserves_dict = await get_token_pair_price_and_white_token_reserves(
                        pair_address=pairAddress, from_block=from_block, to_block=to_block,
                        pair_metadata=new_pair_metadata, white_token=white_token, redis_conn=redis_conn,
                        rpc_helper=rpc_helper,
                    )
                    white_token_derived_eth_dict = await get_token_derived_eth(
                        from_block=from_block, to_block=to_block, token_metadata=white_token_metadata,
                        redis_conn=redis_conn, rpc_helper=rpc_helper,
                    )

                    # Calculate token price in ETH
                    less_than_minimum_liquidity = False
                    for block_num in range(from_block, to_block + 1):
                        white_token_reserves = white_token_reserves_dict.get(block_num) * white_token_derived_eth_dict.get(block_num)

                        # Ignore if reserves are less than threshold
                        if white_token_reserves < 1:
                            less_than_minimum_liquidity = True
                            break

                        # Store ETH price in dictionary
                        token_eth_price_dict[block_num] = white_token_price_dict.get(block_num) * white_token_derived_eth_dict.get(block_num)

                    # If reserves are less than threshold, try next whitelist token pair
                    if less_than_minimum_liquidity:
                        token_eth_price_dict = {}
                        continue

                    break

            # Calculate final USD price
            if token_eth_price_dict:
                eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
                    from_block=from_block, to_block=to_block, redis_conn=redis_conn,
                    rpc_helper=rpc_helper,
                )
                for block_num in range(from_block, to_block + 1):
                    token_price_dict[block_num] = token_eth_price_dict.get(block_num, 0) * eth_usd_price_dict.get(block_num, 0)
            else:
                token_price_dict = {block_num: 0 for block_num in range(from_block, to_block + 1)}

            if debug_log:
                pricing_logger.debug(
                    f"{token_metadata['symbol']}: price is {token_price_dict}"
                    f' | its eth price is {token_eth_price_dict}',
                )

        # Cache calculated prices
        if token_price_dict:
            redis_cache_mapping = {
                json.dumps({'blockHeight': height, 'price': price}): int(height)
                for height, price in token_price_dict.items()
            }

            await redis_conn.zadd(
                name=uniswap_pair_cached_block_height_token_price.format(token_address),
                mapping=redis_cache_mapping,
            )

        return token_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating price of token:'
                f" {token_metadata['symbol']} | {token_metadata['address']}|"
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err
