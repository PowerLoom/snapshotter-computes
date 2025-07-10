import asyncio
import json
import math

from typing import Dict, List, Optional
from redis import asyncio as aioredis
from computes.utils.models.data_models import UniswapEvent
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from rpc_helper.rpc import get_contract_abi_dict
from rpc_helper.rpc import RpcHelper
from web3 import Web3
from computes.metadata import MetadataProcessor

from computes.redis_keys import uniswap_cached_block_height_token_eth_price
from computes.redis_keys import uniswap_tokens_pair_map
from computes.redis_keys import uniswap_v3_token_stable_pair_map
from computes.settings.config import settings as worker_settings
from computes.utils.constants import current_node
from computes.utils.constants import factory_contract_obj
from computes.utils.constants import pair_contract_abi
from computes.utils.constants import STABLE_TOKENS_LIST
from computes.utils.constants import TOKENS_DECIMALS
from computes.utils.constants import ZER0_ADDRESS
from computes.preloaders.eth_price.preloader import eth_price_preloader
from snapshotter.settings.config import settings
from computes.utils.models.message_models import UniswapPoolMetadata
from computes.redis_keys import uniswap_v3_best_pool_map

helper_logger = logger.bind(module='PowerLoom|Uniswap|Helpers')

SCORE_BLOCK_MULTIPLIER = 1_000_000


async def get_token_price_in_block_range(
    pair_metadata: UniswapPoolMetadata,
    from_block: int,
    to_block: int,
    rpc_helper: RpcHelper,
):
    """
    Fetch the price of token0 and token1 for a given Uniswap V3 pool over a specified block range.

    This function queries the Uniswap V3 pool contract's `slot0` function for each block in the range,
    extracts the sqrtPriceX96 value, and converts it to token prices using the provided token decimals.
    The prices are returned as dictionaries mapping block numbers to prices for both token0 and token1.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool, including token decimals and address.
        from_block (int): The starting block number (inclusive).
        to_block (int): The ending block number (inclusive).
        redis_conn: Redis connection object (not used in this function, but included for interface consistency).
        rpc_helper (RpcHelper): Helper object to perform batched RPC calls.

    Returns:
        Tuple[Dict[int, float], Dict[int, float]]:
            - token0_price: Mapping from block number to token0 price.
            - token1_price: Mapping from block number to token1 price.

    Example:
        token0_price, token1_price = await get_token_price_in_block_range(
            pair_metadata, 10000000, 10000010, redis_conn, rpc_helper
        )
    """
    # Perform a batched eth_call to fetch slot0 for each block in the range.
    response = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=get_contract_abi_dict(
            abi=pair_contract_abi,
        ),
        contract_address=pair_metadata.address,
        from_block=from_block,
        to_block=to_block,
        function_name='slot0',
        params=[],
    )

    # Log the slot0 responses for debugging and traceability.
    helper_logger.info(
        'Epoch {}-{} | Pool {} | Slot0 response: {}',
        from_block, to_block, pair_metadata.address, response
    )

    # Initialize dictionaries to store prices for each block.
    token0_price = {}
    token1_price = {}

    # Iterate over each block in the range and compute token prices.
    for i, block_num in enumerate(range(from_block, to_block + 1)):
        # Extract sqrtPriceX96 from the slot0 response for the current block.
        sqrtP = response[i][0]
        # Convert sqrtPriceX96 to token0 and token1 prices using the correct decimals.
        price0, price1 = eth_price_preloader.sqrtPriceX96ToTokenPrices(
            sqrtP,
            pair_metadata.token0.decimals,
            pair_metadata.token1.decimals,
        )
        # Store the computed prices in the result dictionaries.
        token0_price[block_num] = price0
        token1_price[block_num] = price1

    # Return the price mappings for token0 and token1.
    return token0_price, token1_price


async def get_token_price_in_usd_in_block_range(
    pair_metadata: UniswapPoolMetadata,
    from_block: int,
    to_block: int,
    redis_conn,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader,
    protocol_state_contract,
    rpc_helper: RpcHelper,
):
    """
    Fetch the price of token0 and token1 in USD for a given Uniswap V3 pool over a specified block range.

    This function first checks if the prices are already cached in Redis. If not, it computes the prices
    using the best available method depending on whether the tokens are WETH, USDC, or require a reference pool.
    The computed prices are then cached for future use.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool.
        from_block (int): The starting block number (inclusive).
        to_block (int): The ending block number (inclusive).
        redis_conn: Redis connection object.
        anchor_rpc_helper (RpcHelper): Helper for anchor chain RPC calls.
        ipfs_reader: IPFS reader object for metadata.
        protocol_state_contract: Protocol state contract object.
        rpc_helper (RpcHelper): Helper object to perform batched RPC calls.

    Returns:
        Tuple[
            Dict[int, float],  # token0_price_raw: token0 price in terms of token1 for each block
            Dict[int, float],  # token1_price_raw: token1 price in terms of token0 for each block
            Dict[int, float],  # token0_price: token0 price in USD for each block
            Dict[int, float],  # token1_price: token1 price in USD for each block
        ]
    """

    # Check if token prices are already present in Redis cache for the given block range.
    token0_price_cache = await redis_conn.zrangebyscore(
        name=uniswap_cached_block_height_token_eth_price.format(
            Web3.to_checksum_address(pair_metadata.token0.address),
        ),
        min=from_block,
        max=to_block,
    )
    token1_price_cache = await redis_conn.zrangebyscore(
        name=uniswap_cached_block_height_token_eth_price.format(
            Web3.to_checksum_address(pair_metadata.token1.address),
        ),
        min=from_block,
        max=to_block,
    )
    helper_logger.info(f"Token0 price cache: {token0_price_cache}, length: {len(token0_price_cache)}, from_block: {from_block}, to_block: {to_block}")
    helper_logger.info(f"Token1 price cache: {token1_price_cache}, length: {len(token1_price_cache)}, from_block: {from_block}, to_block: {to_block}")

    # If both token0 and token1 prices are fully cached for the block range, use the cached values.
    # Example cache entry: [b'{"blockHeight": 22888493, "price": 110868.32322378595}']
    if token0_price_cache and token1_price_cache and len(token0_price_cache) == len(token1_price_cache) == to_block - from_block + 1:
        token0_price_cache = [json.loads(data.decode('utf-8')) for data in token0_price_cache]
        token1_price_cache = [json.loads(data.decode('utf-8')) for data in token1_price_cache]
        token0_price = {
            int(data['blockHeight']): float(data['price']) for data in token0_price_cache
        }
        token1_price = {
            int(data['blockHeight']): float(data['price']) for data in token1_price_cache
        }
        helper_logger.info("Using cached token prices")
        return token0_price, token1_price

    # If not cached, fetch raw token prices (in terms of each other) for the block range.
    token0_price_raw, token1_price_raw = await get_token_price_in_block_range(
        pair_metadata=pair_metadata,
        from_block=from_block,
        to_block=to_block,
        rpc_helper=rpc_helper,
    )

    weth_address = worker_settings.contract_addresses.WETH
    usdc_address = worker_settings.contract_addresses.USDC

    # If either token0 or token1 is WETH, use ETH/USD price for conversion.
    if pair_metadata.token0.address == weth_address or pair_metadata.token1.address == weth_address:
        # Fetch ETH/USD price for the block range.
        eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        if pair_metadata.token0.address == weth_address:
            # token0 is WETH: its price is ETH/USD, token1 is relative to ETH.
            token0_price = eth_usd_price_dict
            token1_price = {
                block_num: eth_usd_price_dict[block_num] * token1_price_raw[block_num]
                for block_num in token1_price_raw
            }
        else:
            # token1 is WETH: its price is ETH/USD, token0 is relative to ETH.
            token0_price = {
                block_num: eth_usd_price_dict[block_num] * token0_price_raw[block_num]
                for block_num in token0_price_raw
            }
            token1_price = eth_usd_price_dict

    # If either token0 or token1 is USDC, use 1 USD as the price for USDC.
    elif pair_metadata.token0.address == usdc_address or pair_metadata.token1.address == usdc_address:
        if pair_metadata.token0.address == usdc_address:
            # token0 is USDC: price is 1 USD, token1 is relative to USDC.
            token0_price = {
                block_num: 1 for block_num in token0_price_raw
            }
            token1_price = token1_price_raw
        else:
            # token1 is USDC: price is 1 USD, token0 is relative to USDC.
            token0_price = token0_price_raw
            token1_price = {
                block_num: 1 for block_num in token1_price_raw
            }
    else:
        # For other tokens, identify the best pool (with WETH or USDC) to use as a price reference.
        best_pool_token_address = await identify_best_pool_to_calculate_price(
            pair_metadata=pair_metadata,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        # Generate pool metadata for the best reference pool.
        metadata_processor = MetadataProcessor()
        best_pool_metadata: Optional[UniswapPoolMetadata] = await metadata_processor.get_pool_metadata(
            pool_address=best_pool_token_address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )

        # Get the addresses of the tokens in the best pool.
        best_pool_tokens = [best_pool_metadata.token0.address, best_pool_metadata.token1.address]
        # Recursively fetch the USD prices for the tokens in the best pool.
        _, _, best_pool_token0_price_usd, best_pool_token1_price_usd = await get_token_price_in_usd_in_block_range(
            pair_metadata=best_pool_metadata,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            rpc_helper=rpc_helper,
        )

        # Determine which token in the best pool matches token0 or token1 of the original pair.
        if pair_metadata.token0.address in best_pool_tokens:
            # If token0 is in the best pool, use its USD price directly.
            if pair_metadata.token0.address == best_pool_metadata.token0.address:
                best_pool_token_price_usd = best_pool_token0_price_usd
            else:
                best_pool_token_price_usd = best_pool_token1_price_usd

            token0_price = best_pool_token_price_usd
            # token1 price is token0 price * token1/token0 price ratio.
            token1_price = {
                block_num: best_pool_token_price_usd[block_num] * token1_price_raw[block_num]
                for block_num in token1_price_raw
            }
        else:
            # If token1 is in the best pool, use its USD price directly.
            if pair_metadata.token1.address == best_pool_metadata.token0.address:
                best_pool_token_price_usd = best_pool_token0_price_usd
            else:
                best_pool_token_price_usd = best_pool_token1_price_usd

            # token0 price is token1 price * token0/token1 price ratio.
            token0_price = {
                block_num: best_pool_token_price_usd[block_num] * token0_price_raw[block_num]
                for block_num in token0_price_raw
            }
            token1_price = best_pool_token_price_usd

    # Cache the computed token prices at each block height in Redis for future use.
    await cache_token_price_at_height(
        token_address=pair_metadata.token0.address, token_price_dict=token0_price, redis_conn=redis_conn
    ),
    await cache_token_price_at_height(
        token_address=pair_metadata.token1.address, token_price_dict=token1_price, redis_conn=redis_conn
    ),

    return token0_price_raw, token1_price_raw, token0_price, token1_price


async def cache_token_price_at_height(
    token_address: str,
    token_price_dict: Dict[int, float],
    redis_conn: aioredis.Redis,
):
    """
    Cache the token price at each block height in Redis as a sorted set.

    Each entry is stored as a JSON string with the block height and price, and the block height is used as the score.

    Args:
        token_address (str): The address of the token.
        token_price_dict (Dict[int, float]): Mapping from block height to token price.
        redis_conn (aioredis.Redis): Redis connection object.
    """
    # Only proceed if there are prices to cache.
    if len(token_price_dict) > 0:
        max_block_height = max(token_price_dict.keys())

        # Prepare the mapping for Redis ZADD: {json_string: block_height}
        redis_cache_mapping = {
            json.dumps({'blockHeight': height, 'price': price}): int(height)
            for height, price in token_price_dict.items()
        }

        # Get the epoch size for the source chain to determine how much history to keep.
        source_chain_epoch_size = int(
            await redis_conn.get(source_chain_epoch_size_key()),
        )
        pipeline = redis_conn.pipeline()
        # Add the new prices to the sorted set.
        pipeline.zadd(
            name=uniswap_cached_block_height_token_eth_price.format(
                Web3.to_checksum_address(token_address),
            ),
            mapping=redis_cache_mapping,  # Use block height as score.
        )
        helper_logger.info(f"Zadd: {redis_cache_mapping}, token_address: {token_address}, max_block_height: {max_block_height}")
        # Remove old entries outside the retention window.
        pipeline.zremrangebyscore(
            name=uniswap_cached_block_height_token_eth_price.format(
                Web3.to_checksum_address(token_address),
            ),
            min=0,
            max=int(max_block_height) - source_chain_epoch_size * 4,
        )
        await pipeline.execute()


async def identify_best_liquidity_pool(
    token0: str,
    token1: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Get the best Uniswap V3 pair address for two tokens based on liquidity across different fee tiers.

    This function queries the Uniswap V3 factory for all possible pools between token0 and token1
    at common fee tiers, then fetches the liquidity for each pool, and returns the address of the pool
    with the highest liquidity.

    Args:
        token0 (str): The address of the first token.
        token1 (str): The address of the second token.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        Tuple[str, int]: The address of the best pair contract and its liquidity.
    """

    # Prepare tasks to get the pool address for each fee tier.
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
    # Fetch all pool addresses for the given token pair and fee tiers.
    pair_address_list = await asyncio.gather(*tasks)
    # Filter out zero addresses (non-existent pools).
    pair_address_list = [pair for pair in pair_address_list if pair != ZER0_ADDRESS]

    if len(pair_address_list) > 0:
        # For each valid pool, create a contract object.
        pair_contracts = [
            current_node['web3_client'].eth.contract(
                address=Web3.to_checksum_address(pair),
                abi=pair_contract_abi,
            ) for pair in pair_address_list
        ]

        # Prepare tasks to fetch the liquidity for each pool.
        tasks = [
            asyncio.create_task(
                rpc_helper.web3_call(
                    tasks=[('liquidity', [])],
                    contract_addr=pair_contract.address,
                    abi=pair_contract.abi,
                )
            ) for pair_contract in pair_contracts
        ]

        # Fetch all liquidity values.
        liquidity_list = await asyncio.gather(*tasks)
        # Pair each pool address with its liquidity.
        pair_liquidity_dict = zip(pair_address_list, liquidity_list)
        # Find the pool with the highest liquidity.
        best_pair, best_liquidity = max(pair_liquidity_dict, key=lambda x: x[1])
        return best_pair, best_liquidity[0]

    # If no valid pools found, return zero address and zero liquidity.
    return ZER0_ADDRESS, 0


async def identify_best_pool_to_calculate_price(
    pair_metadata: UniswapPoolMetadata,
    redis_conn,
    rpc_helper: RpcHelper,
):
    """
    Identify the best Uniswap V3 pool to use as a price reference for a given pair.

    This function checks if the best pool is already cached in Redis. If not, it tries all combinations
    of the pair's tokens with WETH and USDC, finds the pool with the highest liquidity, and caches the result.

    Args:
        pair_metadata (UniswapPoolMetadata): Metadata for the Uniswap V3 pool.
        redis_conn: Redis connection object.
        rpc_helper (RpcHelper): Helper for making RPC calls.

    Returns:
        str: The address of the best pool to use for price calculation.
    """

    # Check if the best pool address is already cached in Redis.
    best_pair_address = await redis_conn.hget(
        uniswap_v3_best_pool_map,
        pair_metadata.address,
    )
    if best_pair_address:
        return best_pair_address.decode('utf-8')

    # Prepare all token combinations with WETH and USDC.
    token_options = [worker_settings.contract_addresses.WETH, worker_settings.contract_addresses.USDC]
    all_token_options = (
        [(pair_metadata.token0.address, token_option) for token_option in token_options] +
        [(pair_metadata.token1.address, token_option) for token_option in token_options]
    )

    best_pair_address, best_liquidity = ZER0_ADDRESS, 0
    # Prepare tasks to identify the best liquidity pool for each token combination.
    tasks = [
        asyncio.create_task(
            identify_best_liquidity_pool(
                token0=token_option[0], token1=token_option[1], redis_conn=redis_conn, rpc_helper=rpc_helper)
        ) for token_option in all_token_options
    ]
    # Fetch all results.
    results = await asyncio.gather(*tasks)
    # Find the pool with the highest liquidity among all combinations.
    for pair_address, liquidity in results:
        helper_logger.info(f"Pair address: {pair_address}, liquidity: {liquidity}")
        if pair_address != ZER0_ADDRESS and liquidity > best_liquidity:
            best_pair_address = pair_address
            best_liquidity = liquidity

    # Cache the best pool address in Redis for future use.
    if best_pair_address != ZER0_ADDRESS:
        await redis_conn.hset(
            uniswap_v3_best_pool_map,
            mapping={
                pair_metadata.address: best_pair_address,
            },
        )

    return best_pair_address


# TODO: accept RPC helper as fallback?
async def get_events_from_cache(
    pool_address: str,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis
) -> Dict[int, List[UniswapEvent]]:
    """
    Fetch event logs from Redis cache for a given pool address and block range.
    
    Args:
        pool_address (str): The pool contract address
        from_block (int): Starting block number
        to_block (int): Ending block number
        redis_conn (aioredis.Redis): Redis connection
        
    Returns:
        Dict[int, List[UniswapEvent]]: Dictionary mapping block numbers to lists of event objects
        
    Example event JSON entry:
    {
        "eventName": "Swap",
        "filterName": "uniswapv3_pool_events",
        "txHash": "0x2f82087ed4d3bbf77c559d1337e3903a7108927cf9a2c9dae33bcce36f88933c",
        "blockNumber": 22394920,
        "txIndex": 4,
        "logIndex": 38,
        "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "topics": [
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
            "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
            "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"
        ],
        "data": "0x00000000000000000000000000000000000000000000000000000000203d7eb8fffffffffffffffffffffffffffffffffffffffffffffffffbe1f9518d38ad830000000000000000000000000000000000005b81d1e1ed548107534638248648000000000000000000000000000000000000000000000000516f1f5b22a9ea090000000000000000000000000000000000000000000000000000000000031219",
        "args": {
            "sender": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
            "recipient": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
            "amount0": 540901048,
            "amount1": -296681971772772989,
            "sqrtPriceX96": 1855984662392763862973509127145032,
            "liquidity": 5867943315771091465,
            "tick": 201241
        },
        "_score": 22394920000038
    }
    """
    # Calculate score range for zrangebyscore
    min_score = from_block * SCORE_BLOCK_MULTIPLIER
    max_score = (to_block + 1) * SCORE_BLOCK_MULTIPLIER - 1  # -1 to not include next block's events
    
    # Get events from Redis zset
    events = await redis_conn.zrangebyscore(
        name=f"events:{settings.namespace}:address:{pool_address}",
        min=min_score,
        max=max_score,
        withscores=True
    )

    helper_logger.info(f"Found {len(events)} events in raw cache for pool {pool_address} from block {from_block} to block {to_block}")

    if len(events) > 0:
        helper_logger.info(f"First event: {events[0]}")
        helper_logger.info(f"Last event: {events[-1]}")
    
    # Group events by block number
    block_events: Dict[int, List[UniswapEvent]] = {}
    for block in range(from_block, to_block + 1):
        block_events[block] = []

    for event_json, score in events:
        event_data = json.loads(event_json)
        event_data['_score'] = score  # Add score to event data
        
        try:
            event = UniswapEvent.model_validate(event_data)
        except Exception as e_parse:
            helper_logger.error(f"Failed to parse event data: {event_data}. Error: {e_parse}")
            continue
            
        block_number = event.blockNumber
        
        if block_number not in block_events:
            block_events[block_number] = []
            
        block_events[block_number].append(event)

    helper_logger.info(f"Found {len(block_events)} events in cache for pool {pool_address} from block {from_block} to block {to_block}")
    
    return block_events


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