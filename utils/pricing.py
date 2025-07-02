import json

from asyncio import gather
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from computes.preloaders.eth_price.preloader import eth_price_preloader
from computes.utils.helpers import get_token_eth_price_dict
from computes.redis_keys import uniswap_pair_cached_block_height_token_price
from computes.settings.config import settings as worker_settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key


pricing_logger = logger.bind(module="PowerLoom|Uniswap|Pricing")

async def get_token_price_in_block_range(
    token_metadata: dict,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    debug_log: bool = True,
) -> dict:
    """
    Retrieves the price of a token for a given block range.

    Args:
        token_metadata (dict): Metadata of the token including address and decimals.
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for blockchain interactions.
        debug_log (bool, optional): Flag to enable debug logging. Defaults to True.

    Returns:
        dict: A dictionary mapping block numbers to token prices in USD.

    Raises:
        Exception: If there's an error during price calculation.
    """
    try:
        token_price_dict = dict()
        token_address = Web3.to_checksum_address(token_metadata["address"])
        token_decimals = int(token_metadata["decimals"])

        # Check if cache exists for the given block range
        cached_price_entry_json_list = await redis_conn.zrangebyscore(
            name=uniswap_pair_cached_block_height_token_price_key(settings.namespace, token_address),
            min=int(from_block),
            max=int(to_block),
            withscores=False
        )
        
        if cached_price_entry_json_list and len(cached_price_entry_json_list) == to_block - (from_block - 1):
            price_entry_list = [json.loads(price_entry_json.decode("utf-8")) for price_entry_json in cached_price_entry_json_list]
            token_price_dict = {
                price_entry["blockHeight"]: price_entry["price"]
                for price_entry in price_entry_list
            }
            return token_price_dict

        # Handle WETH separately
        if token_address == Web3.to_checksum_address(worker_settings.contract_addresses.WETH):
            token_price_dict = await eth_price_preloader.get_eth_price_usd(
                from_block=from_block,
                to_block=to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        else:
            # Get token price in ETH
            token_eth_price_dict = await get_token_eth_price_dict(
                token_address=token_address,
                token_decimals=token_decimals,  
                from_block=from_block,
                to_block=to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )

            if token_eth_price_dict:
                # Get ETH price in USD
                eth_usd_price_dict = await eth_price_preloader.get_eth_price_usd(
                    from_block=from_block,
                    to_block=to_block,
                    redis_conn=redis_conn,
                    rpc_helper=rpc_helper,
                )

                if debug_log:
                    pricing_logger.debug(
                        f"token_eth_price_dict: {token_eth_price_dict}"
                    )
                    pricing_logger.debug(
                        f"eth_usd_price_dict: {eth_usd_price_dict}"
                    )

                # Calculate token price in USD
                for block_num in range(from_block, to_block + 1):
                    token_price_dict[block_num] = token_eth_price_dict.get(
                        block_num,
                        0,
                    ) * (eth_usd_price_dict.get(block_num, 0))
            else:
                # Set price to 0 if no ETH price is available
                token_price_dict = {
                    block_num: 0 for block_num in range(from_block, to_block + 1)
                }
        
        if debug_log:
            pricing_logger.debug(
                f"{token_metadata['symbol']}: price is {token_price_dict}"
                f" | its eth price is {token_eth_price_dict}",
            )

        # Cache the calculated prices
        if token_price_dict:
            redis_cache_mapping = {
                json.dumps({"blockHeight": height, "price": price}): int(
                    height,
                )
                for height, price in token_price_dict.items()
            }
            source_chain_epoch_size = await redis_conn.get(source_chain_epoch_size_key())
            if source_chain_epoch_size:
                source_chain_epoch_size = int(source_chain_epoch_size)
            else:
                pricing_logger.error(
                    f"source_chain_epoch_size is not set"
                )
                raise Exception("source_chain_epoch_size is not set")
            pipeline = redis_conn.pipeline()
            pipeline.zadd(
                name=uniswap_pair_cached_block_height_token_price_key(
                        settings.namespace, Web3.to_checksum_address(token_metadata["address"]),
                    ),
                mapping=redis_cache_mapping,
            )
            pipeline.zremrangebyscore(
                name=uniswap_pair_cached_block_height_token_price_key(
                    settings.namespace, Web3.to_checksum_address(token_metadata["address"]),
                ),
                min=0,
                max=int(from_block) - source_chain_epoch_size * 4,
            )
            await pipeline.execute()
        return token_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                "Error while calculating price of token:"
                f" {token_metadata['symbol']} | {token_metadata['address']}|"
                " err: {err}"
            ),
            err=lambda: str(err),
        )
        raise err
