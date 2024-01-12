import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_cached_block_height_asset_price
from ..settings.config import settings as worker_settings
from .constants import aave_oracle_abi

pricing_logger = logger.bind(module='PowerLoom|Aave|Pricing')


async def get_asset_price_in_block_range(
    asset_metadata,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    debug_log=True,
):
    """
    returns the price of a token at a given block range
    """
    try:
        asset_price_dict = dict()
        asset_address = Web3.to_checksum_address(asset_metadata['address'])
        # check if cahce exist for given epoch
        cached_price_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_asset_price.format(
                asset_address,
            ),
            min=int(from_block),
            max=int(to_block),
        )

        if cached_price_dict and len(cached_price_dict) == to_block - (from_block - 1):
            price_dict = {
                json.loads(
                    price.decode(
                        'utf-8',
                    ),
                )['blockHeight']: json.loads(
                    price.decode('utf-8'),
                )['price']
                for price in cached_price_dict
            }

            return price_dict

        abi_dict = get_contract_abi_dict(
            abi=aave_oracle_abi,
        )

        asset_usd_quote = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetPrice',
            params=[asset_address],
            redis_conn=redis_conn,
        )

        # all asset prices are returned in 8 decimal format
        asset_usd_quote = [(quote[0] * (10 ** -8)) for quote in asset_usd_quote]
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            asset_price_dict[block_num] = asset_usd_quote[i]

        if debug_log:
            pricing_logger.debug(
                f"{asset_metadata['symbol']}: usd price is {asset_price_dict}"
            )

        # cache price at height
        if len(asset_price_dict) > 0:
            redis_cache_mapping = {
                json.dumps({'blockHeight': height, 'price': price}): int(
                    height,
                )
                for height, price in asset_price_dict.items()
            }

            await redis_conn.zadd(
                name=aave_cached_block_height_asset_price.format(
                    Web3.to_checksum_address(asset_metadata['address']),
                ),
                mapping=redis_cache_mapping,  # timestamp so zset do not ignore same height on multiple heights
            )

        return asset_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating price of asset:'
                f" {asset_metadata['symbol']} | {asset_metadata['address']}|"
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err