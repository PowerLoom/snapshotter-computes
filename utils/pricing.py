import json
from asyncio import gather

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_cached_block_height_asset_price
from ..redis_keys import aave_cached_block_height_assets_prices
from ..redis_keys import aave_pool_asset_set_data
from ..settings.config import settings as worker_settings
from .constants import aave_oracle_abi
from .constants import pool_contract_obj

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
                f"{asset_metadata['symbol']}: usd price is {asset_price_dict}",
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


async def get_all_asset_prices(
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    debug_log=True,
):
    try:

        cached_price_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_assets_prices,
            min=int(from_block),
            max=int(to_block),
        )

        if cached_price_dict and len(cached_price_dict) == to_block - (from_block - 1):
            all_assets_price_dict = {
                json.loads(
                    data.decode(
                        'utf-8',
                    ),
                )['blockHeight']: json.loads(
                    data.decode('utf-8'),
                )['data']
                for data in cached_price_dict
            }

            return all_assets_price_dict

        # check if asset set cache exist
        asset_list_data_cache = await redis_conn.smembers(
            aave_pool_asset_set_data,
        )

        if asset_list_data_cache:
            asset_list = [asset.decode('utf-8') for asset in asset_list_data_cache]
        else:
            # get list of all assets from Pool contract
            [asset_list] = await rpc_helper.web3_call(
                tasks=[pool_contract_obj.functions.getReservesList()],
                redis_conn=redis_conn,
            )

            await redis_conn.sadd(
                aave_pool_asset_set_data, *asset_list,
            )

        abi_dict = get_contract_abi_dict(
            abi=aave_oracle_abi,
        )

        # get all asset prices in the block range from the Aave Oracle contract
        asset_prices_bulk = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetsPrices',
            params=[asset_list],
            redis_conn=redis_conn,
        )

        if debug_log:
            pricing_logger.debug(
                f'Retrieved bulk prices for aave assets: {asset_prices_bulk}',
            )

        all_assets_price_dict = {block_num: {} for block_num in range(from_block, to_block + 1)}

        # match each asset to its price for each block
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            matches = zip(asset_list, asset_prices_bulk[i][0])

            for match in matches:
                # all asset prices are returned with 8 decimal format
                all_assets_price_dict[block_num][match[0]] = match[1]

        source_chain_epoch_size = await redis_conn.get(source_chain_epoch_size_key())
        source_chain_epoch_size = int(source_chain_epoch_size)

        redis_data_cache_mapping = {
            json.dumps({'blockHeight': height, 'data': asset_prices}): int(
                height,
            )
            for height, asset_prices in all_assets_price_dict.items() if len(asset_prices) > 0
        }

        await gather(
            redis_conn.zadd(
                name=aave_cached_block_height_assets_prices,
                mapping=redis_data_cache_mapping,
            ),
            redis_conn.zremrangebyscore(
                name=aave_cached_block_height_assets_prices,
                min=0,
                max=from_block - source_chain_epoch_size * 3,
            ),
        )

        return all_assets_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating bulk asset prices:'
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err
