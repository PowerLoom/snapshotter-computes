import asyncio

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper

from computes.settings.config import settings as worker_settings
from computes.utils.constants import aave_oracle_abi
from computes.utils.pricing import get_all_asset_prices


async def test_aave_oracle_bulk_pricing():
    # Mock your parameters
    from_block = 18995000
    to_block = from_block + 9
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    asset_price_dict = await get_all_asset_prices(
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        debug_log=False,
    )

    abi_dict = get_contract_abi_dict(
        abi=aave_oracle_abi,
    )

    for asset, price_dict in asset_price_dict.items():

        # get direct values for each asset
        asset_usd_quote = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetPrice',
            params=[asset],
            redis_conn=redis_conn,
        )

        asset_usd_quote = [(quote[0] * (10 ** -8)) for quote in asset_usd_quote]

        check = [
            price_dict[block_num] == asset_usd_quote[i]
            for i, block_num in enumerate(range(from_block, to_block + 1))
        ]

        assert not False in check, f"Prices don't match for {asset}"

        await asyncio.sleep(0.5)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_aave_oracle_bulk_pricing())
