import asyncio

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..utils.constants import pool_data_provider_contract_obj
from ..utils.core import get_asset_supply_and_debt_bulk
from ..utils.helpers import get_bulk_asset_data
from ..utils.models.data_models import data_provider_reserve_data


async def test_total_supply_and_debt_calc():
    # Mock your parameters
    # 18 Decimal tokens are slightly off due to compound interest precision, working to
    # have them match exact chain results as well.
    asset_address = Web3.to_checksum_address(
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    )

    from_block = 19033030

    to_block = from_block + 9
    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()

    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool

    # set key for get_block_details_in_block_range
    await redis_conn.set(
        source_chain_epoch_size_key(),
        to_block - from_block,
    )

    # simulate preloader call
    await get_bulk_asset_data(
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
    )

    asset_supply_debt_total = await get_asset_supply_and_debt_bulk(
        asset_address=asset_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        fetch_timestamp=True,
    )

    data_contract_abi_dict = get_contract_abi_dict(pool_data_provider_contract_obj.abi)

    # fetch actual data from chain for comparison
    chain_data = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=data_contract_abi_dict,
        function_name='getReserveData',
        contract_address=pool_data_provider_contract_obj.address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        params=[asset_address],
    )

    chain_data = [data_provider_reserve_data(*data) for data in chain_data]

    for i, block_num in enumerate(range(from_block, to_block + 1)):

        # Only debt is computed manually for the bulk version
        target_variable_debt = chain_data[i].totalVariableDebt
        computed_variable_debt = asset_supply_debt_total[block_num]['total_variable_debt']['token_debt']

        target_stable_debt = chain_data[i].totalStableDebt
        computed_stable_debt = asset_supply_debt_total[block_num]['total_stable_debt']['token_debt']

        # # may be +/- 1 due to rounding
        assert abs(target_variable_debt - computed_variable_debt) <= 2, 'Variable debt results do not match chain data'
        assert abs(target_stable_debt - computed_stable_debt) <= 2, 'Stable debt results do not match chain data'

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_and_debt_calc())
