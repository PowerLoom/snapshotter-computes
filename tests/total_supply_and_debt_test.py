import asyncio

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..utils.constants import pool_data_provider_contract_obj
from ..utils.core import get_asset_supply_and_debt
from ..utils.models.data_models import DataProviderReserveData


async def test_total_supply_and_debt_calc():
    # Mock your parameters
    asset_address = Web3.to_checksum_address(
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    )

    from_block = 19017214  # USDC withdraw event
    # from_block = 19017220 # USDC borrow event
    # from_block = 19015050 # USDC repay event
    # from_block = 18774276 # liq call event
    # from_block = 18780760  # withdraw event

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

    asset_supply_debt_total = await get_asset_supply_and_debt(
        asset_address=asset_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        fetch_timestamp=True,  # get timestamps so events are computed
    )

    assert isinstance(asset_supply_debt_total, dict), 'Should return a dict'
    assert len(asset_supply_debt_total) == (to_block - from_block + 1), 'Should return data for all blocks'

    data_contract_abi_dict = get_contract_abi_dict(pool_data_provider_contract_obj.abi)

    # fetch actual data from chain
    chain_data = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=data_contract_abi_dict,
        function_name='getReserveData',
        contract_address=pool_data_provider_contract_obj.address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        params=[asset_address],
    )

    chain_data = [DataProviderReserveData(*data) for data in chain_data]

    for i, block_num in enumerate(range(from_block, to_block + 1)):

        # Might need to use a more precise type than float for more accurate return data
        target_supply = chain_data[i].totalAToken
        computed_supply = asset_supply_debt_total[block_num]['total_supply']['token_supply']

        target_variable_debt = chain_data[i].totalVariableDebt
        computed_variable_debt = asset_supply_debt_total[block_num]['total_variable_debt']['token_debt']

        target_stable_debt = chain_data[i].totalStableDebt
        computed_stable_debt = asset_supply_debt_total[block_num]['total_stable_debt']['token_debt']

        # # may be +/- 1 due to rounding
        assert abs(target_supply - computed_supply) <= 2, 'Supply results do not match chain data'
        assert abs(target_variable_debt - computed_variable_debt) <= 2, 'Variable debt results do not match chain data'
        assert abs(target_stable_debt - computed_stable_debt) <= 2, 'Stable debt results do not match chain data'

    print('PASSED')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_and_debt_calc())
