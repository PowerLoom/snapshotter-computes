import asyncio
import decimal

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..utils.constants import pool_data_provider_contract_obj
from ..utils.core import get_asset_supply_and_debt
from ..utils.helpers import get_asset_metadata
from ..utils.models.data_models import data_provider_reserve_data


async def test_total_supply():
    # Mock your parameters
    asset_address = Web3.to_checksum_address(
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    )

    # from_block = 18774276 # liq call event
    # from_block = 18780748 # supply event
    from_block = 18780760  # withdraw event

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

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address, redis_conn=redis_conn, rpc_helper=rpc_helper,
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

    chain_data = [data_provider_reserve_data(*data) for data in chain_data]

    for i, block_num in enumerate(range(from_block, to_block + 1)):

        # Might need to use a more precise type than float for more accurate return data
        target = decimal.Decimal(str(chain_data[i].totalAToken / 10 ** int(asset_metadata['decimals'])))
        computed_supply = decimal.Decimal(asset_supply_debt_total[block_num]['total_supply']['token_supply'])

        # get decimal precision for chain data
        decimal_places = target.as_tuple().exponent

        # may be +/- 1 due to rounding
        assert abs(target - computed_supply) < (2e-1 ** -decimal_places), 'Results do not match chain data'

    print('PASSED')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply())
