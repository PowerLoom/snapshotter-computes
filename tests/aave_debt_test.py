import asyncio
import decimal

from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..utils.constants import pool_data_provider_contract_obj, seconds_in_year, ray, HALF_RAY, AAVE_CORE_EVENTS, debt_token_contract_obj
from ..utils.helpers import get_asset_metadata
from ..utils.helpers import get_supply_events
from ..utils.models.data_models import data_provider_reserve_data

from pprint import pprint
from decimal import Decimal
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

# c := div(add(mul(a, b), HALF_RAY), RAY)

# c := div(y, RAY)

def rayMul(a: int, b: int):
    x = Decimal(a) * Decimal(b)
    y = x + Decimal(HALF_RAY)
    z = y / Decimal(ray)
    return int(z)

def rayDiv(a: int, b: int):
    x = Decimal(b) / 2
    y = Decimal(a) * Decimal(ray)
    z = (x + y) / b
    return int(z)

def calculate_normalized_debt(interest: int, index: int) -> int:
    return rayMul(interest, index)

# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/MathUtils.sol line 50
def calculate_compound_interest(rate: int, current_timestamp: int, last_update_timestamp: int) -> int:
    exp = current_timestamp - last_update_timestamp

    if exp == 0:
        print("No change")
        return int(ray)

    expMinusOne = exp - 1
    expMinusTwo = max(0, exp - 2)

    basePowerTwo = int(rayMul(rate, rate) / (seconds_in_year * seconds_in_year))
    basePowerThree = int(rayMul(basePowerTwo, rate) / seconds_in_year)

    secondTerm = (exp * expMinusOne * basePowerTwo) / 2
    thirdTerm = (exp * expMinusOne * expMinusTwo * basePowerThree) / 6

    interest = Decimal(ray) + Decimal(rate * exp) / Decimal(seconds_in_year) + Decimal(secondTerm + thirdTerm)

    return interest

async def test_total_supply():
    # Mock your parameters
    asset_address = Web3.to_checksum_address(
        '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
    )

    from_block = 52077802 # borrow event
    
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

    block_details_dict = await get_block_details_in_block_range(
        from_block,
        to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    data_contract_abi_dict = get_contract_abi_dict(pool_data_provider_contract_obj.abi)

    # fetch actual data from chain
    chain_data = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=data_contract_abi_dict,
        function_name='getReserveData',
        contract_address=pool_data_provider_contract_obj.address,
        from_block=from_block + 1,
        to_block=to_block,
        redis_conn=redis_conn,
        params=[asset_address],
    )

    chain_data = [data_provider_reserve_data(*data) for data in chain_data]

    initial_data = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=data_contract_abi_dict,
        function_name='getReserveData',
        contract_address=pool_data_provider_contract_obj.address,
        from_block=from_block,
        to_block=from_block,
        redis_conn=redis_conn,
        params=[asset_address],
    )

    events = await get_supply_events(
            rpc_helper=rpc_helper,
            from_block=from_block + 1,
            to_block=to_block,
            redis_conn=redis_conn,
    )

    core_events = list(
        filter(
            lambda x:
            x['args']['reserve'] == asset_address and
            x['event'] in AAVE_CORE_EVENTS,
            events,
        ),
    )

    pprint(core_events)

    data_events = filter(
        lambda x:
        x['args']['reserve'] == asset_address and
        x['event'] == 'ReserveDataUpdated',
        events,
    )

    data_events = {event['blockNumber']: {event['transactionIndex']: event} for event in data_events}

    first_data = data_provider_reserve_data(*initial_data[0])

    pprint(first_data)

    first_block = block_details_dict.get(from_block, None)
    timestamp = first_block['timestamp']

    variable_debt = first_data.totalVariableDebt
    variable_rate = first_data.variableBorrowRate
    variable_index = first_data.variableBorrowIndex
    last_update = first_data.lastUpdateTimestamp
    variable_interest = calculate_compound_interest(variable_rate, timestamp, last_update)
    normalized_variable_debt = calculate_normalized_debt(variable_interest, variable_index)
    scaled_variable_debt = rayDiv(variable_debt, normalized_variable_debt)

    stable_debt = first_data.totalStableDebt
    last_supply_update = first_data.lastUpdateTimestamp
    average_stable_rate = first_data.averageStableBorrowRate
    stable_interest = calculate_compound_interest(average_stable_rate, timestamp, last_supply_update)
    scaled_stable_debt = rayDiv(stable_debt, stable_interest)

    for i, block_num in enumerate(range(from_block + 1, to_block + 1)):
        block = block_details_dict.get(block_num, None)
        block_events = filter(lambda x: x.get('blockNumber') == block_num, core_events)
        timestamp = block['timestamp']

        variable_debt_adj = 0
        stable_debt_adj = 0
        update_scaled_v_flag = False
        update_scaled_s_flag = False

        variable_interest = calculate_compound_interest(variable_rate, timestamp, last_update)
        normalized_variable_debt = calculate_normalized_debt(variable_interest, variable_index)
        variable_debt = rayMul(scaled_variable_debt, normalized_variable_debt)

        stable_interest = calculate_compound_interest(average_stable_rate, timestamp, last_supply_update)
        stable_debt = rayMul(stable_interest, scaled_stable_debt)

        for event in block_events:

            pprint(event)

            if event['event'] == 'Borrow':
                if event['args']['interestRateMode'] == 1:
                    stable_debt_adj += event['args']['amount']
                    update_scaled_s_flag = True
                    
                else:
                    variable_debt_adj += event['args']['amount']
                    update_scaled_v_flag = True

            # repay event does not indicate whether stable or variable debt is repaid. Need to parse burn/mint for the stable and variable tokens instead.   
            elif event['event'] == 'Repay':
                # if event['args']['interestRateMode'] == 1:
                #     stable_debt_adj -= event['args']['amount']
                #     update_scaled_s_flag = True
                    
                # else:
                # variable_debt_adj -= event['args']['amount']
                # update_scaled_v_flag = True
                pass

            data_update = data_events[block_num][event['transactionIndex']]
            variable_rate = data_update['args']['variableBorrowRate']
            variable_index = data_update['args']['variableBorrowIndex']
            last_update = timestamp

        variable_debt += variable_debt_adj
        stable_debt += stable_debt_adj

        if update_scaled_v_flag:
            scaled_variable_debt = rayDiv(variable_debt, normalized_variable_debt)
        if update_scaled_s_flag:
            scaled_stable_debt = rayDiv(stable_debt, stable_interest)

        # print(debt_adjustment)
        print("VARIABLE:")
        print(variable_debt)
        print(chain_data[i].totalVariableDebt)
        print("= = =")
        print("STABLE")
        print(stable_debt)
        print(chain_data[i].totalStableDebt)
        print("------------------")

    print('PASSED')



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply())
