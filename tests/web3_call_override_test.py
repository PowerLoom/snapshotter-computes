import pytest
import functools
from web3 import Web3

from computes.utils.constants import univ3_helper_bytecode, override_address, helper_contract
from computes.utils.constants import MAX_TICK, MIN_TICK
from computes.total_value_locked import transform_tick_bytes_to_list
from snapshotter.utils.rpc import RpcHelper

@pytest.mark.asyncio
async def test_web3_call_with_override():
    # Initialize RpcHelper
    rpc_helper = RpcHelper()
    await rpc_helper.init()

    # Create overrides dictionary
    overrides = {
        override_address: {'code': univ3_helper_bytecode},
    }

    pair_address = Web3.to_checksum_address('0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640')

    # Define a simple test function and its ABI
    step = (MAX_TICK - MIN_TICK) // 4

    tick_tasks = []
    for idx in range(MIN_TICK, MAX_TICK + 1, step):
        tick_tasks.append(
            ('getTicks', [pair_address, idx, min(idx + step - 1, MAX_TICK)]),
        )

    print(f"tick_tasks: {tick_tasks}")

    # Call the web3_call_with_override function
    result = await rpc_helper.web3_call_with_override(
        tasks=tick_tasks,
        contract_addr=helper_contract.address,
        abi=helper_contract.abi,
        overrides=overrides,
    )

    # Transform the decoded results
    ticks_list = []
    for decoded_result in result:
        ticks_list.append(transform_tick_bytes_to_list(decoded_result))

    ticks_list = functools.reduce(lambda x, y: x + y, ticks_list)

    # Add your assertions here
    assert len(ticks_list) > 0
    assert 'liquidity_net' in ticks_list[0]
    assert 'idx' in ticks_list[0]
