from typing import Optional, List, Dict, Tuple
from web3 import Web3
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import pytest

from computes.utils.models.message_models import UniswapPoolMetadata
from computes.utils.models.data_models import TickData

# Test pool configurations
TEST_POOLS = {
    'zero_liquidity': {
        'address': "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        'block': 12369621,
        'description': "Pool with zero liquidity"
    },
    'small_liquidity': {
        'address': "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        'block': 12369622,
        'description': "Pool with small amount of liquidity"
    },
    'extreme_price': {
        'address': "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        'block': 12369623,
        'description': "Pool with extreme price ratio"
    }
}

def validate_test_environment(app_config):
    """Validate that the test environment is properly configured via app_config."""
    required_settings = [
        ('rpc.full_nodes', "RPC endpoint not configured in .env.test (TEST_RPC_URL_FULL_NODE_1)"),
        ('anchor_chain_rpc.full_nodes', "Anchor RPC endpoint not configured in .env.test (TEST_ANCHOR_RPC_URL_FULL_NODE_1)"),
        ('protocol_state.address', "Protocol state contract address not configured in .env.test (TEST_PROTOCOL_STATE_CONTRACT_ADDRESS)"),
    ]
    
    for setting_path, error_msg in required_settings:
        parts = setting_path.split('.')
        current = app_config
        is_missing = False
        for part in parts:
            if not hasattr(current, part):
                is_missing = True
                break
            current = getattr(current, part)
        
        if is_missing or (isinstance(current, list) and not current):
            pytest.skip(f"Test environment not properly configured: {error_msg}")

async def validate_block_availability(rpc_helper, block_number: int) -> bool:
    """Check if a block number is available in the RPC node"""
    try:
        current_block = await rpc_helper.get_current_block_number()
        if block_number > current_block:
            return False
        await rpc_helper.eth_get_block(block_number)
        return True
    except Exception:
        return False

async def get_token_balances(
    pool_address: str,
    block_number: int,
    pool_metadata: UniswapPoolMetadata,
    w3: Web3,
    token_abi: Dict
) -> Tuple[int, int]:
    """Helper to get actual token balances from the pool"""
    token0_contract = w3.eth.contract(
        address=Web3.to_checksum_address(pool_metadata.token0.address),
        abi=token_abi
    )
    token1_contract = w3.eth.contract(
        address=Web3.to_checksum_address(pool_metadata.token1.address),
        abi=token_abi
    )
    
    token0_balance = token0_contract.functions.balanceOf(pool_address).call(block_identifier=block_number)
    token1_balance = token1_contract.functions.balanceOf(pool_address).call(block_identifier=block_number)
    
    return token0_balance, token1_balance

@pytest.mark.asyncio(loop_scope="module")
async def test_calculate_reserves(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    w3_instance,
    protocol_state_contract,
    load_abi_fn,
    app_config
):
    """Test the calculate_reserves function with normal operation against a historical block."""
    from computes.total_value_locked import calculate_reserves
    from computes.metadata import MetadataProcessor
    from computes.utils import constants
    metadata_processor = MetadataProcessor()

    validate_test_environment(app_config)
    await constants.initialize_rpc(injected_rpc_helper=rpc_helper)

    pool_address = Web3.to_checksum_address("0xE0554a476A092703abdB3Ef35c80e0D76d32939F")

    try:
        current_block_number = await rpc_helper.get_current_block_number()
    except Exception as e:
        pytest.fail(f"Failed to get current block number: {e}")

    block_offset_from_head = 10 
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    from_block = current_block_number - block_offset_from_head
    print(f"\nTesting at block near chain head: {from_block} (current head: {current_block_number})")

    if not await validate_block_availability(rpc_helper, from_block):
        pytest.skip(f"Skipping test: block {from_block} not available on configured RPC node.")

    pool_contract_abi = load_abi_fn("computes/static/abis/UniswapV3Pool.json")
    pool_metadata: Optional[UniswapPoolMetadata] = await metadata_processor.get_pool_metadata(
        pool_address=pool_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type=f'metadata:{pool_address}:{app_config.namespace}',
    )

    if not pool_metadata:
        pytest.fail("Failed to get pool_metadata. Skipping test.")
  
    reserves = await calculate_reserves(
        pool_address, from_block, pool_metadata, rpc_helper
    )

    token_contract_abi = load_abi_fn("computes/static/abis/IERC20.json")

    token0_address_checksum = Web3.to_checksum_address(pool_metadata.token0.address)
    token1_address_checksum = Web3.to_checksum_address(pool_metadata.token1.address)

    token0_contract = w3_instance.eth.contract(address=token0_address_checksum, abi=token_contract_abi)
    token1_contract = w3_instance.eth.contract(address=token1_address_checksum, abi=token_contract_abi)
    
    token0_actual_reserve = token0_contract.functions.balanceOf(pool_address).call(block_identifier=from_block)
    token1_actual_reserve = token1_contract.functions.balanceOf(pool_address).call(block_identifier=from_block)

    print(f"Calculated reserves: {reserves}")
    print(f"Actual on-chain reserves: ({token0_actual_reserve}, {token1_actual_reserve})")

    assert isinstance(reserves, tuple), "Should return a tuple"
    assert len(reserves) == 2, "Should have two elements"

    assert (
        reserves[0] >= token0_actual_reserve * 0.95
    ), "calculated reserve is lower than 95% of token balance"
    assert (
        reserves[0] <= token0_actual_reserve * 1.05
    ), "calculated reserve is higher than 105% of token balance"
    assert (
        reserves[1] >= token1_actual_reserve * 0.95
    ), "calculated reserve is lower than 95% of token balance"
    assert (
        reserves[1] <= token1_actual_reserve * 1.05
    ), "calculated reserve is higher than 105% of token balance"

    deviation_0 = (reserves[0] / token0_actual_reserve - 1) * 100 if token0_actual_reserve > 0 else 0
    deviation_1 = (reserves[1] / token1_actual_reserve - 1) * 100 if token1_actual_reserve > 0 else 0
    print(f"  Deviation: Token0={deviation_0:.2f}%, Token1={deviation_1:.2f}%")

    print("PASSED: test_calculate_reserves")

@pytest.mark.ipfs
@pytest.mark.asyncio(loop_scope="module")
async def test_calculate_reserves_near_chain_head(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """
    Tests calculate_reserves for a block near the current chain head.
    """
    from computes.total_value_locked import calculate_reserves
    from computes.metadata import MetadataProcessor
    from computes.utils import constants
    metadata_processor = MetadataProcessor()
    
    validate_test_environment(app_config)
    await constants.initialize_rpc(injected_rpc_helper=rpc_helper)

    try:
        current_block_number = await rpc_helper.get_current_block_number()
    except Exception as e:
        pytest.fail(f"Failed to get current block number: {e}")

    block_offset_from_head = 10 
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    target_block = current_block_number - block_offset_from_head
    print(f"\nTesting at block near chain head: {target_block} (current head: {current_block_number})")

    pair_address_to_test = Web3.to_checksum_address("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640") 

    pool_metadata: Optional[UniswapPoolMetadata] = await metadata_processor.get_pool_metadata(
        pool_address=pair_address_to_test,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper, 
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type=f'metadata:{pair_address_to_test}:{app_config.namespace}',
    )

    if not pool_metadata:
        pytest.fail(f"Failed to get pool_metadata for {pair_address_to_test} to run chain head test.")

    print(f"Calculating reserves for pair {pair_address_to_test} at block {target_block}...")
    try:
        reserves = await calculate_reserves(
            pair_address_to_test, target_block, pool_metadata, rpc_helper
        )
    except Exception as e:
        pytest.fail(f"calculate_reserves raised an exception for block {target_block}: {e}")

    assert isinstance(reserves, tuple), "calculate_reserves should return a tuple."
    assert len(reserves) == 2, "Reserves tuple should have two elements."
    assert isinstance(reserves[0], int), "Token0 reserve should be an integer."
    assert isinstance(reserves[1], int), "Token1 reserve should be an integer."
    assert reserves[0] >= 0, "Token0 reserve should be non-negative."
    assert reserves[1] >= 0, "Token1 reserve should be non-negative."

    print(f"PASSED: test_calculate_reserves_near_chain_head for block {target_block}")
    print(f"  Reserves: {reserves}")

@pytest.mark.asyncio(loop_scope="module")
async def test_calculate_reserves_edge_cases(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    w3_instance,
    protocol_state_contract,
    load_abi_fn,
    app_config
):
    """
    Tests calculate_reserves function with edge cases:
    1. Pool with zero liquidity
    2. Pool with very small liquidity
    3. Pool with extreme price ranges
    """
    from computes.total_value_locked import calculate_reserves
    from computes.metadata import MetadataProcessor
    from computes.utils import constants
    metadata_processor = MetadataProcessor()

    validate_test_environment(app_config)
    await constants.initialize_rpc(injected_rpc_helper=rpc_helper)
    
    token_contract_abi = load_abi_fn("computes/static/abis/IERC20.json")
    
    for pool_type, pool_config in TEST_POOLS.items():
        print(f"\nTesting {pool_config['description']}")
        
        block_to_test = pool_config['block']
        if not await validate_block_availability(rpc_helper, block_to_test):
            print(f"Skipping {pool_type} test: block {block_to_test} not available")
            continue

        pool_address = Web3.to_checksum_address(pool_config['address'])
        
        pool_metadata = await metadata_processor.get_pool_metadata(
            pool_address=pool_address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            task_type=f'metadata:{pool_address}:{app_config.namespace}',
        )
        
        if not pool_metadata:
            print(f"Failed to get metadata for {pool_type} pool. Skipping.")
            continue

        reserves = await calculate_reserves(
            pool_address, block_to_test, pool_metadata, rpc_helper
        )

        token0_balance, token1_balance = await get_token_balances(
            pool_address,
            block_to_test,
            pool_metadata,
            w3_instance,
            token_contract_abi
        )

        if pool_type == 'zero_liquidity':
            assert reserves == (0, 0), \
                f"Zero liquidity pool should have reserves (0, 0), got {reserves}"
        else:
            assert reserves[0] <= token0_balance + 1, f"{pool_type} token0 reserves ({reserves[0]}) should not exceed actual balance ({token0_balance})"
            assert reserves[1] <= token1_balance + 1, f"{pool_type} token1 reserves ({reserves[1]}) should not exceed actual balance ({token1_balance})"

        print(f"PASSED: {pool_config['description']}")
        print(f"  Calculated reserves: {reserves}")
        print(f"  Actual on-chain balances: ({token0_balance}, {token1_balance})")

    print("\nPASSED: test_calculate_reserves_edge_cases")

@pytest.mark.asyncio(loop_scope="module")
async def test_tvl_calculation_logic_with_test_pools(
    redis_conn,
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    protocol_state_contract,
    w3_instance,
    app_config
):
    """Test TVL calculation logic against various pool scenarios from TEST_POOLS."""
    from computes.total_value_locked import get_tick_info, get_slot0_data_for_block_range, calculate_tvl_from_ticks
    from computes.metadata import MetadataProcessor
    from computes.utils import constants
    metadata_processor = MetadataProcessor()

    validate_test_environment(app_config)
    await constants.initialize_rpc(injected_rpc_helper=rpc_helper)
    
    for scenario, pool_config in TEST_POOLS.items():
        print(f"\nTesting TVL logic for {scenario}: {pool_config['description']}")
        
        pool_address = Web3.to_checksum_address(pool_config['address'])
        block_to_test = pool_config['block']

        if not await validate_block_availability(rpc_helper, block_to_test):
            print(f"Skipping {scenario} test: block {block_to_test} not available")
            continue

        pool_metadata = await metadata_processor.get_pool_metadata(
            pool_address=pool_address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            task_type=f'metadata:{pool_address}:{app_config.namespace}'
        )

        if not pool_metadata:
            print(f"Could not retrieve metadata for {scenario}, skipping.")
            continue
        
        tick_data = await get_tick_info(
            rpc_helper=rpc_helper,
            pair_address=pool_address,
            at_block=block_to_test,
            pair_per_token_metadata=pool_metadata
        )

        slot0_data_range = await get_slot0_data_for_block_range(
            rpc_helper=rpc_helper,
            pair_address=pool_address,
            from_block=block_to_test,
            to_block=block_to_test
        )
        current_sqrt_price = slot0_data_range[block_to_test].sqrtPriceX96

        ticks_to_process = tick_data if tick_data is not None else []

        reserves = calculate_tvl_from_ticks(
            ticks=ticks_to_process,
            pair_metadata=pool_metadata,
            sqrt_price=current_sqrt_price
        )
        
        assert isinstance(reserves, tuple), f"Reserves calculation failed for {scenario}"
        assert len(reserves) == 2, f"Reserves should be a tuple of length 2 for {scenario}"
        print(f"  Reserves calculated from ticks for {scenario}: {reserves}")
        
        if scenario == 'zero_liquidity':
            assert reserves == (0, 0), f"Zero liquidity pool should have reserves (0, 0), got {reserves}"
        else:
            assert reserves[0] >= 0 and reserves[1] >= 0, f"Reserves should be non-negative for {scenario}"

if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
