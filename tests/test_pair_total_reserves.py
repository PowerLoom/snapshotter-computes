import json
import time
from typing import Optional, List, Dict, Tuple
from web3 import Web3
import pytest

from computes.pair_total_reserves import PairTotalReservesProcessor
from computes.utils.models.message_models import UniswapPoolMetadata, UniswapBaseSnapshot
from snapshotter.utils.models.message_models import SnapshotProcessMessage


def load_pool_settings():
    """Load pool addresses from settings file."""
    try:
        with open("computes/settings/settings.json", 'r') as f:
            settings = json.load(f)
        return settings["contract_addresses"]
    except Exception as e:
        pytest.skip(f"Failed to load settings file: {e}")


async def get_active_pools_from_redis(redis_conn, block_number: int, namespace: str) -> List[str]:
    """Get active pools from Redis for a specific block (read-only)."""
    key = f"active_pools:{block_number}:{namespace}"
    pools = await redis_conn.smembers(key)
    return [pool.decode('utf-8') for pool in pools]


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
    metadata_processor = MetadataProcessor()

    validate_test_environment(app_config)
    
    # Load pool address from settings
    pool_settings = load_pool_settings()
    pool_address = Web3.to_checksum_address(pool_settings["USDC_WETH_PAIR"])

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


@pytest.mark.asyncio(loop_scope="module")
async def test_pair_total_reserves_processor(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    w3_instance,
    protocol_state_contract,
    load_abi_fn,
    app_config
):
    """Test the PairTotalReservesProcessor with active pools from Redis."""
    validate_test_environment(app_config)
    
    processor = PairTotalReservesProcessor()
    
    try:
        current_block_number = await rpc_helper.get_current_block_number()
    except Exception as e:
        pytest.fail(f"Failed to get current block number: {e}")

    block_offset_from_head = 10
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    from_block = current_block_number - block_offset_from_head
    to_block = from_block  # Single block test
    
    print(f"\nTesting PairTotalReservesProcessor at block: {from_block} (current head: {current_block_number})")

    if not await validate_block_availability(rpc_helper, from_block):
        pytest.skip(f"Skipping test: block {from_block} not available on configured RPC node.")

    # Get active pools from Redis for the test block
    active_pools = await get_active_pools_from_redis(redis_conn, from_block, app_config.namespace)
    
    if not active_pools:
        pytest.skip(f"No active pools found in Redis for block {from_block} and namespace {app_config.namespace}")
    
    print(f"Found {len(active_pools)} active pools in Redis for block {from_block}")
    for i, pool in enumerate(active_pools[:5]):  # Print first 5 pools
        print(f"  Pool {i+1}: {pool}")
    if len(active_pools) > 5:
        print(f"  ... and {len(active_pools) - 5} more pools")

    # Create epoch message
    epoch = SnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=from_block,
        timestamp=int(time.time())
    )

    # Run the processor
    print(f"\nRunning PairTotalReservesProcessor.compute()...")
    start_time = time.time()
    
    results = await processor.compute(
        epoch=epoch,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type="baseSnapshot:{poolAddress}:{Namespace}"
    )
    
    compute_time = time.time() - start_time
    print(f"Processor completed in {compute_time:.2f} seconds")

    # Validate results
    assert isinstance(results, list), "Results should be a list"
    print(f"Processor returned {len(results)} snapshots")
    
    if not results:
        print("ℹ️  No snapshots returned. This could mean:")
        print("    - No pools had sufficient data for processing")
        print("    - All pools were filtered out due to missing metadata")
        print("    - No events found for the test block")
        pytest.skip("No snapshots returned from processor")

    # Validate each snapshot
    for i, (task_key, snapshot) in enumerate(results):
        print(f"\n📊 Validating snapshot {i+1}/{len(results)}")
        
        # Validate task key format
        assert isinstance(task_key, str), f"Task key should be string, got {type(task_key)}"
        assert "baseSnapshot:" in task_key, f"Task key should contain 'baseSnapshot:', got: {task_key}"
        assert app_config.namespace in task_key, f"Task key should contain namespace '{app_config.namespace}', got: {task_key}"
        
        # Validate snapshot structure
        assert isinstance(snapshot, UniswapBaseSnapshot), f"Snapshot should be UniswapBaseSnapshot, got {type(snapshot)}"
        
        # Validate basic snapshot fields
        assert snapshot.address, "Snapshot should have a pool address"
        assert Web3.is_checksum_address(snapshot.address), f"Pool address should be checksum format: {snapshot.address}"
        assert snapshot.epoch.begin == from_block, f"Snapshot begin block should be {from_block}, got {snapshot.epoch.begin}"
        assert snapshot.epoch.end == to_block, f"Snapshot end block should be {to_block}, got {snapshot.epoch.end}"
        
        # Validate token addresses
        assert Web3.is_checksum_address(snapshot.token0), f"Token0 address should be checksum format: {snapshot.token0}"
        assert Web3.is_checksum_address(snapshot.token1), f"Token1 address should be checksum format: {snapshot.token1}"
        
        # Validate reserves data structure
        assert isinstance(snapshot.token0Reserves, dict), "token0Reserves should be a dict"
        assert isinstance(snapshot.token1Reserves, dict), "token1Reserves should be a dict"
        assert isinstance(snapshot.token0ReservesUSD, dict), "token0ReservesUSD should be a dict"
        assert isinstance(snapshot.token1ReservesUSD, dict), "token1ReservesUSD should be a dict"
        
        # Validate that reserves contain data for the test block
        assert from_block in snapshot.token0Reserves, f"token0Reserves should contain data for block {from_block}"
        assert from_block in snapshot.token1Reserves, f"token1Reserves should contain data for block {from_block}"
        
        # Validate reserves values are numeric and non-negative
        token0_reserve = snapshot.token0Reserves[from_block]
        token1_reserve = snapshot.token1Reserves[from_block]
        assert isinstance(token0_reserve, (int, float)), f"token0 reserve should be numeric, got {type(token0_reserve)}"
        assert isinstance(token1_reserve, (int, float)), f"token1 reserve should be numeric, got {type(token1_reserve)}"
        assert token0_reserve >= 0, f"token0 reserve should be non-negative, got {token0_reserve}"
        assert token1_reserve >= 0, f"token1 reserve should be non-negative, got {token1_reserve}"
        
        # Validate USD values
        token0_usd = snapshot.token0ReservesUSD[from_block]
        token1_usd = snapshot.token1ReservesUSD[from_block]
        assert isinstance(token0_usd, (int, float)), f"token0 USD value should be numeric, got {type(token0_usd)}"
        assert isinstance(token1_usd, (int, float)), f"token1 USD value should be numeric, got {type(token1_usd)}"
        assert token0_usd >= 0, f"token0 USD value should be non-negative, got {token0_usd}"
        assert token1_usd >= 0, f"token1 USD value should be non-negative, got {token1_usd}"
        
        # Validate timestamps
        assert isinstance(snapshot.timestamps, dict), "timestamps should be a dict"
        assert from_block in snapshot.timestamps, f"timestamps should contain data for block {from_block}"
        timestamp = snapshot.timestamps[from_block]
        if timestamp is not None:
            assert isinstance(timestamp, int), f"timestamp should be int or None, got {type(timestamp)}"
            assert timestamp > 0, f"timestamp should be positive if not None, got {timestamp}"
        
        # Validate trade data fields
        assert isinstance(snapshot.totalTrade, (int, float)), f"totalTrade should be numeric, got {type(snapshot.totalTrade)}"
        assert isinstance(snapshot.totalFee, (int, float)), f"totalFee should be numeric, got {type(snapshot.totalFee)}"
        assert snapshot.totalTrade >= 0, f"totalTrade should be non-negative, got {snapshot.totalTrade}"
        assert snapshot.totalFee >= 0, f"totalFee should be non-negative, got {snapshot.totalFee}"
        
        print(f"  ✅ Pool {snapshot.address}: Valid snapshot")
        print(f"     Token0 Reserve: {token0_reserve}")
        print(f"     Token1 Reserve: {token1_reserve}")
        print(f"     Token0 USD: ${token0_usd:.2f}")
        print(f"     Token1 USD: ${token1_usd:.2f}")
        print(f"     Total Trade: ${snapshot.totalTrade:.2f}")
        print(f"     Total Fee: ${snapshot.totalFee:.2f}")

    # Validate that we processed some of the active pools
    processed_pool_addresses = {snapshot.address for _, snapshot in results}
    active_pool_addresses = {Web3.to_checksum_address(pool) for pool in active_pools}
    
    # Check if processed pools are subset of active pools
    assert processed_pool_addresses.issubset(active_pool_addresses), \
        f"Processed pools should be subset of active pools. Extra pools: {processed_pool_addresses - active_pool_addresses}"
    
    coverage_ratio = len(processed_pool_addresses) / len(active_pool_addresses)
    print(f"\n📈 Processing Coverage: {len(processed_pool_addresses)}/{len(active_pool_addresses)} pools ({coverage_ratio:.1%})")
    
    if coverage_ratio < 1:
        print("⚠️  Low processing coverage. This could indicate:")
        print("    - Many pools lack sufficient metadata")
        print("    - Pools have no liquidity/events in this block")
        print("    - RPC or network issues during processing")

    print("PASSED: test_pair_total_reserves_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")