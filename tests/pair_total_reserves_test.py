import asyncio
import pytest

from computes.utils.core import get_pair_reserves
from computes.utils.constants import initialize_rpc

def validate_test_environment(app_config):
    """Validate that the test environment is properly configured via app_config."""
    required_settings = [
        ('rpc.full_nodes', 
         "RPC endpoint not configured in .env.test (TEST_RPC_URL_FULL_NODE_1)"),
        ('anchor_chain_rpc.full_nodes', 
         "Anchor RPC endpoint not configured in .env.test (TEST_ANCHOR_RPC_URL_FULL_NODE_1)"),
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


@pytest.mark.asyncio(loop_scope="module")
async def test_pair_total_reserves(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test pair total reserves calculation."""
    validate_test_environment(app_config)
    await initialize_rpc(rpc_helper._rpc_settings)
    # Test with a historical block
    current_block_number = await rpc_helper.get_current_block_number()
    block_offset_from_head = 10
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    block_num = current_block_number - block_offset_from_head
    print(f"\nTesting at block near chain head: {block_num} (current head: {current_block_number})")
    
    pair_address = "0xE0554a476A092703abdB3Ef35c80e0D76d32939F"

    # Check if the block is available
    if not await validate_block_availability(rpc_helper, block_num):
        pytest.skip(f"Skipping test: block {block_num} not available on configured RPC node.")

    # Get pair reserves
    data = await get_pair_reserves(
        pair_address,
        block_num,
        block_num,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )
    
    # Validate the data structure
    assert data is not None, "Pair reserves data should not be None"
    assert block_num in data.token0Reserves, f"Block {block_num} should be present in reserves data"
    
    print(f"Pair reserves for {pair_address} at block {block_num}:")
    print(f"  Token0 USD: {data.token0ReservesUSD[block_num]}")
    print(f"  Token1 USD: {data.token1ReservesUSD[block_num]}")
    print(f"  Token0 reserves: {data.token0Reserves[block_num]}")
    print(f"  Token1 reserves: {data.token1Reserves[block_num]}")
    
    print("PASSED: test_pair_total_reserves")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
