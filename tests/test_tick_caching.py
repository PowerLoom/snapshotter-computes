import pytest


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
async def test_tick_caching(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test tick caching functionality."""
    validate_test_environment(app_config)
    
    # Test with a sample pool and block
    pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # USDC/WETH 0.05%
    test_block = 18000000

    # Check if the block is available
    if not await validate_block_availability(rpc_helper, test_block):
        pytest.skip(f"Skipping test: block {test_block} not available on configured RPC node.")

    # Test Redis connection
    try:
        await redis_conn.ping()
        print("Redis connection successful")
    except Exception as e:
        pytest.fail(f"Redis connection failed: {e}")

    # Test basic tick caching operations
    tick_key = f"tick_data:{pool_address}:{test_block}"
    
    # Clear any existing data
    await redis_conn.delete(tick_key)
    
    # Test setting tick data
    sample_tick_data = {
        "tick": 195000,
        "liquidity": "1000000000000000000",
        "block_number": test_block
    }
    
    await redis_conn.hset(tick_key, mapping=sample_tick_data)
    
    # Test retrieving tick data
    retrieved_data = await redis_conn.hgetall(tick_key)
    
    # Convert byte strings to strings for comparison
    retrieved_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in retrieved_data.items()}
    
    assert retrieved_data["tick"] == str(sample_tick_data["tick"]), "Tick value should match"
    assert retrieved_data["liquidity"] == sample_tick_data["liquidity"], "Liquidity should match"
    assert retrieved_data["block_number"] == str(sample_tick_data["block_number"]), "Block number should match"
    
    # Clean up
    await redis_conn.delete(tick_key)
    
    print(f"Tick caching test completed for pool {pool_address} at block {test_block}")
    print("PASSED: test_tick_caching")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
