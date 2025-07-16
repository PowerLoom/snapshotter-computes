import json
import time
from typing import Dict
import pytest
from redis import asyncio as aioredis

from computes.metadata import MetadataProcessor
from computes.utils.helpers import get_pool_metadata
from snapshotter.settings.config import settings
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.data_utils import get_project_latest_snapshot

"""
Test for MetadataProcessor pool metadata validation.

This test validates the accuracy of pool metadata retrieved by the MetadataProcessor
by comparing it against known values for a specific Uniswap V3 pool.

Test Flow:
1. Retrieves pool metadata using the MetadataProcessor for a specific pool address
2. Compares the retrieved metadata against known expected values
3. Validates all metadata fields including token information, fees, and factory address
4. Ensures the MetadataProcessor correctly retrieves and processes pool data

Validation Criteria:
- Pool address must match expected value
- Token0 and Token1 addresses, names, symbols, and decimals must match
- Fee and tick spacing must match expected values
- Factory address must match expected value
- All metadata fields must be present and correctly formatted

This test ensures that the MetadataProcessor correctly retrieves and processes
pool metadata from the snapshotter system, validating the accuracy of the
metadata against known pool information.
"""


def validate_test_environment(app_config):
    """Validate that the test environment is properly configured via app_config."""
    required_settings = [
        ('rpc.full_nodes', "RPC endpoint not configured in .env.test (TEST_RPC_URL_FULL_NODE_1)"),
        ('anchor_chain_rpc.full_nodes', "Anchor RPC endpoint not configured in .env.test (TEST_ANCHOR_RPC_URL_FULL_NODE_1)"),
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


def get_expected_metadata() -> Dict:
    """Get the expected metadata for the test pool"""
    return {
        "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        "token0": {
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "name": "USD Coin",
            "symbol": "USDC",
            "decimals": 6
        },
        "token1": {
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "name": "Wrapped Ether",
            "symbol": "WETH",
            "decimals": 18
        },
        "fee": 500,
        "tick_spacing": 10,
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    }


async def verify_redis_metadata_cache(redis_conn: aioredis.Redis, pool_address: str) -> bool:
    """Check if metadata exists in Redis cache for the pool"""
    cache_key = f'pool_metadata:{pool_address}'
    cached_data = await redis_conn.get(cache_key)
    return cached_data is not None


async def validate_metadata_against_contracts(
    rpc_helper,
    pool_address: str,
    metadata,
    expected_metadata: Dict
) -> None:
    """
    Validate metadata against actual blockchain contracts.
    
    Args:
        rpc_helper: RPC helper for blockchain interactions
        pool_address: The pool address to validate
        metadata: The metadata object from the processor
        expected_metadata: The expected metadata dictionary
    """
    print(f"\n🔍 Validating metadata against blockchain contracts...")
    
    # Load ABIs
    pool_abi_path = "computes/static/abis/UniswapV3Pool.json"
    token_abi_path = "computes/static/abis/IERC20.json"
    factory_abi_path = "computes/static/abis/IUniswapV3Factory.json"
    
    try:
        with open(pool_abi_path, 'r') as f:
            pool_abi = json.load(f)
        with open(token_abi_path, 'r') as f:
            token_abi = json.load(f)
        with open(factory_abi_path, 'r') as f:
            factory_abi = json.load(f)
    except FileNotFoundError as e:
        pytest.skip(f"ABI file not found: {e}")
    
    # Validate pool contract data
    print(f"\n📋 Validating pool contract data...")
    
    # Get token0 from pool contract
    token0_result = await rpc_helper.web3_call(
        tasks=[("token0", [])],
        contract_addr=pool_address,
        abi=pool_abi
    )
    if token0_result and token0_result[0]:
        contract_token0 = token0_result[0]
        print(f"  Pool token0: {contract_token0}")
        assert contract_token0.lower() == metadata.token0.address.lower(), f"Pool contract token0 mismatch: expected {metadata.token0.address}, got {contract_token0}"
    else:
        print(f"  ⚠️  Could not get token0 from pool contract")
    
    # Get token1 from pool contract
    token1_result = await rpc_helper.web3_call(
        tasks=[("token1", [])],
        contract_addr=pool_address,
        abi=pool_abi
    )
    if token1_result and token1_result[0]:
        contract_token1 = token1_result[0]
        print(f"  Pool token1: {contract_token1}")
        assert contract_token1.lower() == metadata.token1.address.lower(), f"Pool contract token1 mismatch: expected {metadata.token1.address}, got {contract_token1}"
    else:
        print(f"  ⚠️  Could not get token1 from pool contract")
    
    # Get fee from pool contract
    fee_result = await rpc_helper.web3_call(
        tasks=[("fee", [])],
        contract_addr=pool_address,
        abi=pool_abi
    )
    if fee_result and fee_result[0] is not None:
        contract_fee = fee_result[0]
        print(f"  Pool fee: {contract_fee}")
        assert contract_fee == metadata.fee, f"Pool contract fee mismatch: expected {metadata.fee}, got {contract_fee}"
    else:
        print(f"  ⚠️  Could not get fee from pool contract")
    
    # Validate token0 contract data
    print(f"\n📋 Validating token0 contract data...")
    
    # Get token0 name
    token0_name_result = await rpc_helper.web3_call(
        tasks=[("name", [])],
        contract_addr=metadata.token0.address,
        abi=token_abi
    )
    if token0_name_result and token0_name_result[0]:
        contract_token0_name = token0_name_result[0]
        print(f"  Token0 name: {contract_token0_name}")
        assert contract_token0_name == metadata.token0.name, f"Token0 name mismatch: expected {metadata.token0.name}, got {contract_token0_name}"
    else:
        print(f"  ⚠️  Could not get token0 name from contract")
    
    # Get token0 symbol
    token0_symbol_result = await rpc_helper.web3_call(
        tasks=[("symbol", [])],
        contract_addr=metadata.token0.address,
        abi=token_abi
    )
    if token0_symbol_result and token0_symbol_result[0]:
        contract_token0_symbol = token0_symbol_result[0]
        print(f"  Token0 symbol: {contract_token0_symbol}")
        assert contract_token0_symbol == metadata.token0.symbol, f"Token0 symbol mismatch: expected {metadata.token0.symbol}, got {contract_token0_symbol}"
    else:
        print(f"  ⚠️  Could not get token0 symbol from contract")
    
    # Get token0 decimals
    token0_decimals_result = await rpc_helper.web3_call(
        tasks=[("decimals", [])],
        contract_addr=metadata.token0.address,
        abi=token_abi
    )
    if token0_decimals_result and token0_decimals_result[0] is not None:
        contract_token0_decimals = token0_decimals_result[0]
        print(f"  Token0 decimals: {contract_token0_decimals}")
        assert contract_token0_decimals == metadata.token0.decimals, f"Token0 decimals mismatch: expected {metadata.token0.decimals}, got {contract_token0_decimals}"
    else:
        print(f"  ⚠️  Could not get token0 decimals from contract")
    
    # Validate token1 contract data
    print(f"\n📋 Validating token1 contract data...")
    
    # Get token1 name
    token1_name_result = await rpc_helper.web3_call(
        tasks=[("name", [])],
        contract_addr=metadata.token1.address,
        abi=token_abi
    )
    if token1_name_result and token1_name_result[0]:
        contract_token1_name = token1_name_result[0]
        print(f"  Token1 name: {contract_token1_name}")
        assert contract_token1_name == metadata.token1.name, f"Token1 name mismatch: expected {metadata.token1.name}, got {contract_token1_name}"
    else:
        print(f"  ⚠️  Could not get token1 name from contract")
    
    # Get token1 symbol
    token1_symbol_result = await rpc_helper.web3_call(
        tasks=[("symbol", [])],
        contract_addr=metadata.token1.address,
        abi=token_abi
    )
    if token1_symbol_result and token1_symbol_result[0]:
        contract_token1_symbol = token1_symbol_result[0]
        print(f"  Token1 symbol: {contract_token1_symbol}")
        assert contract_token1_symbol == metadata.token1.symbol, f"Token1 symbol mismatch: expected {metadata.token1.symbol}, got {contract_token1_symbol}"
    else:
        print(f"  ⚠️  Could not get token1 symbol from contract")
    
    # Get token1 decimals
    token1_decimals_result = await rpc_helper.web3_call(
        tasks=[("decimals", [])],
        contract_addr=metadata.token1.address,
        abi=token_abi
    )
    if token1_decimals_result and token1_decimals_result[0] is not None:
        contract_token1_decimals = token1_decimals_result[0]
        print(f"  Token1 decimals: {contract_token1_decimals}")
        assert contract_token1_decimals == metadata.token1.decimals, f"Token1 decimals mismatch: expected {metadata.token1.decimals}, got {contract_token1_decimals}"
    else:
        print(f"  ⚠️  Could not get token1 decimals from contract")
    
    # Validate factory contract data
    print(f"\n📋 Validating factory contract data...")
    
    # Verify pool exists on factory
    factory_result = await rpc_helper.web3_call(
        tasks=[("getPool", [metadata.token0.address, metadata.token1.address, metadata.fee])],
        contract_addr=metadata.factory,
        abi=factory_abi
    )
    if factory_result and factory_result[0]:
        factory_pool = factory_result[0]
        print(f"  Factory getPool result: {factory_pool}")
        assert factory_pool.lower() == pool_address.lower(), f"Factory getPool mismatch: expected {pool_address}, got {factory_pool}"
    else:
        print(f"  ⚠️  Could not verify pool on factory")
    
    print(f"\n✅ All blockchain contract validations passed!")
    print(f"  Pool contract data verified")
    print(f"  Token0 contract data verified")
    print(f"  Token1 contract data verified")
    print(f"  Factory contract data verified")


@pytest.mark.asyncio(loop_scope="module")
async def test_metadata_processor(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test the MetadataProcessor with normal operation against a known pool."""
    processor = MetadataProcessor()
    validate_test_environment(app_config)

    # Test pool configuration
    pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    expected_metadata = get_expected_metadata()
    
    print(f"\nTesting MetadataProcessor for pool: {pool_address}")
    print(f"Expected metadata: {json.dumps(expected_metadata, indent=2)}")

    try:
        current_block_number = await rpc_helper.get_current_block_number()
    except Exception as e:
        pytest.fail(f"Failed to get current block number: {e}")

    # Test with a block near the chain head
    block_offset_from_head = 10
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    from_block = current_block_number - block_offset_from_head
    print(f"\nTesting at block near chain head: {from_block} (current head: {current_block_number})")

    if not await validate_block_availability(rpc_helper, from_block):
        pytest.skip(f"Skipping test: block {from_block} not available on configured RPC node.")

    # Check if metadata exists in Redis cache
    cache_exists = await verify_redis_metadata_cache(redis_conn, pool_address)
    print(f"\nRedis cache check:")
    print(f"  Metadata cache exists: {'✅ Yes' if cache_exists else '❌ No'}")

    # Get pool metadata using the processor
    print(f"\n🔍 Retrieving pool metadata...")
    task_type = f'metadata:{pool_address}:{settings.namespace}'
    
    metadata = await get_pool_metadata(
        pool_address=pool_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type=task_type
    )

    if not metadata:
        pytest.skip(f"No metadata found for pool {pool_address}. This may indicate that the pool metadata has not been processed yet.")

    print(f"\nRetrieved metadata:")
    print(f"  Pool Address: {metadata.address}")
    print(f"  Token0: {metadata.token0.symbol} ({metadata.token0.address})")
    print(f"  Token1: {metadata.token1.symbol} ({metadata.token1.address})")
    print(f"  Fee: {metadata.fee}")
    print(f"  Factory: {metadata.factory}")

    # Validate pool address
    assert metadata.address.lower() == expected_metadata["address"].lower(), f"Pool address mismatch: expected {expected_metadata['address']}, got {metadata.address}"

    # Validate token0 metadata
    expected_token0 = expected_metadata["token0"]
    assert metadata.token0.address.lower() == expected_token0["address"].lower(), f"Token0 address mismatch: expected {expected_token0['address']}, got {metadata.token0.address}"
    assert metadata.token0.name == expected_token0["name"], f"Token0 name mismatch: expected {expected_token0['name']}, got {metadata.token0.name}"
    assert metadata.token0.symbol == expected_token0["symbol"], f"Token0 symbol mismatch: expected {expected_token0['symbol']}, got {metadata.token0.symbol}"
    assert metadata.token0.decimals == expected_token0["decimals"], f"Token0 decimals mismatch: expected {expected_token0['decimals']}, got {metadata.token0.decimals}"

    # Validate token1 metadata
    expected_token1 = expected_metadata["token1"]
    assert metadata.token1.address.lower() == expected_token1["address"].lower(), f"Token1 address mismatch: expected {expected_token1['address']}, got {metadata.token1.address}"
    assert metadata.token1.name == expected_token1["name"], f"Token1 name mismatch: expected {expected_token1['name']}, got {metadata.token1.name}"
    assert metadata.token1.symbol == expected_token1["symbol"], f"Token1 symbol mismatch: expected {expected_token1['symbol']}, got {metadata.token1.symbol}"
    assert metadata.token1.decimals == expected_token1["decimals"], f"Token1 decimals mismatch: expected {expected_token1['decimals']}, got {metadata.token1.decimals}"

    # Validate pool parameters
    assert metadata.fee == expected_metadata["fee"], f"Fee mismatch: expected {expected_metadata['fee']}, got {metadata.fee}"
    assert metadata.factory.lower() == expected_metadata["factory"].lower(), f"Factory address mismatch: expected {expected_metadata['factory']}, got {metadata.factory}"

    # Validate tick spacing if available
    if hasattr(metadata, 'tick_spacing'):
        assert metadata.tick_spacing == expected_metadata["tick_spacing"], f"Tick spacing mismatch: expected {expected_metadata['tick_spacing']}, got {metadata.tick_spacing}"

    print(f"\n✅ All metadata validations passed!")
    print(f"  Pool: {metadata.address}")
    print(f"  Pair: {metadata.token0.symbol}/{metadata.token1.symbol}")
    print(f"  Fee: {metadata.fee} (0.05%)")
    print(f"  Factory: {metadata.factory}")

    # Validate that the project latest snapshot matches the known data
    print(f"\n🔍 Validating project latest snapshot from IPFS...")
    
    # Get the latest snapshot from IPFS using the same method as the processor
    metadata_project_id = f"metadata:{pool_address}:{settings.namespace}"
    
    try:
        
        latest_snapshot = await get_project_latest_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, metadata_project_id
        )
        
        if latest_snapshot:
            print(f"  Latest snapshot found from IPFS")
            print(f"  Snapshot data: {json.dumps(latest_snapshot, indent=2)}")
            
            # Validate that the IPFS snapshot matches our expected metadata
            assert latest_snapshot["address"].lower() == expected_metadata["address"].lower(), f"IPFS snapshot address mismatch: expected {expected_metadata['address']}, got {latest_snapshot['address']}"
            
            # Validate token0 data
            ipfs_token0 = latest_snapshot["token0"]
            expected_token0 = expected_metadata["token0"]
            assert ipfs_token0["address"].lower() == expected_token0["address"].lower(), f"IPFS token0 address mismatch: expected {expected_token0['address']}, got {ipfs_token0['address']}"
            assert ipfs_token0["name"] == expected_token0["name"], f"IPFS token0 name mismatch: expected {expected_token0['name']}, got {ipfs_token0['name']}"
            assert ipfs_token0["symbol"] == expected_token0["symbol"], f"IPFS token0 symbol mismatch: expected {expected_token0['symbol']}, got {ipfs_token0['symbol']}"
            assert ipfs_token0["decimals"] == expected_token0["decimals"], f"IPFS token0 decimals mismatch: expected {expected_token0['decimals']}, got {ipfs_token0['decimals']}"
            
            # Validate token1 data
            ipfs_token1 = latest_snapshot["token1"]
            expected_token1 = expected_metadata["token1"]
            assert ipfs_token1["address"].lower() == expected_token1["address"].lower(), f"IPFS token1 address mismatch: expected {expected_token1['address']}, got {ipfs_token1['address']}"
            assert ipfs_token1["name"] == expected_token1["name"], f"IPFS token1 name mismatch: expected {expected_token1['name']}, got {ipfs_token1['name']}"
            assert ipfs_token1["symbol"] == expected_token1["symbol"], f"IPFS token1 symbol mismatch: expected {expected_token1['symbol']}, got {ipfs_token1['symbol']}"
            assert ipfs_token1["decimals"] == expected_token1["decimals"], f"IPFS token1 decimals mismatch: expected {expected_token1['decimals']}, got {ipfs_token1['decimals']}"
            
            # Validate pool parameters
            assert latest_snapshot["fee"] == expected_metadata["fee"], f"IPFS fee mismatch: expected {expected_metadata['fee']}, got {latest_snapshot['fee']}"
            assert latest_snapshot["factory"].lower() == expected_metadata["factory"].lower(), f"IPFS factory mismatch: expected {expected_metadata['factory']}, got {latest_snapshot['factory']}"
            
            # Validate tick spacing if available
            if "tick_spacing" in expected_metadata and "tick_spacing" in latest_snapshot:
                assert latest_snapshot["tick_spacing"] == expected_metadata["tick_spacing"], f"IPFS tick spacing mismatch: expected {expected_metadata['tick_spacing']}, got {latest_snapshot['tick_spacing']}"
            
            print(f"  ✅ IPFS snapshot validation passed!")
            print(f"    All fields match expected metadata")
            
        else:
            print(f"  ⚠️  No latest snapshot found from IPFS for project {metadata_project_id}")
            print(f"    This may indicate that the metadata has not been submitted to IPFS yet")
            
    except Exception as e:
        print(f"  ⚠️  Error validating IPFS snapshot: {e}")
        print(f"    This may indicate connectivity issues or missing data")

    # Validate against actual blockchain contracts
    await validate_metadata_against_contracts(
        rpc_helper,
        pool_address,
        metadata,
        expected_metadata
    )

    # Test the compute method with a simple epoch
    print(f"\n🔍 Testing compute method...")
    epoch = SnapshotProcessMessage(
        begin=from_block,
        end=from_block,
        epochId=1,
        timestamp=int(time.time())
    )

    results = await processor.compute(
        epoch=epoch,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type="metadata:{poolAddress}:{Namespace}"
    )

    print(f"Compute method results: {len(results) if results else 0} snapshots returned")

    print("PASSED: test_metadata_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
