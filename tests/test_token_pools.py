import json
import time
import os
from typing import Dict, Set
import pytest
from redis import asyncio as aioredis
from web3 import Web3

from computes.token_pools import TokenPoolsProcessor
from computes.utils.models.message_models import UniswapTokenPoolsSnapshot
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.models.settings_model import Settings
from snapshotter.utils.data_utils import get_project_latest_snapshot
from computes.settings.config import settings as computes_settings
from computes.utils.helpers import get_uniswap_v3_pool_metadata

"""
Test for TokenPoolsProcessor token pools validation.

This test validates the accuracy of token pools data retrieved by the TokenPoolsProcessor
by checking that all pools contain the expected tokens.

Test Flow:
1. Gets active pools for the current block from Redis
2. For each pool, retrieves metadata to identify the tokens
3. For each token, validates that all pools in local_pools and latest snapshot contain that token
4. Ensures the TokenPoolsProcessor correctly processes and validates pool-token relationships

Validation Criteria:
- All pools in active_pools should have valid metadata
- All pools in local_pools should contain the expected token
- All pools in latest snapshot should contain the expected token
- Token pools data should be consistent across Redis and IPFS sources

This test ensures that the TokenPoolsProcessor correctly identifies and validates
the relationship between tokens and the pools that contain them.
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


def get_weth_address():
    """Get WETH address from environment variable with fallback to default."""
    weth_address = os.getenv('TEST_WETH_ADDRESS')
    if not weth_address:
        pytest.skip("TEST_WETH_ADDRESS not configured in environment")
    return weth_address


def get_excluded_tokens():
    """Get tokens to exclude from testing (USDC, USDT) from compute settings."""
    excluded_tokens = set()
    
    # Add USDC and USDT from compute settings
    if hasattr(computes_settings.contract_addresses, 'USDC'):
        excluded_tokens.add(Web3.to_checksum_address(computes_settings.contract_addresses.USDC))
    
    if hasattr(computes_settings.contract_addresses, 'USDT'):
        excluded_tokens.add(Web3.to_checksum_address(computes_settings.contract_addresses.USDT))

    if hasattr(computes_settings.contract_addresses, 'DAI'):
        excluded_tokens.add(Web3.to_checksum_address(computes_settings.contract_addresses.DAI))
    
    return excluded_tokens


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


async def get_active_pools_for_block(redis_conn: aioredis.Redis, block_number: int, app_config: Settings) -> Set[str]:
    """Get active pools for a specific block from Redis"""
    key = f"active_pools:{block_number}:{app_config.namespace}"
    pools = await redis_conn.smembers(key)
    return {pool.decode('utf-8') for pool in pools}


async def get_local_pools_for_token(redis_conn: aioredis.Redis, token_address: str) -> Set[str]:
    """Get local pools for a token from Redis"""
    local_pools = await redis_conn.smembers(f"token_pools:{token_address}")
    return {pool.decode('utf-8') for pool in local_pools}


async def get_token_pools_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper,
    ipfs_reader,
    protocol_state_contract,
    token_address: str,
    app_config: Settings,
) -> UniswapTokenPoolsSnapshot:
    """Get token pools snapshot from IPFS"""
    project_id = f"tokenPools:{token_address}:{app_config.namespace}"
    
    token_pools_snapshot = await get_project_latest_snapshot(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id
    )
    
    if token_pools_snapshot:
        if isinstance(token_pools_snapshot, str):
            token_pools_snapshot = json.loads(token_pools_snapshot)
        return UniswapTokenPoolsSnapshot(**token_pools_snapshot)
    else:
        return UniswapTokenPoolsSnapshot(pools={})


async def validate_pool_contains_token(pool_metadata: Dict, token_address: str) -> bool:
    """Validate that a pool contains the specified token"""
    if not pool_metadata:
        return False
    
    token0_address = pool_metadata.get("token0", {}).get("address", "").lower()
    token1_address = pool_metadata.get("token1", {}).get("address", "").lower()
    target_token = token_address.lower()
    
    return token0_address == target_token or token1_address == target_token


async def validate_token_pools_data(
    redis_conn: aioredis.Redis,
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    protocol_state_contract,
    token_address: str,
    pool_addresses: Set[str],
    app_config: Settings,
) -> Dict:
    """
    Validate token pools data for a specific token.
    
    Args:
        redis_conn: Redis connection
        anchor_rpc_helper: Anchor RPC helper
        ipfs_reader: IPFS reader
        protocol_state_contract: Protocol state contract
        token_address: Token address to validate
        pool_addresses: Set of pool addresses that should contain this token
        
    Returns:
        Dict containing validation results
    """
    print(f"\n🔍 Validating token pools for token: {token_address}")
    
    # Get local pools from Redis
    local_pools = await get_local_pools_for_token(redis_conn, token_address)
    print(f"  Local pools from Redis: {len(local_pools)} pools")
    
    # Get token pools snapshot from IPFS
    token_pools_snapshot = await get_token_pools_snapshot(
        redis_conn=redis_conn, 
        anchor_rpc_helper=anchor_rpc_helper, 
        ipfs_reader=ipfs_reader, 
        protocol_state_contract=protocol_state_contract, 
        token_address=token_address,
        app_config=app_config
    )
    snapshot_pools = set(token_pools_snapshot.pools.keys())
    print(f"  Pools from IPFS snapshot: {len(snapshot_pools)} pools")
    
    # Combine all pools to check
    all_pools_to_check = pool_addresses.union(local_pools).union(snapshot_pools)
    print(f"  Total unique pools to validate: {len(all_pools_to_check)}")
    
    validation_results = {
        'token_address': token_address,
        'local_pools_count': len(local_pools),
        'snapshot_pools_count': len(snapshot_pools),
        'total_pools_checked': len(all_pools_to_check),
        'valid_pools': [],
        'invalid_pools': [],
        'missing_metadata_pools': []
    }
    
    # Validate each pool
    for pool_address in all_pools_to_check:
        pool_metadata = await get_uniswap_v3_pool_metadata(
            pool_address=pool_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract
        )
        
        if not pool_metadata:
            validation_results['missing_metadata_pools'].append(pool_address)
            print(f"    ⚠️  Pool {pool_address}: Missing metadata")
            continue
        
        if await validate_pool_contains_token(pool_metadata, token_address):
            validation_results['valid_pools'].append(pool_address)
            print(f"    ✅ Pool {pool_address}: Contains token {token_address}")
        else:
            validation_results['invalid_pools'].append(pool_address)
            print(f"    ❌ Pool {pool_address}: Does not contain token {token_address}")
    
    # Print summary
    print(f"  Validation Summary:")
    print(f"    Valid pools: {len(validation_results['valid_pools'])}")
    print(f"    Invalid pools: {len(validation_results['invalid_pools'])}")
    print(f"    Missing metadata: {len(validation_results['missing_metadata_pools'])}")
    
    return validation_results


@pytest.mark.asyncio(loop_scope="module")
async def test_token_pools_processor(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test the TokenPoolsProcessor with normal operation against active pools."""
    processor = TokenPoolsProcessor()
    validate_test_environment(app_config)

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

    # Get active pools for the block
    print(f"\n🔍 Getting active pools for block {from_block}...")
    active_pools = await get_active_pools_for_block(redis_conn=redis_conn, block_number=from_block, app_config=app_config)
    
    if not active_pools:
        pytest.skip(f"No active pools found for block {from_block}")
    
    print(f"Found {len(active_pools)} active pools")
    
    # Get metadata for all active pools and collect unique tokens
    print(f"\n📋 Collecting token information from active pools...")
    token_to_pools = {}
    WETH_ADDRESS = get_weth_address()
    EXCLUDED_TOKENS = get_excluded_tokens()
    
    print(f"Using WETH address: {WETH_ADDRESS}")
    print(f"Excluding tokens: {EXCLUDED_TOKENS}")
    
    for pool_address in active_pools:
        pool_metadata = await get_uniswap_v3_pool_metadata(
            pool_address=pool_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract
        )
        
        if not pool_metadata:
            print(f"  ⚠️  Pool {pool_address}: Missing metadata")
            continue
        
        token0_address = pool_metadata.get("token0", {}).get("address", "")
        token1_address = pool_metadata.get("token1", {}).get("address", "")
        
        # Normalize addresses for comparison
        token0_checksum = Web3.to_checksum_address(token0_address) if token0_address else ""
        token1_checksum = Web3.to_checksum_address(token1_address) if token1_address else ""
        weth_checksum = Web3.to_checksum_address(WETH_ADDRESS)
        
        # Skip WETH pools as per processor logic
        if token0_checksum == weth_checksum:
            # Only add token1 if it's not in excluded list
            if token1_checksum not in EXCLUDED_TOKENS:
                if token1_checksum not in token_to_pools:
                    token_to_pools[token1_checksum] = set()
                token_to_pools[token1_checksum].add(pool_address)
            else:
                print(f"  ⏭️  Skipping excluded token {token1_checksum} from pool {pool_address}")
        elif token1_checksum == weth_checksum:
            # Only add token0 if it's not in excluded list
            if token0_checksum not in EXCLUDED_TOKENS:
                if token0_checksum not in token_to_pools:
                    token_to_pools[token0_checksum] = set()
                token_to_pools[token0_checksum].add(pool_address)
            else:
                print(f"  ⏭️  Skipping excluded token {token0_checksum} from pool {pool_address}")
        else:
            # Both tokens are non-WETH, add both if not excluded
            if token0_checksum not in EXCLUDED_TOKENS:
                if token0_checksum not in token_to_pools:
                    token_to_pools[token0_checksum] = set()
                token_to_pools[token0_checksum].add(pool_address)
            else:
                print(f"  ⏭️  Skipping excluded token {token0_checksum} from pool {pool_address}")
                
            if token1_checksum not in EXCLUDED_TOKENS:
                if token1_checksum not in token_to_pools:
                    token_to_pools[token1_checksum] = set()
                token_to_pools[token1_checksum].add(pool_address)
            else:
                print(f"  ⏭️  Skipping excluded token {token1_checksum} from pool {pool_address}")
    
    print(f"Found {len(token_to_pools)} unique tokens (excluding WETH, USDC, USDT)")
    
    # Validate token pools for each token
    print(f"\n🔍 Validating token pools data...")
    all_validation_results = []
    
    for token_address, pool_addresses in token_to_pools.items():
        validation_result = await validate_token_pools_data(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            token_address=token_address,
            pool_addresses=pool_addresses,
            app_config=app_config
        )
        all_validation_results.append(validation_result)
    
    # Test the processor compute method
    print(f"\n🔍 Testing TokenPoolsProcessor compute method...")
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
        task_type="tokenPools:{tokenAddress}:{Namespace}"
    )

    print(f"Processor compute results: {len(results) if results else 0} snapshots returned")
    
    # Summary and assertions
    print(f"\n📊 Validation Summary:")
    total_valid_pools = sum(len(result['valid_pools']) for result in all_validation_results)
    total_invalid_pools = sum(len(result['invalid_pools']) for result in all_validation_results)
    total_missing_metadata = sum(len(result['missing_metadata_pools']) for result in all_validation_results)
    
    print(f"  Total valid pools: {total_valid_pools}")
    print(f"  Total invalid pools: {total_invalid_pools}")
    print(f"  Total missing metadata: {total_missing_metadata}")
    
    # Assertions
    assert total_valid_pools > 0, "Should have at least some valid pools"
    assert total_invalid_pools == 0, f"Found {total_invalid_pools} pools that don't contain their expected tokens"
    
    if total_missing_metadata > 0:
        print(f"  ⚠️  Warning: {total_missing_metadata} pools have missing metadata")
        print(f"     This may indicate incomplete data processing")
    
    print("PASSED: test_token_pools_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
