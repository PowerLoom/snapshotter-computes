import asyncio
import json
import os
import time
from typing import List, Dict
from web3 import Web3
import pytest
from redis import asyncio as aioredis

from computes.active_pools import ActivePoolsProcessor
from computes.utils.models.message_models import ActivePoolsSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.settings.config import settings


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


async def get_block_transaction_receipts(rpc_helper, block_number: int) -> List[Dict]:
    """Get all transaction receipts in a block by fetching receipts in batches"""
    # First get the block data to get transaction hashes
    block = await rpc_helper.eth_get_block(block_number)
    if not block or 'transactions' not in block:
        return []
    
    # Process transaction receipts in batches of 50
    BATCH_SIZE = 50
    receipts = []
    tx_hashes = block['transactions']
    
    for i in range(0, len(tx_hashes), BATCH_SIZE):
        batch_hashes = tx_hashes[i:i + BATCH_SIZE]
        # Create tasks for each transaction receipt in the batch
        tasks = [
            rpc_helper.get_transaction_receipt(tx_hash)
            for tx_hash in batch_hashes
        ]
        
        try:
            # Execute batch of tasks concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for receipt in batch_results:
                if isinstance(receipt, Exception):
                    logger.warning(f"Failed to get transaction receipt: {receipt}")
                    continue
                if receipt:
                    receipts.append(receipt)
                    
        except Exception as e:
            logger.error(f"Error processing batch of transaction receipts: {e}")
    
    return receipts


async def count_pool_occurrences_in_logs(receipts: List[Dict], pool_address: str) -> int:
    """Count how many times a pool address appears in transaction logs"""
    count = 0
    pool_address = Web3.to_checksum_address(pool_address)
    
    print(f"\nSearching for pool address in transaction logs: {pool_address}")
    print(f"Total receipts to process: {len(receipts)}")
    
    found_details = []
    complete_logs = []
    
    for i, receipt in enumerate(receipts):
        if not receipt or 'logs' not in receipt or not isinstance(receipt.get('logs'), list):
            continue
            
        receipt_count = 0
        tx_hash = receipt.get('transactionHash', 'unknown')
        
        for j, log_entry in enumerate(receipt['logs']):
            log_count = 0
            
            # Check if the log address (contract that emitted the log) is the pool
            if log_entry.get('address'):
                log_address = Web3.to_checksum_address(log_entry['address'])
                if log_address == pool_address:
                    count += 1
                    log_count += 1
                    receipt_count += 1
                    found_details.append(f"receipt[{i}] log[{j}] address: {log_address}")
                    print(f"  Found in receipt[{i}] log[{j}] address: {log_address}")
            
            if log_count > 0:
                print(f"    Log {j} total matches: {log_count}")
                # Store complete log entry for detailed analysis
                complete_logs.append({
                    'tx_hash': tx_hash,
                    'receipt_index': i,
                    'log_index': j,
                    'log_entry': log_entry
                })
        
        if receipt_count > 0:
            print(f"  Receipt {i} total matches: {receipt_count}")
    
    print(f"Total log occurrences found: {count}")
    print(f"Detailed findings:")
    for i, detail in enumerate(found_details, 1):
        print(f"  {i}. {detail}")
    
    return count, complete_logs


async def verify_redis_data(redis_conn: aioredis.Redis, block_number: int) -> Dict[str, int]:
    """Verify and return Redis data for a specific block"""
    key = f"active_pools_per_block:{block_number}:{settings.namespace}"
    data = await redis_conn.zrange(key, 0, -1, withscores=True)

    print(f"Redis data for block {block_number}: {data}")
    
    if not data:
        pytest.skip(f"No Redis data found for block {block_number}")
    
    return {
        pool_address.decode('utf-8'): int(score)
        for pool_address, score in data
    }


async def verify_pool_on_factory(rpc_helper, pool_address: str, factory_address: str = "0x1F98431c8aD98523631AE4a59f267346ea31F984") -> bool:
    """Verify if a pool address is actually a Uniswap V3 pool by querying the factory contract"""
    try:
        # Get the path to the factory ABI
        abi_path = "computes/static/abis/IUniswapV3Factory.json"
        with open(abi_path, 'r') as f:
            factory_abi = json.load(f)
        
        # Load the pool ABI to get token0 and token1
        pool_abi_path = "computes/static/abis/UniswapV3Pool.json"
        with open(pool_abi_path, 'r') as f:
            pool_abi = json.load(f)
        
        # Get token0 and token1 from the pool contract
        try:
            token0_result = await rpc_helper.web3_call(
                tasks=[("token0", [])],
                contract_addr=pool_address,
                abi=pool_abi
            )
            token1_result = await rpc_helper.web3_call(
                tasks=[("token1", [])],
                contract_addr=pool_address,
                abi=pool_abi
            )
            
            if not token0_result or not token1_result:
                print(f"❌ Could not get token0/token1 from pool {pool_address}")
                return False
                
            token0 = token0_result[0]
            token1 = token1_result[0]
            
            print(f"🔍 Pool {pool_address} has token0: {token0}, token1: {token1}")
            
        except Exception as e:
            print(f"❌ Error getting token0/token1 from pool {pool_address}: {e}")
            return False
        
        # Get fee from the pool contract
        try:
            fee_result = await rpc_helper.web3_call(
                tasks=[("fee", [])],
                contract_addr=pool_address,
                abi=pool_abi
            )
            
            if not fee_result:
                print(f"❌ Could not get fee from pool {pool_address}")
                return False
                
            fee = fee_result[0]
            print(f"🔍 Pool {pool_address} has fee: {fee}")
            
        except Exception as e:
            print(f"❌ Error getting fee from pool {pool_address}: {e}")
            return False
        
        # Now verify on the factory by calling getPool with the actual tokens and fee
        try:
            factory_result = await rpc_helper.web3_call(
                tasks=[("getPool", [token0, token1, fee])],
                contract_addr=factory_address,
                abi=factory_abi
            )
            
            if not factory_result or not factory_result[0]:
                print(f"❌ Factory getPool returned null for tokens {token0}, {token1}, fee {fee}")
                return False
                
            factory_pool = factory_result[0]
            
            if factory_pool.lower() == pool_address.lower():
                print(f"✅ Verified pool {pool_address} exists on factory!")
                print(f"  Token0: {token0}")
                print(f"  Token1: {token1}")
                print(f"  Fee: {fee}")
                return True
            else:
                print(f"❌ Factory returned different pool address: {factory_pool}")
                print(f"  Expected: {pool_address}")
                print(f"  Got: {factory_pool}")
                return False
                
        except Exception as e:
            print(f"❌ Error calling factory getPool: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying pool {pool_address} on factory: {e}")
        return False


@pytest.mark.asyncio(loop_scope="module")
async def test_active_pools_processor(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test the ActivePoolsProcessor with normal operation against a historical block."""
    processor = ActivePoolsProcessor()
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

    # Verify Redis data exists for this block
    redis_data = await verify_redis_data(redis_conn, from_block)
    assert redis_data, f"No Redis data found for block {from_block}"
    
    print(f"\nRedis data contains {len(redis_data)} pools:")
    for pool_addr, freq in redis_data.items():
        print(f"  {pool_addr}: {freq}")

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
        task_type="activePools:{Namespace}"
    )

    assert len(results) == 1, "Should return one result tuple"
    task_type, snapshot = results[0]
    assert isinstance(snapshot, ActivePoolsSnapshot), "Should return ActivePoolsSnapshot"
    assert snapshot.epoch.begin == from_block, "Snapshot should have correct begin block"
    assert snapshot.epoch.end == from_block, "Snapshot should have correct end block"

    assert snapshot.pools == redis_data, "Snapshot pools should match Redis data"

    receipts = await get_block_transaction_receipts(rpc_helper, from_block)
    print(f"\nNumber of transaction receipts in block: {len(receipts)}")
    
    # Verify each pool's frequency matches actual occurrences in transaction logs
    for pool_address, frequency in snapshot.pools.items():
        # Verify if this is actually a Uniswap V3 pool
        print(f"\n🔍 Verifying pool on Uniswap V3 factory...")
        is_valid_pool = await verify_pool_on_factory(rpc_helper, pool_address)
        print(f"  Pool verification result: {'✅ Valid' if is_valid_pool else '❌ Invalid'}")

        actual_occurrences, complete_logs = await count_pool_occurrences_in_logs(receipts, pool_address)
        
        if frequency != actual_occurrences:
            print(f"\n🚨 FREQUENCY MISMATCH for pool {pool_address}:")
            print(f"  Redis frequency: {frequency}")
            print(f"  Actual occurrences: {actual_occurrences}")
            print(f"  Difference: {actual_occurrences - frequency}")
            
            print(f"\n📋 Complete logs for this pool:")
            for i, log_info in enumerate(complete_logs, 1):
                print(f"\n  Log {i}:")
                print(f"    Transaction Hash: {str(log_info['tx_hash'].hex())}")
                print(f"    Receipt Index: {log_info['receipt_index']}")
                print(f"    Log Index: {log_info['log_index']}")
                print(f"    Log Address: {log_info['log_entry'].get('address')}")
                print(f"    Log Topics: {log_info['log_entry'].get('topics')}")
                print(f"    Log Data: {log_info['log_entry'].get('data')}")
                print(f"    Log Index (hex): {log_info['log_entry'].get('logIndex')}")
                print(f"    Block Number: {log_info['log_entry'].get('blockNumber')}")
            
            print(f"\n🔍 Analysis:")
            print(f"  - Found {len(complete_logs)} log entries containing this pool")
            print(f"  - Redis shows frequency {frequency}")
            print(f"  - Pool verification: {'Valid' if is_valid_pool else 'Invalid'}")
            if not is_valid_pool:
                print(f"  - ⚠️  This pool address may not be a valid Uniswap V3 pool!")
            print(f"  - This suggests Redis data may be incomplete or our counting logic needs adjustment")
        
        assert frequency == actual_occurrences, f"Pool {pool_address} frequency mismatch: Redis={frequency}, Actual={actual_occurrences}"

    print("PASSED: test_active_pools_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
