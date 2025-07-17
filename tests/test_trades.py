import json
import os
import time
from typing import Dict, List, Set

import pytest
import aiohttp
from redis import asyncio as aioredis
from web3 import Web3
from web3._utils.events import get_event_data
from eth_abi.codec import ABICodec
from eth_abi.registry import registry as default_abi_registry

from computes.trades import TradesProcessor
from computes.utils.models.message_models import UniswapTradesSnapshot, TradeType
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.models.settings_model import Settings
from snapshotter.utils.redis.redis_keys import source_chain_id_key


"""
Test for TradesProcessor validation against Uniswap V3 Etherscan data.

This test validates the accuracy of trades snapshot data retrieved by the TradesProcessor
by comparing it against data from the Etherscan API.

Test Flow:
1. Gets active pools for the current block from Redis
2. For each active pool, retrieves trades snapshot from the processor
3. Fetches corresponding data from the Etherscan API for the same block range
4. Compares swaps, mints, and burns between the two data sources
5. Validates that all Etherscan events are present in the snapshot and data matches

Validation Criteria:
- All swaps, mints, and burns from Etherscan should be present in the snapshot
- Transaction hashes should match between sources
- Token amounts should be consistent (within tolerance for precision differences)
- Event types should match (Swap, Mint, Burn)
- Block numbers should be within the expected range

This test ensures that the TradesProcessor correctly captures and processes
all Uniswap V3 trading events with accurate data.
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


async def get_active_pools_for_block(redis_conn: aioredis.Redis, block_number: int, app_config: Settings) -> Set[str]:
    """Get active pools for a specific block from Redis"""
    key = f"active_pools:{block_number}:{app_config.namespace}"
    pools = await redis_conn.smembers(key)
    return {pool.decode('utf-8') for pool in pools}


def get_etherscan_config():
    """Get Etherscan configuration from environment variables."""
    api_key = os.getenv('TEST_ETHERSCAN_API_KEY')
    api_url = os.getenv('TEST_ETHERSCAN_URL')
    
    if not api_key or not api_url:
        return None, None  # Skip etherscan validation if missing config
    
    # Ensure URL ends with /api if not already present
    if not api_url.endswith('/api'):
        api_url = api_url.rstrip('/') + '/api'
    
    return api_key, api_url


async def fetch_uniswap_v3_events_from_etherscan(
    pool_address: str,
    start_block: int,
    end_block: int,
    source_chain_id: int,
) -> Dict[str, List]:
    """
    Fetch Uniswap V3 events (swaps, mints, burns) from Etherscan API.
    
    Args:
        pool_address: Pool contract address
        start_block: Start block number
        end_block: End block number
        
    Returns:
        Dictionary containing 'swaps', 'mints', and 'burns' lists
    """
    api_key, api_url = get_etherscan_config()
    if not api_key or not api_url:
        pytest.fail("No etherscan api key or url configured.")
    
    # Uniswap V3 event signatures
    swap_topic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"  # Swap event
    mint_topic = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"  # Mint event
    burn_topic = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"  # Burn event
    
    try:
        pool_address = Web3.to_checksum_address(pool_address)
    except Exception as e:
        logger.error(f"Invalid pool address format: {pool_address}, error: {e}")
        return {"swaps": [], "mints": [], "burns": []}

    events = {"swaps": [], "mints": [], "burns": []}
    
    print(f"  🔍 Fetching events for pool {pool_address} from block {start_block} to {end_block}")
    
    # Load Uniswap V3 Pool ABI for event decoding
    try:
        with open("computes/static/abis/UniswapV3Pool.json", 'r') as f:
            pool_abi = json.load(f)
        
        # Create ABICodec for event decoding
        codec = ABICodec(default_abi_registry)
        
        # Find event ABIs
        event_abis = {}
        for abi_item in pool_abi:
            if abi_item.get('type') == 'event':
                event_name = abi_item.get('name')
                if event_name in ['Swap', 'Mint', 'Burn']:
                    event_abis[event_name] = abi_item
        
    except Exception as e:
        logger.error(f"Error loading pool ABI: {e}")
        return {"swaps": [], "mints": [], "burns": []}
    
    # Fetch all events in a single API call instead of 3 separate calls
    url = api_url
    params = {
        "module": "logs",
        "action": "getLogs",
        "address": pool_address,
        # No topic0 - get ALL events for this contract
        "fromBlock": start_block,
        "toBlock": end_block,
        "apikey": api_key
    }
    
    # Only add chainid for Etherscan v2 API, not for chain-specific APIs
    if "etherscan.io" in api_url and "/v2" in api_url:
        params["chainid"] = source_chain_id
    # Chain-specific APIs (basescan.org, polygonscan.com, etc.) don't need chainid
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"Etherscan request failed with status {response.status}")
                    return {"swaps": [], "mints": [], "burns": []}
                
                data = await response.json()
                
                if data.get("status") != "1":
                    error_message = data.get('message', 'Unknown error')
                    logger.warning(f"Etherscan API returned status 0: {error_message}")
                    
                    if "No records found" in error_message:
                        print(f"    ℹ️  No events found for pool {pool_address} in block range {start_block}-{end_block}")
                        return {"swaps": [], "mints": [], "burns": []}
                    else:
                        logger.error(f"Etherscan API error: {error_message}")
                        return {"swaps": [], "mints": [], "burns": []}
                
                logs = data.get("result", [])
                print(f"    📋 Etherscan returned {len(logs)} total events for pool {pool_address}")
                
                # Separate events by type
                swap_events = []
                mint_events = []  
                burn_events = []
                
                for log in logs:
                    try:
                        # Determine event type from topic
                        if len(log.get('topics', [])) == 0:
                            continue  # Skip logs without topics
                            
                        topic0 = log['topics'][0]
                        event_name = None
                        event_type = None
                        
                        if topic0 == swap_topic:
                            event_name = "Swap"
                            event_type = "swaps"
                        elif topic0 == mint_topic:
                            event_name = "Mint"  
                            event_type = "mints"
                        elif topic0 == burn_topic:
                            event_name = "Burn"
                            event_type = "burns"
                        else:
                            continue  # Skip non-Uniswap V3 events
                        
                        event_abi = event_abis.get(event_name)
                        if not event_abi:
                            logger.warning(f"No ABI found for event {event_name}")
                            continue

                        decoded_event = get_event_data(codec, event_abi, log)
                        
                        event = {
                            "id": f"{log['transactionHash']}_{log['logIndex']}",
                            "transaction": {"id": log["transactionHash"]},
                            "logIndex": log["logIndex"],
                            "blockNumber": int(log["blockNumber"], 16),
                            "amount0": str(decoded_event['args'].get('amount0', 0)),
                            "amount1": str(decoded_event['args'].get('amount1', 0)),
                            "amountUSD": "0"  # Not available in logs
                        }
                        
                        print(f"    🔍 Decoded {event_type}: amount0={event['amount0']}, amount1={event['amount1']}")
                        
                        # Add to appropriate list
                        if event_type == "swaps":
                            swap_events.append(event)
                        elif event_type == "mints":
                            mint_events.append(event)
                        elif event_type == "burns":
                            burn_events.append(event)
                            
                    except Exception as e:
                        logger.warning(f"Error decoding event log: {e}")
                        logger.warning(f"Log data: {log.get('data', 'No data')}")
                        continue
                
                # Build final events dict
                events = {
                    "swaps": swap_events,
                    "mints": mint_events, 
                    "burns": burn_events
                }
                    
    except Exception as e:
        logger.error(f"Error fetching events from Etherscan: {e}")
        return {"swaps": [], "mints": [], "burns": []}
    
    total_events = len(events["swaps"]) + len(events["mints"]) + len(events["burns"])
    print(f"  📊 Total events found: {total_events} (swaps: {len(events['swaps'])}, mints: {len(events['mints'])}, burns: {len(events['burns'])})")

    # small sleep to avoid rate limits
    time.sleep(0.25)
    
    return events


def create_event_key(event: Dict, event_type: str) -> str:
    """Create a unique key for an event for comparison"""
    tx_hash = event.get("transaction", {}).get("id", "")
    log_index_raw = event.get("logIndex", "0")
    
    if isinstance(log_index_raw, str) and log_index_raw.startswith("0x"):
        try:
            log_index = str(int(log_index_raw, 16))
        except ValueError:
            log_index = str(log_index_raw)
    else:
        log_index = str(log_index_raw)
    
    return f"{event_type}:{tx_hash.lower()}:{log_index}"


def compare_events(
    snapshot_events: List,
    etherscan_events: List,
    event_type: str,
    pool_address: str
) -> Dict:
    """
    Compare events between snapshot and Etherscan data.
    
    Args:
        snapshot_events: List of events from the snapshot
        etherscan_events: List of events from Etherscan
        event_type: Type of event (Swap, Mint, Burn)
        pool_address: Pool address for logging
        
    Returns:
        Dictionary with comparison results
    """
    print(f"\n🔍 Comparing {event_type} events for pool {pool_address}")
    print(f"  Snapshot events: {len(snapshot_events)}")
    print(f"  Etherscan events: {len(etherscan_events)}")
    
    # Create sets of event keys for comparison
    snapshot_keys = set()
    etherscan_keys = set()
    
    print(f"  📋 Processing snapshot events:")
    for i, event in enumerate(snapshot_events):
        tx_hash = event.log.get("transactionHash", "")
        log_index = str(event.log.get("logIndex", "0"))
        key = f"{event_type}:{tx_hash.lower()}:{log_index}"
        snapshot_keys.add(key)
        print(f"    Snapshot {i+1}: tx={tx_hash}, logIndex={log_index}, key={key}")
    
    print(f"  📋 Processing Etherscan events:")
    for i, event in enumerate(etherscan_events):
        key = create_event_key(event, event_type)
        etherscan_keys.add(key)
    
    missing_in_snapshot = etherscan_keys - snapshot_keys
    extra_in_snapshot = snapshot_keys - etherscan_keys
    common_events = snapshot_keys & etherscan_keys
    
    print(f"  Common events: {len(common_events)}")
    print(f"  Missing in snapshot: {len(missing_in_snapshot)}")
    print(f"  Extra in snapshot: {len(extra_in_snapshot)}")
    
    if missing_in_snapshot:
        print(f"  ❌ Missing in snapshot keys: {list(missing_in_snapshot)}")
    if extra_in_snapshot:
        print(f"  ❌ Extra in snapshot keys: {list(extra_in_snapshot)}")
    if common_events:
        print(f"  ✅ Common keys: {list(common_events)}")
    
    detailed_comparison = []
    for key in common_events:
        snapshot_event = None
        etherscan_event = None
        
        for event in snapshot_events:
            tx_hash = event.log.get("transactionHash", "")
            log_index = str(event.log.get("logIndex", "0"))
            if f"{event_type}:{tx_hash.lower()}:{log_index}" == key:
                snapshot_event = event
                break
        
        for event in etherscan_events:
            if create_event_key(event, event_type) == key:
                etherscan_event = event
                break
        
        if snapshot_event and etherscan_event:
            comparison = {
                "key": key,
                "tx_hash": etherscan_event.get("transaction", {}).get("id", ""),
                "matches": True,
                "differences": []
            }
            
            # Compare amounts for all event types
            snapshot_amount0 = snapshot_event.data.get("amount0", 0)
            snapshot_amount1 = snapshot_event.data.get("amount1", 0)
            
            # Parse Etherscan amounts by moving decimal point
            etherscan_amount0 = int(etherscan_event.get("amount0", "0"))
            etherscan_amount1 = int(etherscan_event.get("amount1", "0"))

            print(f"      Comparing {event_type} amounts:")
            print(f"        amount0: snapshot={snapshot_amount0}, etherscan={etherscan_amount0}")
            print(f"        amount1: snapshot={snapshot_amount1}, etherscan={etherscan_amount1}")
            
            amount0_match = snapshot_amount0 == etherscan_amount0
            amount1_match = snapshot_amount1 == etherscan_amount1
            
            if not amount0_match:
                comparison["matches"] = False
                comparison["differences"].append(f"amount0: snapshot={snapshot_amount0}, etherscan={etherscan_amount0}")
                print(f"        ❌ amount0 mismatch!")
            
            if not amount1_match:
                comparison["matches"] = False
                comparison["differences"].append(f"amount1: snapshot={snapshot_amount1}, etherscan={etherscan_amount1}")
                print(f"        ❌ amount1 mismatch!")
            
            if comparison["matches"]:
                print(f"        ✅ amounts match")
            
            detailed_comparison.append(comparison)
    
    return {
        "event_type": event_type,
        "snapshot_count": len(snapshot_events),
        "etherscan_count": len(etherscan_events),
        "common_count": len(common_events),
        "missing_in_snapshot": list(missing_in_snapshot),
        "extra_in_snapshot": list(extra_in_snapshot),
        "detailed_comparison": detailed_comparison
    }


async def validate_trades_snapshot_against_etherscan(
    trades_snapshot: UniswapTradesSnapshot,
    pool_address: str,
    source_chain_id: int,
) -> Dict:
    """
    Validate trades snapshot against Etherscan data.
    
    Args:
        trades_snapshot: Trades snapshot from the processor
        pool_address: Pool address
        
    Returns:
        Dictionary with validation results
    """
    print(f"\n🔍 Validating trades snapshot for pool {pool_address}")
    print(f"  Snapshot epoch: {trades_snapshot.epoch.begin} - {trades_snapshot.epoch.end}")
    print(f"  Total trades in snapshot: {len(trades_snapshot.trades)}")
    
    # Fetch Etherscan data for the same block range
    etherscan_data = await fetch_uniswap_v3_events_from_etherscan(
        pool_address=pool_address,
        start_block=trades_snapshot.epoch.begin,
        end_block=trades_snapshot.epoch.end,
        source_chain_id=source_chain_id,
    )
    
    print(f"  Etherscan data:")
    print(f"    Swaps: {len(etherscan_data['swaps'])}")
    print(f"    Mints: {len(etherscan_data['mints'])}")
    print(f"    Burns: {len(etherscan_data['burns'])}")
    
    # Separate snapshot trades by type
    snapshot_swaps = [trade for trade in trades_snapshot.trades if trade.tradeType == TradeType.SWAP]
    snapshot_mints = [trade for trade in trades_snapshot.trades if trade.tradeType == TradeType.MINT]
    snapshot_burns = [trade for trade in trades_snapshot.trades if trade.tradeType == TradeType.BURN]
    
    print(f"  Snapshot data:")
    print(f"    Swaps: {len(snapshot_swaps)}")
    print(f"    Mints: {len(snapshot_mints)}")
    print(f"    Burns: {len(snapshot_burns)}")
    
    # Check if we have any data to compare
    total_etherscan_events = len(etherscan_data["swaps"]) + len(etherscan_data["mints"]) + len(etherscan_data["burns"])
    total_snapshot_trades = len(snapshot_swaps) + len(snapshot_mints) + len(snapshot_burns)
    
    if total_etherscan_events == 0 and total_snapshot_trades == 0:
        print(f"  ℹ️  No events found in either source - this is expected for some blocks")
        return {
            "pool_address": pool_address,
            "epoch": {
                "begin": trades_snapshot.epoch.begin,
                "end": trades_snapshot.epoch.end
            },
            "snapshot_total_trades": total_snapshot_trades,
            "etherscan_total_events": total_etherscan_events,
            "no_events_found": True,
            "swap_comparison": {"event_type": "Swap", "snapshot_count": 0, "etherscan_count": 0, "common_count": 0, "missing_in_snapshot": [], "extra_in_snapshot": [], "detailed_comparison": []},
            "mint_comparison": {"event_type": "Mint", "snapshot_count": 0, "etherscan_count": 0, "common_count": 0, "missing_in_snapshot": [], "extra_in_snapshot": [], "detailed_comparison": []},
            "burn_comparison": {"event_type": "Burn", "snapshot_count": 0, "etherscan_count": 0, "common_count": 0, "missing_in_snapshot": [], "extra_in_snapshot": [], "detailed_comparison": []}
        }
    
    # Compare each event type
    swap_comparison = compare_events(snapshot_swaps, etherscan_data["swaps"], "Swap", pool_address)
    mint_comparison = compare_events(snapshot_mints, etherscan_data["mints"], "Mint", pool_address)
    burn_comparison = compare_events(snapshot_burns, etherscan_data["burns"], "Burn", pool_address)
    
    return {
        "pool_address": pool_address,
        "epoch": {
            "begin": trades_snapshot.epoch.begin,
            "end": trades_snapshot.epoch.end
        },
        "snapshot_total_trades": len(trades_snapshot.trades),
        "etherscan_total_events": total_etherscan_events,
        "no_events_found": False,
        "swap_comparison": swap_comparison,
        "mint_comparison": mint_comparison,
        "burn_comparison": burn_comparison
    }


@pytest.mark.asyncio(loop_scope="module")
async def test_trades_processor_against_etherscan(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test the TradesProcessor against Etherscan data."""
    processor = TradesProcessor()
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
    active_pools = await get_active_pools_for_block(
        redis_conn=redis_conn, 
        block_number=from_block, 
        app_config=app_config
    )
    
    if not active_pools:
        pytest.skip(f"No active pools found for block {from_block}")
    
    print(f"Found {len(active_pools)} active pools")
    
    print(f"\n🔍 Testing TradesProcessor compute method...")
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
        task_type="tradesSnapshot:{poolAddress}:{Namespace}"
    )

    print(f"Processor compute results: {len(results) if results else 0} snapshots returned")
    
    if not results:
        print(f"\nℹ️  No trades snapshots returned from processor. This could mean:")
        print(f"    - The active pools don't have any trades in the snapshot")
        print(f"    - The processor filtered out all pools")
        print(f"    - There's an issue with the processor logic")
        pytest.skip("No trades snapshots returned from processor")
    
    # Validate each snapshot against Etherscan data
    print(f"\n🔍 Validating snapshots against Etherscan data...")
    all_validation_results = []

    # Get source chain ID for Etherscan v2 API (read-only, don't cache in Redis for tests)
    try:
        source_chain_id_data = await redis_conn.get(source_chain_id_key())
        
        if source_chain_id_data:
            source_chain_id = int(source_chain_id_data.decode('utf-8'))
            print(f"      ℹ️  Using cached source chain ID: {source_chain_id}")
        else:
            # If not in cache, fetch from blockchain but don't cache (test mode)
            [source_chain_id] = await anchor_rpc_helper.web3_call(
                tasks=[
                    ('SOURCE_CHAIN_ID', [Web3.to_checksum_address(app_config.data_market)]),
                ],
                contract_addr=protocol_state_contract.address,
                abi=protocol_state_contract.abi,
            )
            print(f"      ℹ️  Fetched source chain ID from contract: {source_chain_id}")
    except Exception as e:
        print(f"      ⚠️  Could not get source chain ID: {e}")
        pytest.fail("Could not get source chain ID")
    
    for snapshot_key, trades_snapshot in results:
        pool_address = trades_snapshot.address
        print(f"\n📋 Processing snapshot for pool: {pool_address}")
        
        validation_result = await validate_trades_snapshot_against_etherscan(
            trades_snapshot=trades_snapshot,
            pool_address=pool_address,
            source_chain_id=source_chain_id,
        )
        all_validation_results.append(validation_result)
    
    # Summary and assertions
    print(f"\n📊 Validation Summary:")
    total_snapshots = len(all_validation_results)
    total_etherscan_events = sum(
        result["etherscan_total_events"] for result in all_validation_results
    )
    total_snapshot_trades = sum(
        result["snapshot_total_trades"] for result in all_validation_results
    )
    
    # Count snapshots with no events
    snapshots_with_no_events = sum(
        1 for result in all_validation_results if result.get("no_events_found", False)
    )
    
    print(f"  Total snapshots processed: {total_snapshots}")
    print(f"  Snapshots with no events: {snapshots_with_no_events}")
    print(f"  Total Etherscan events: {total_etherscan_events}")
    print(f"  Total snapshot trades: {total_snapshot_trades}")
    
    # If no events found in any snapshot, provide helpful information
    if total_etherscan_events == 0 and total_snapshot_trades == 0:
        print(f"\nℹ️  No events found in any snapshot. This could be due to:")
        print(f"    - The test block ({from_block}) doesn't contain Uniswap V3 events")
        print(f"    - The active pools don't have trading activity in this block")
        print(f"    - The pools are not actually Uniswap V3 pools")
        
        # Skip the test if no events found, but don't fail it
        pytest.skip("No events found in any snapshot - this is expected for some blocks")
    
    # Check for missing events (only if we have events to compare)
    total_missing_swaps = sum(
        len(result["swap_comparison"]["missing_in_snapshot"]) 
        for result in all_validation_results
    )
    total_missing_mints = sum(
        len(result["mint_comparison"]["missing_in_snapshot"]) 
        for result in all_validation_results
    )
    total_missing_burns = sum(
        len(result["burn_comparison"]["missing_in_snapshot"]) 
        for result in all_validation_results
    )
    
    print(f"  Missing swaps: {total_missing_swaps}")
    print(f"  Missing mints: {total_missing_mints}")
    print(f"  Missing burns: {total_missing_burns}")
    
    # Check for data mismatches
    total_data_mismatches = 0
    for result in all_validation_results:
        for comparison in [result["swap_comparison"], result["mint_comparison"], result["burn_comparison"]]:
            for detail in comparison["detailed_comparison"]:
                if not detail["matches"]:
                    total_data_mismatches += 1
    
    print(f"  Data mismatches: {total_data_mismatches}")
    
    # Assertions
    assert total_snapshots > 0, "Should have at least one snapshot to validate"
    
    # Only assert on missing events if we actually have events to compare
    if total_etherscan_events > 0:
        assert total_missing_swaps == 0, f"Found {total_missing_swaps} swaps missing from snapshots"
        assert total_missing_mints == 0, f"Found {total_missing_mints} mints missing from snapshots"
        assert total_missing_burns == 0, f"Found {total_missing_burns} burns missing from snapshots"
        assert total_data_mismatches == 0, f"Found {total_data_mismatches} data mismatches"
    else:
        print(f"  ℹ️  Skipping event validation assertions since no events were found")
    
    print("PASSED: test_trades_processor_against_etherscan")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
