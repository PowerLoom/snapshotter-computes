import asyncio
import json
import time
import os
from typing import Optional, List, Dict, Tuple
from web3 import Web3
from web3._utils.events import get_event_data
from eth_abi.codec import ABICodec
from eth_abi.registry import registry as default_abi_registry
import aiohttp
import pytest

from computes.pair_total_reserves import PairTotalReservesProcessor
from computes.utils.models.message_models import UniswapPoolMetadata, UniswapBaseSnapshot
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.settings.config import settings
from computes.settings.config import settings as compute_settings


async def get_active_pools_from_redis(redis_conn, block_number: int, namespace: str) -> List[str]:
    """Get active pools from Redis for a specific block (read-only)."""
    key = f"active_pools:{block_number}:{namespace}"
    pools = await redis_conn.smembers(key)
    return [pool.decode('utf-8') for pool in pools]


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


async def fetch_trade_events_from_etherscan(
    pool_address: str,
    block_number: int,
    pool_metadata: UniswapPoolMetadata,
    redis_conn,
    protocol_state_contract,
    anchor_rpc_helper
) -> Dict[str, any]:
    """
    Fetch trade events from Etherscan and calculate raw token amounts.
    
    Returns:
        Dict with trade metrics: {
            'total_swap_token0_amount': float,
            'total_swap_token1_amount': float,
            'total_mint_burn_token0_amount': float,
            'total_mint_burn_token1_amount': float,
            'swap_count': int,
            'mint_count': int,
            'burn_count': int,
            'events_details': List[Dict]
        }
    """
    api_key, api_url = get_etherscan_config()
    if not api_key or not api_url:
        return {}  # Return empty dict if missing config
    
    # Get source chain ID for Etherscan v2 API (read-only, don't cache in Redis for tests)
    try:
        from snapshotter.utils.redis.redis_keys import source_chain_id_key
        source_chain_id_data = await redis_conn.get(source_chain_id_key())
        
        if source_chain_id_data:
            source_chain_id = int(source_chain_id_data.decode('utf-8'))
            print(f"      ℹ️  Using cached source chain ID: {source_chain_id}")
        else:
            # If not in cache, fetch from blockchain but don't cache (test mode)
            [source_chain_id] = await anchor_rpc_helper.web3_call(
                tasks=[
                    ('SOURCE_CHAIN_ID', [Web3.to_checksum_address(settings.data_market)]),
                ],
                contract_addr=protocol_state_contract.address,
                abi=protocol_state_contract.abi,
            )
            print(f"      ℹ️  Fetched source chain ID from contract: {source_chain_id}")
    except Exception as e:
        print(f"      ⚠️  Could not get source chain ID: {e}")
        print(f"      ⚠️  Defaulting to Ethereum mainnet (chain ID 1) for Etherscan API")
        return {}
    
    # Uniswap V3 event signatures
    swap_topic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    mint_topic = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"  
    burn_topic = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    
    try:
        pool_address = Web3.to_checksum_address(pool_address)
    except Exception:
        return {}

    # Load Uniswap V3 Pool ABI for event decoding
    try:
        with open("computes/static/abis/UniswapV3Pool.json", 'r') as f:
            pool_abi = json.load(f)
        
        codec = ABICodec(default_abi_registry)
        
        # Find event ABIs
        event_abis = {}
        for abi_item in pool_abi:
            if abi_item.get('type') == 'event':
                event_name = abi_item.get('name')
                if event_name in ['Swap', 'Mint', 'Burn']:
                    event_abis[event_name] = abi_item
        
    except Exception:
        return {}
    
    trade_metrics = {
        'total_swap_token0_amount': 0.0,
        'total_swap_token1_amount': 0.0,
        'total_mint_burn_token0_amount': 0.0,
        'total_mint_burn_token1_amount': 0.0,
        'swap_count': 0,
        'mint_count': 0,
        'burn_count': 0,
        'events_details': []
    }
    
    # Fetch all events in a single API call (no topic0 filter)
    url = api_url
    params = {
        "module": "logs",
        "action": "getLogs",
        "address": pool_address,
        "fromBlock": block_number,
        "toBlock": block_number,
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
                    print(f"      ⚠️  Etherscan request failed with status {response.status}")
                    return {}
                
                data = await response.json()
                
                if data.get("status") != "1":
                    error_message = data.get('message', 'Unknown error')
                    result = data.get('result', '')
                    if "No records found" not in error_message:
                        print(f"      ⚠️  Etherscan API error: {error_message}")
                        if result and result != error_message:
                            print(f"      ⚠️  API result: {result}")
                    return {}
                
                logs = data.get("result", [])
                print(f"      📋 Etherscan returned {len(logs)} total events for block {block_number}")
                
                for log in logs:
                    try:
                        # Determine event type from topic (filter to only Uniswap V3 events)
                        event_name = None
                        if len(log.get('topics', [])) > 0:
                            topic0 = log['topics'][0]
                            if topic0 == swap_topic:
                                event_name = "Swap"
                            elif topic0 == mint_topic:
                                event_name = "Mint"
                            elif topic0 == burn_topic:
                                event_name = "Burn"
                        
                        if not event_name:
                            continue  # Skip non-Uniswap V3 events
                            
                        event_abi = event_abis.get(event_name)
                        if not event_abi:
                            continue  # Skip if no ABI found

                        decoded_event = get_event_data(codec, event_abi, log)
                        amount0 = decoded_event['args'].get('amount0', 0)
                        amount1 = decoded_event['args'].get('amount1', 0)
                        
                        # Convert to token units (normalized by decimals)
                        token0_amount = abs(amount0) / (10 ** int(pool_metadata.token0.decimals))
                        token1_amount = abs(amount1) / (10 ** int(pool_metadata.token1.decimals))
                        
                        # Store event details for debugging
                        event_detail = {
                            'event_type': event_name,
                            'tx_hash': log['transactionHash'],
                            'log_index': int(log['logIndex'], 16),
                            'token0_amount': token0_amount,
                            'token1_amount': token1_amount,
                            'amount0_raw': amount0,
                            'amount1_raw': amount1
                        }
                        trade_metrics['events_details'].append(event_detail)
                        
                        # Accumulate token amounts by event type
                        if event_name == "Swap":
                            trade_metrics['swap_count'] += 1
                            trade_metrics['total_swap_token0_amount'] += token0_amount
                            trade_metrics['total_swap_token1_amount'] += token1_amount
                        
                        elif event_name in ["Mint", "Burn"]:
                            if event_name == "Mint":
                                trade_metrics['mint_count'] += 1
                            else:
                                trade_metrics['burn_count'] += 1
                            
                            trade_metrics['total_mint_burn_token0_amount'] += token0_amount
                            trade_metrics['total_mint_burn_token1_amount'] += token1_amount
                        
                    except Exception as e:
                        print(f"      ⚠️  Error decoding event {log.get('transactionHash', 'unknown')}: {e}")
                        continue  # Skip malformed events
                        
    except Exception as e:
        print(f"      ⚠️  Error fetching events from Etherscan: {e}")
        return {}
    
    return trade_metrics


async def validate_trade_data_against_etherscan(
    snapshot: UniswapBaseSnapshot,
    pool_metadata: UniswapPoolMetadata,
    block_number: int,
    redis_conn,
    protocol_state_contract,
    anchor_rpc_helper
) -> Dict[str, any]:
    """
    Validate snapshot trade data against Etherscan by comparing raw token amounts.
    
    Returns validation results with comparison data.
    """
    etherscan_data = await fetch_trade_events_from_etherscan(
        snapshot.address, block_number, pool_metadata, redis_conn, protocol_state_contract, anchor_rpc_helper
    )
    
    if not etherscan_data:
        return {
            'etherscan_available': False,
            'reason': 'No Etherscan API key/URL configured or fetch failed'
        }
    
    # Extract snapshot trade data (USD values for reference only)
    snapshot_swap_volume_usd = snapshot.totalTrade
    snapshot_fees_usd = snapshot.totalFee
    snapshot_mint_burn_volume_usd = getattr(snapshot, 'totalTradeMintBurn', 0)
    
    # Extract snapshot raw token amounts - first log what we actually get
    token0_trade_vol = getattr(snapshot, 'token0TradeVolume', None)
    token1_trade_vol = getattr(snapshot, 'token1TradeVolume', None)
    token0_mb_vol = getattr(snapshot, 'token0MintBurnVolume', None)
    token1_mb_vol = getattr(snapshot, 'token1MintBurnVolume', None)
    
    print(f"      🔍 Snapshot token volumes:")
    print(f"        token0TradeVolume: {type(token0_trade_vol)} = {token0_trade_vol}")
    print(f"        token1TradeVolume: {type(token1_trade_vol)} = {token1_trade_vol}")
    print(f"        token0MintBurnVolume: {type(token0_mb_vol)} = {token0_mb_vol}")
    print(f"        token1MintBurnVolume: {type(token1_mb_vol)} = {token1_mb_vol}")
    
    if token0_trade_vol:
        snapshot_swap_token0_amount = float(token0_trade_vol)
    else:
        snapshot_swap_token0_amount = 0.0
    
    if token1_trade_vol:
        snapshot_swap_token1_amount = float(token1_trade_vol)
    else:
        snapshot_swap_token1_amount = 0.0
    
    if token0_mb_vol:
        snapshot_mint_burn_token0_amount = float(token0_mb_vol)
    else:
        snapshot_mint_burn_token0_amount = 0.0
    
    if token1_mb_vol:
        snapshot_mint_burn_token1_amount = float(token1_mb_vol)
    else:
        snapshot_mint_burn_token1_amount = 0.0
    
    # Compare with etherscan data (focus on raw token amounts, not USD)
    validation_result = {
        'etherscan_available': True,
        'pool_address': snapshot.address,
        'block_number': block_number,
        'comparison': {
            'swap_volume_token0': {
                'snapshot': snapshot_swap_token0_amount,
                'etherscan': etherscan_data['total_swap_token0_amount'],
                'count_etherscan': etherscan_data['swap_count']
            },
            'swap_volume_token1': {
                'snapshot': snapshot_swap_token1_amount,
                'etherscan': etherscan_data['total_swap_token1_amount'],
                'count_etherscan': etherscan_data['swap_count']
            },
            'mint_burn_volume_token0': {
                'snapshot': snapshot_mint_burn_token0_amount,
                'etherscan': etherscan_data['total_mint_burn_token0_amount'],
                'mint_count_etherscan': etherscan_data['mint_count'],
                'burn_count_etherscan': etherscan_data['burn_count']
            },
            'mint_burn_volume_token1': {
                'snapshot': snapshot_mint_burn_token1_amount,
                'etherscan': etherscan_data['total_mint_burn_token1_amount'],
                'mint_count_etherscan': etherscan_data['mint_count'],
                'burn_count_etherscan': etherscan_data['burn_count']
            },
            # Include USD values from snapshot for reference only
            'usd_values_reference': {
                'snapshot_swap_volume_usd': snapshot_swap_volume_usd,
                'snapshot_fees_usd': snapshot_fees_usd,
                'snapshot_mint_burn_volume_usd': snapshot_mint_burn_volume_usd
            },
            'events_details': etherscan_data.get('events_details', [])
        }
    }

    # Log transaction details BEFORE any potential assertion failures
    events_details = etherscan_data.get('events_details', [])
    if events_details:
        print(f"     📋 Etherscan found {len(events_details)} events in block {block_number}:")
        for i, event in enumerate(events_details):
            tx_hash = event['tx_hash']
            event_type = event['event_type']
            token0_amt = event['token0_amount']
            token1_amt = event['token1_amount']
            log_idx = event['log_index']
            print(f"       Event {i+1}: {event_type} - Tx: {tx_hash} (LogIdx: {log_idx})")
            print(f"         Token0: {token0_amt:.6f}, Token1: {token1_amt:.6f}")

    # Show comparison results before any potential assertion failures
    print(f"     📊 Trade Volume Comparison:")
    print(f"       Token0 Swap - Snapshot: {snapshot_swap_token0_amount:.6f}, Etherscan: {etherscan_data['total_swap_token0_amount']:.6f} ({etherscan_data['swap_count']} swaps)")
    print(f"       Token1 Swap - Snapshot: {snapshot_swap_token1_amount:.6f}, Etherscan: {etherscan_data['total_swap_token1_amount']:.6f}")
    print(f"       Token0 Mint/Burn - Snapshot: {snapshot_mint_burn_token0_amount:.6f}, Etherscan: {etherscan_data['total_mint_burn_token0_amount']:.6f} ({etherscan_data['mint_count']} mints, {etherscan_data['burn_count']} burns)")
    print(f"       Token1 Mint/Burn - Snapshot: {snapshot_mint_burn_token1_amount:.6f}, Etherscan: {etherscan_data['total_mint_burn_token1_amount']:.6f}")

    # Add assertions to ensure test fails on discrepancies
    tolerance = 0.01  # 1% tolerance for floating point precision
    print(f"     🔧 Validating with {tolerance:.1%} tolerance...")
    
    # Check swap volume discrepancies
    if etherscan_data['swap_count'] > 0:
        for token_name, snapshot_vol, etherscan_vol in [
            ('Token0', snapshot_swap_token0_amount, etherscan_data['total_swap_token0_amount']),
            ('Token1', snapshot_swap_token1_amount, etherscan_data['total_swap_token1_amount'])
        ]:
            if etherscan_vol > 0:
                relative_diff = abs(snapshot_vol - etherscan_vol) / etherscan_vol
                assert relative_diff <= tolerance, \
                    f"{token_name} swap volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol} " \
                    f"(relative diff: {relative_diff:.2%}, tolerance: {tolerance:.2%})"
            elif snapshot_vol != 0:
                assert False, f"{token_name} swap volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol}"
    
    # Check mint/burn volume discrepancies  
    if etherscan_data['mint_count'] > 0 or etherscan_data['burn_count'] > 0:
        for token_name, snapshot_vol, etherscan_vol in [
            ('Token0', snapshot_mint_burn_token0_amount, etherscan_data['total_mint_burn_token0_amount']),
            ('Token1', snapshot_mint_burn_token1_amount, etherscan_data['total_mint_burn_token1_amount'])
        ]:
            if etherscan_vol > 0:
                relative_diff = abs(snapshot_vol - etherscan_vol) / etherscan_vol
                assert relative_diff <= tolerance, \
                    f"{token_name} mint/burn volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol} " \
                    f"(relative diff: {relative_diff:.2%}, tolerance: {tolerance:.2%})"
            elif snapshot_vol != 0:
                assert False, f"{token_name} mint/burn volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol}"
    
    return validation_result


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
    pool_address = Web3.to_checksum_address(compute_settings.contract_addresses.USDC_WETH_PAIR)

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
        
        # Validate trade data against Etherscan if API key is available
        print(f"  🔍 Validating trade data against Etherscan...")
        from computes.metadata import MetadataProcessor
        metadata_processor = MetadataProcessor()
        
        pool_metadata = await metadata_processor.get_pool_metadata(
            pool_address=snapshot.address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )
        
        if pool_metadata:
            trade_validation = await validate_trade_data_against_etherscan(
                snapshot, pool_metadata, from_block, redis_conn, protocol_state_contract, anchor_rpc_helper
            )
            
            if trade_validation.get('etherscan_available'):
                comparison = trade_validation['comparison']
                
                # Token0 swap volume comparison
                snap_swap_t0 = comparison['swap_volume_token0']['snapshot']
                eth_swap_t0 = comparison['swap_volume_token0']['etherscan']
                swap_count = comparison['swap_volume_token0']['count_etherscan']
                
                # Token1 swap volume comparison
                snap_swap_t1 = comparison['swap_volume_token1']['snapshot']
                eth_swap_t1 = comparison['swap_volume_token1']['etherscan']
                
                # Token0 mint/burn comparison
                snap_mb_t0 = comparison['mint_burn_volume_token0']['snapshot']
                eth_mb_t0 = comparison['mint_burn_volume_token0']['etherscan']
                mint_count = comparison['mint_burn_volume_token0']['mint_count_etherscan']
                burn_count = comparison['mint_burn_volume_token0']['burn_count_etherscan']
                
                # Token1 mint/burn comparison
                snap_mb_t1 = comparison['mint_burn_volume_token1']['snapshot']
                eth_mb_t1 = comparison['mint_burn_volume_token1']['etherscan']
                
                # Log USD values from snapshot for reference only
                usd_ref = comparison['usd_values_reference']
                print(f"     💰 USD Values from Snapshot (Reference Only):")
                print(f"       Swap Volume USD: ${usd_ref['snapshot_swap_volume_usd']:.2f}")
                print(f"       Fees USD: ${usd_ref['snapshot_fees_usd']:.2f}")
                print(f"       Mint/Burn Volume USD: ${usd_ref['snapshot_mint_burn_volume_usd']:.2f}")
                
                # Validation using raw token amounts with reasonable tolerance
                total_events = swap_count + mint_count + burn_count
                if total_events > 0:
                    # Token0 swap validation
                    if snap_swap_t0 > 0.000001 and eth_swap_t0 > 0.000001:  # Only validate if both have meaningful amounts
                        token0_swap_ratio = min(snap_swap_t0, eth_swap_t0) / max(snap_swap_t0, eth_swap_t0)
                        if token0_swap_ratio < 0.5:  # More than 2x difference
                            print(f"       ⚠️  Significant discrepancy in Token0 swap volumes (ratio: {token0_swap_ratio:.3f})")
                    
                    # Token1 swap validation
                    if snap_swap_t1 > 0.000001 and eth_swap_t1 > 0.000001:
                        token1_swap_ratio = min(snap_swap_t1, eth_swap_t1) / max(snap_swap_t1, eth_swap_t1)
                        if token1_swap_ratio < 0.5:  # More than 2x difference
                            print(f"       ⚠️  Significant discrepancy in Token1 swap volumes (ratio: {token1_swap_ratio:.3f})")
                    
                    # Token0 mint/burn validation
                    if snap_mb_t0 > 0.000001 and eth_mb_t0 > 0.000001:
                        token0_mb_ratio = min(snap_mb_t0, eth_mb_t0) / max(snap_mb_t0, eth_mb_t0)
                        if token0_mb_ratio < 0.5:
                            print(f"       ⚠️  Significant discrepancy in Token0 mint/burn volumes (ratio: {token0_mb_ratio:.3f})")
                    
                    # Token1 mint/burn validation
                    if snap_mb_t1 > 0.000001 and eth_mb_t1 > 0.000001:
                        token1_mb_ratio = min(snap_mb_t1, eth_mb_t1) / max(snap_mb_t1, eth_mb_t1)
                        if token1_mb_ratio < 0.5:
                            print(f"       ⚠️  Significant discrepancy in Token1 mint/burn volumes (ratio: {token1_mb_ratio:.3f})")
                            
                    # Show detailed event information if available
                    events_details = comparison.get('events_details', [])
                    if events_details:
                        print(f"     🔍 Etherscan Events Details ({len(events_details)} events):")
                        
                        # Check for any discrepancies to determine if we should show more details
                        has_discrepancy = (
                            (abs(snap_swap_t0 - eth_swap_t0) > 0.000001) or
                            (abs(snap_swap_t1 - eth_swap_t1) > 0.000001) or
                            (abs(snap_mb_t0 - eth_mb_t0) > 0.000001) or
                            (abs(snap_mb_t1 - eth_mb_t1) > 0.000001)
                        )
                        
                        # Show all events if there's a discrepancy, otherwise just the first few
                        events_to_show = events_details if has_discrepancy else events_details[:3]
                        
                        for i, event in enumerate(events_to_show):
                            tx_hash = event['tx_hash']
                            event_type = event['event_type']
                            token0_amt = event['token0_amount']
                            token1_amt = event['token1_amount']
                            log_idx = event['log_index']
                            print(f"       Event {i+1}: {event_type} - Tx: {tx_hash} (LogIdx: {log_idx})")
                            print(f"         Token0: {token0_amt:.6f}, Token1: {token1_amt:.6f}")
                            
                        if not has_discrepancy and len(events_details) > 3:
                            print(f"       ... and {len(events_details) - 3} more events")
                        elif has_discrepancy:
                            print(f"     ⚠️  DISCREPANCY DETECTED - Showing all {len(events_details)} Etherscan events above")
                else:
                    print(f"       ℹ️  No trades found in this block")
            else:
                reason = trade_validation.get('reason', 'Unknown')
                print(f"     ℹ️  Etherscan validation skipped: {reason}")

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