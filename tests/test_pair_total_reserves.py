import json
import time
import os
import warnings
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from web3 import Web3
from web3._utils.events import get_event_data
from eth_abi.codec import ABICodec
from eth_abi.registry import registry as default_abi_registry
from rpc_helper.rpc import RpcHelper
import aiohttp
import pytest

from computes.pair_total_reserves import PairTotalReservesProcessor
from computes.utils.core import base_snapshot_from_block_range, get_block_details_in_block_range
from computes.utils.models.message_models import UniswapPoolMetadata, UniswapBaseSnapshot
from computes.utils.helpers import calculate_reserves, get_pool_metadata

from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import source_chain_id_key
from snapshotter.settings.config import settings


def sqrtPriceX96ToTokenPrices(sqrtPriceX96, token0_decimals, token1_decimals):
    # https://blog.uniswap.org/uniswap-v3-math-primer

    price0 = ((sqrtPriceX96 / (2**96))** 2) / (10 ** token1_decimals / 10 ** token0_decimals)
    price1 = 1 / price0

    price0 = round(price0, token0_decimals)
    price1 = round(price1, token1_decimals)

    return price0, price1


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
    source_chain_id: int,
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
                            print(f"      ⚠️  No event name found for log {log}")
                            continue  # Skip non-Uniswap V3 events
                            
                        event_abi = event_abis.get(event_name)
                        if not event_abi:
                            print(f"      ⚠️  No ABI found for event {event_name}")
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
    source_chain_id: int,
) -> Dict[str, any]:
    """
    Validate snapshot trade data against Etherscan by comparing raw token amounts.
    
    Returns validation results with comparison data and any validation errors.
    """
    etherscan_data = await fetch_trade_events_from_etherscan(
        snapshot.address, block_number, pool_metadata, source_chain_id
    )
    
    if not etherscan_data:
        return {
            'etherscan_available': False,
            'reason': 'No Etherscan API key/URL configured or fetch failed',
            'validation_errors': []
        }
    
    # Extract snapshot raw token amounts - first log what we actually get
    token0_trade_vol = getattr(snapshot, 'token0TradeVolume', None)
    token1_trade_vol = getattr(snapshot, 'token1TradeVolume', None)
    token0_mb_vol = getattr(snapshot, 'token0MintBurnVolume', None)
    token1_mb_vol = getattr(snapshot, 'token1MintBurnVolume', None)
    
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
        'validation_errors': [],  # Collect errors instead of asserting
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

    # Collect validation errors instead of asserting immediately
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
                if relative_diff > tolerance:
                    error_msg = f"{token_name} swap volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol} (relative diff: {relative_diff:.2%}, tolerance: {tolerance:.2%})"
                    print(f"       ❌ VALIDATION ERROR: {error_msg}")
                    validation_result['validation_errors'].append(error_msg)
            elif snapshot_vol != 0:
                error_msg = f"{token_name} swap volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol}"
                print(f"       ❌ VALIDATION ERROR: {error_msg}")
                validation_result['validation_errors'].append(error_msg)
    
    # Check mint/burn volume discrepancies  
    if etherscan_data['mint_count'] > 0 or etherscan_data['burn_count'] > 0:
        for token_name, snapshot_vol, etherscan_vol in [
            ('Token0', snapshot_mint_burn_token0_amount, etherscan_data['total_mint_burn_token0_amount']),
            ('Token1', snapshot_mint_burn_token1_amount, etherscan_data['total_mint_burn_token1_amount'])
        ]:
            if etherscan_vol > 0:
                relative_diff = abs(snapshot_vol - etherscan_vol) / etherscan_vol
                if relative_diff > tolerance:
                    error_msg = f"{token_name} mint/burn volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol} (relative diff: {relative_diff:.2%}, tolerance: {tolerance:.2%})"
                    print(f"       ❌ VALIDATION ERROR: {error_msg}")
                    validation_result['validation_errors'].append(error_msg)
                else:
                    print(f"       ✅ {token_name} mint/burn volume validation passed")
            elif snapshot_vol != 0:
                error_msg = f"{token_name} mint/burn volume mismatch: snapshot={snapshot_vol}, etherscan={etherscan_vol}"
                print(f"       ❌ VALIDATION ERROR: {error_msg}")
                validation_result['validation_errors'].append(error_msg)
            else:
                print(f"       ✅ {token_name} mint/burn volume validation passed (both zero)")
    else:
        print(f"       ℹ️  No mint/burn events found - skipping mint/burn validation")
    
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

def get_coinmarketcap_config():
    """Get CoinMarketCap configuration from environment variables."""
    api_key = os.getenv('COINMARKETCAP_API_KEY')
    api_url = os.getenv('COINMARKETCAP_API_URL', 'https://pro-api.coinmarketcap.com')
    
    if not api_key:
        return None, None  # Skip CMC validation if missing API key
    
    return api_key, api_url


def emit_pytest_warnings(warning_messages: List[str], test_name: str = "price_validation") -> None:
    """
    Emit pytest warnings that will appear in the final test output.
    
    Args:
        warning_messages: List of warning messages to emit
        test_name: Name of the test for context
    """
    for warning_msg in warning_messages:
        warnings.warn(f"[{test_name}] {warning_msg}", UserWarning, stacklevel=2)


def validate_price_differences(
    snapshot_token0_usd: float,
    snapshot_token1_usd: float,
    cmc_token0_usd: float,
    cmc_token1_usd: float,
    pool_metadata: UniswapPoolMetadata,
    data_is_fresh: bool,
    tolerance: float
) -> Tuple[List[str], List[str]]:
    """
    Validate price differences between snapshot and CoinMarketCap data.
    
    Args:
        snapshot_token0_usd: Token0 USD price from snapshot
        snapshot_token1_usd: Token1 USD price from snapshot
        cmc_token0_usd: Token0 USD price from CoinMarketCap
        cmc_token1_usd: Token1 USD price from CoinMarketCap
        pool_metadata: Pool metadata containing token symbols
        data_is_fresh: Whether the CMC data is fresh (within time threshold)
        tolerance: Tolerance level for price differences (e.g., 0.01 for 1%)
        
    Returns:
        Tuple of (validation_warnings, validation_errors) for stale/fresh data that exceeds tolerance
    """
    validation_warnings = []
    validation_errors = []
    
    # Log comparison results and validate Token0
    print(f"       Token0 ({pool_metadata.token0.symbol}) USD Price:")
    print(f"         Snapshot: ${snapshot_token0_usd:.6f}")
    print(f"         CMC: ${cmc_token0_usd:.6f}")
    
    if cmc_token0_usd > 0 and snapshot_token0_usd > 0:
        token0_diff_pct = abs(snapshot_token0_usd - cmc_token0_usd) / cmc_token0_usd * 100
        print(f"         Difference: {token0_diff_pct:.3f}%")
        
        # Validate with tolerance
        if token0_diff_pct / 100 > tolerance:
            print(f"         ⚠️  Price difference ({token0_diff_pct:.3f}%) exceeds tolerance ({tolerance:.1%})")
    
    # Log comparison results and validate Token1
    print(f"       Token1 ({pool_metadata.token1.symbol}) USD Price:")
    print(f"         Snapshot: ${snapshot_token1_usd:.6f}")
    print(f"         CMC: ${cmc_token1_usd:.6f}")
    
    if cmc_token1_usd > 0 and snapshot_token1_usd > 0:
        token1_diff_pct = abs(snapshot_token1_usd - cmc_token1_usd) / cmc_token1_usd * 100
        print(f"         Difference: {token1_diff_pct:.3f}%")
        
        # Validate with tolerance
        if token1_diff_pct / 100 > tolerance:
            print(f"         ⚠️  Price difference ({token1_diff_pct:.3f}%) exceeds tolerance ({tolerance:.1%})")
    
    # Collect validation errors/warnings instead of asserting immediately
    if cmc_token0_usd > 0 and snapshot_token0_usd > 0:
        token0_usd_diff = abs(snapshot_token0_usd - cmc_token0_usd) / cmc_token0_usd
        if data_is_fresh:
            if token0_usd_diff > tolerance:
                error_msg = f"Token0 USD price mismatch: snapshot=${snapshot_token0_usd:.6f}, cmc=${cmc_token0_usd:.6f} (relative diff: {token0_usd_diff:.2%}, tolerance: {tolerance:.2%})"
                print(f"         ❌ VALIDATION ERROR: {error_msg}")
                validation_errors.append(error_msg)
        else:
            if token0_usd_diff > tolerance:
                warning_msg = f"Token0 USD price difference ({token0_usd_diff:.2%}) exceeds tolerance but data is stale"
                print(f"         ⚠️  WARNING: {warning_msg}")
                validation_warnings.append(warning_msg)
    
    if cmc_token1_usd > 0 and snapshot_token1_usd > 0:
        token1_usd_diff = abs(snapshot_token1_usd - cmc_token1_usd) / cmc_token1_usd
        if data_is_fresh:
            if token1_usd_diff > tolerance:
                error_msg = f"Token1 USD price mismatch: snapshot=${snapshot_token1_usd:.6f}, cmc=${cmc_token1_usd:.6f} (relative diff: {token1_usd_diff:.2%}, tolerance: {tolerance:.2%})"
                print(f"         ❌ VALIDATION ERROR: {error_msg}")
                validation_errors.append(error_msg)
        else:
            if token1_usd_diff > tolerance:
                warning_msg = f"Token1 USD price difference ({token1_usd_diff:.2%}) exceeds tolerance but data is stale"
                print(f"         ⚠️  WARNING: {warning_msg}")
                validation_warnings.append(warning_msg)
    
    return validation_warnings, validation_errors


async def validate_prices_against_coinmarketcap(
    snapshot: UniswapBaseSnapshot,
    pool_metadata: UniswapPoolMetadata,
    block_number: int,
    source_chain_id: int,
    tolerance: float = 0.01
) -> Dict[str, any]:
    """
    Validate snapshot prices against CoinMarketCap's DEX historical OHLCV API.
    
    Args:
        snapshot: The UniswapBaseSnapshot to validate
        pool_metadata: Pool metadata containing token info
        block_number: Block number for validation
        source_chain_id: Chain ID for network selection
        
    Returns:
        Validation results with comparison data
    """
    # Initialize warnings list
    warnings = []
    
    api_key, api_url = get_coinmarketcap_config()
    if not api_key or not api_url:
        return {
            'cmc_available': False,
            'reason': 'No CoinMarketCap API key configured'
        }
    
    # Get pool address in checksum format
    pool_address = Web3.to_checksum_address(snapshot.address)
    
    # Get block timestamp from snapshot
    block_timestamp = snapshot.timestamps.get(block_number)
    if not block_timestamp:
        return {
            'cmc_available': False,
            'reason': f'No timestamp found for block {block_number}'
        }
    
    # Convert block timestamp to ISO format for CMC API
    block_time = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)
    
    # Construct CMC API URL for latest quotes (reverting from historical)
    endpoint = f"{api_url}/v4/dex/pairs/quotes/latest"

    # Network slug mapping (case sensitive)
    slugs = {
        1: 'ethereum',  # Ethereum mainnet
        137: 'polygon-pos',  # Polygon PoS
        8453: 'base',  # Base
        42161: 'arbitrum-one',  # Arbitrum One
        10: 'optimistic-ethereum',  # Optimism
    }
    
    network_slug = slugs.get(source_chain_id)
    if not network_slug:
        print(f"      ⚠️  Unsupported chain ID {source_chain_id}, defaulting to ethereum")
        network_slug = 'ethereum'
    
    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accept': 'application/json'
    }
    
    params = {
        'contract_address': pool_address,
        'network_slug': network_slug,
        'aux': '',
        'skip_invalid': 'true'
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint, params=params, headers=headers, timeout=30) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"      ⚠️  CMC request failed with status {response.status}")
                print(f"      ⚠️  Error response: {error_text}")
                return {
                    'cmc_available': False,
                    'reason': f'API request failed: {response.status} - {error_text}'
                }
            
            data = await response.json()
            
            if 'data' not in data:
                print(f"      ⚠️  Unexpected CMC response format: {data}")
                return {
                    'cmc_available': False,
                    'reason': 'Invalid API response format'
                }
            
            # Data comes as a list in the 'data' field
            pair_data = data['data'][0] if data['data'] else None
            if not pair_data:
                return {
                    'cmc_available': False,
                    'reason': 'No data returned for pool address'
                }
            
            # Get the first quote (should be USD with convert_id '2781')
            quote = pair_data['quote'][0] if pair_data.get('quote') else {}
            if not quote:
                return {
                    'cmc_available': False,
                    'reason': 'No quote data available'
                }
            
            # Parse CMC's last_updated timestamp (ISO format with timezone)
            last_updated_str = quote.get('last_updated')
            if last_updated_str:
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                last_updated_ts = int(last_updated.timestamp())
                
                # Calculate time difference in seconds
                time_diff = abs(block_timestamp - last_updated_ts)
                
                # Warn if difference is more than 2 minutes (120 seconds)
                if time_diff > 120:
                    print(f"      ⚠️  WARNING: CMC data is {time_diff} seconds ({time_diff/60:.1f} minutes) stale")
                    warnings.append(f"CMC data for block {block_number} is stale (difference: {time_diff} seconds)")
            else:
                print(f"      ⚠️  WARNING: No last_updated timestamp in CMC response")
                print(f"         Cannot verify data freshness, continuing with validation")
                last_updated_ts = None
                time_diff = None
                warnings.append(f"No last_updated timestamp found for block {block_number} in CMC response")
            
            # Determine which token is the base asset
            base_asset_address = pair_data.get('base_asset_contract_address', '').lower()
            is_token0_base = base_asset_address == pool_metadata.token0.address.lower()
            
            # Get snapshot USD prices for comparison
            snapshot_token0_usd = snapshot.token0PricesUSD.get(block_number, 0)
            snapshot_token1_usd = snapshot.token1PricesUSD.get(block_number, 0)
            
            # Extract CMC price data
            # price: Base asset price in USD
            # price_by_quote_asset: Raw token price (base/quote ratio)
            cmc_base_usd = quote.get('price', 0)  # Base token USD price
            cmc_raw_price = quote.get('price_by_quote_asset', 0)  # Raw token price ratio
            
            # Map CMC prices to token0/token1 format with high precision
            from decimal import Decimal, getcontext
            getcontext().prec = 50  # High precision for calculations
            
            cmc_raw_price_decimal = Decimal(str(cmc_raw_price))
            cmc_base_usd_decimal = Decimal(str(cmc_base_usd))
            
            if is_token0_base:
                # If token0 is base:
                cmc_token0_usd = float(cmc_base_usd_decimal)
                cmc_token1_usd = float(cmc_base_usd_decimal / cmc_raw_price_decimal) if cmc_raw_price_decimal else 0
            else:
                # If token1 is base:
                cmc_token0_usd = float(cmc_base_usd_decimal * cmc_raw_price_decimal) if cmc_raw_price_decimal else 0
                cmc_token1_usd = float(cmc_base_usd_decimal)
            
            validation_result = {
                'cmc_available': True,
                'pool_address': pool_address,
                'block_number': block_number,
                'warnings': warnings,
                'validation_errors': [],  # Will be populated by validate_price_differences
                'timestamp_validation': {
                    'block_timestamp': block_timestamp,
                    'cmc_last_updated': last_updated_ts,
                    'time_difference_seconds': time_diff,
                    'is_within_threshold': time_diff <= 120 if time_diff else None
                },
                'comparison': {
                    'usd_prices': {
                        'token0': {
                            'snapshot': snapshot_token0_usd,
                            'cmc': cmc_token0_usd
                        },
                        'token1': {
                            'snapshot': snapshot_token1_usd,
                            'cmc': cmc_token1_usd
                        }
                    },
                    'metadata': {
                        'base_token': pair_data.get('base_asset_symbol'),
                        'quote_token': pair_data.get('quote_asset_symbol'),
                        'dex': pair_data.get('dex_slug'),
                        'network': pair_data.get('network_slug'),
                        'market_data': {
                            'price': quote.get('price', 0),
                            'price_by_quote_asset': quote.get('price_by_quote_asset', 0),
                            'liquidity': quote.get('liquidity', 0),
                            'volume_24h': quote.get('volume_24h', 0),
                            'percent_change_24h': quote.get('percent_change_price_24h', 0)
                        }
                    }
                }
            }
            
            # Log comparison results
            print(f"     📊 CoinMarketCap Price Validation:")
            print(f"       Pool: {pair_data.get('base_asset_symbol')}/{pair_data.get('quote_asset_symbol')} on {pair_data.get('dex_slug')}")
            print(f"       Market Data: Price=${quote.get('price', 0):.2f}, Liquidity=${quote.get('liquidity', 0):,.2f}")
            print(f"       24h Volume: ${quote.get('volume_24h', 0):,.2f}, 24h Change: {quote.get('percent_change_price_24h', 0):.2f}%")
            
            price_validation_warnings, price_validation_errors = validate_price_differences(
                snapshot_token0_usd=snapshot_token0_usd,
                snapshot_token1_usd=snapshot_token1_usd,
                cmc_token0_usd=cmc_token0_usd,
                cmc_token1_usd=cmc_token1_usd,
                pool_metadata=pool_metadata,
                data_is_fresh=time_diff is not None and time_diff <= 120,
                tolerance=tolerance
            )
            warnings.extend(price_validation_warnings)
            
            # Add validation errors to the result
            validation_result['validation_errors'] = price_validation_errors
            
            return validation_result


async def validate_raw_token_prices_from_onchain_data(
    snapshot: UniswapBaseSnapshot,
    pool_metadata: UniswapPoolMetadata,
    block_number: int,
    rpc_helper: RpcHelper
) -> Dict[str, any]:
    """
    Validate raw token prices by querying on-chain pool state directly.
    
    This provides independent verification by getting the actual price from the pool's slot0
    at the specific block, rather than relying on the snapshot's reserves.
    
    Args:
        snapshot: The UniswapBaseSnapshot to validate
        pool_metadata: Pool metadata
        block_number: Block number for validation
        rpc_helper: RPC helper for blockchain queries
    
    Returns:
        Validation results with comparison data and validation errors
    """
    # Extract reported prices from snapshot
    token0_price_reported = snapshot.token0Prices.get(block_number, 0)
    token1_price_reported = snapshot.token1Prices.get(block_number, 0)
    
    # Query pool's slot0 directly from blockchain
    pool_contract = rpc_helper.get_current_node()['web3_client'].eth.contract(
        address=Web3.to_checksum_address(snapshot.address),
        abi=json.load(open("computes/static/abis/UniswapV3Pool.json"))
    )
    
    # Get slot0 at the specific block
    slot0_data = await pool_contract.functions.slot0().call(block_identifier=block_number)
    sqrt_price_x96 = slot0_data[0]  # sqrtPriceX96 is the first element

    token0_price_onchain, token1_price_onchain = sqrtPriceX96ToTokenPrices(
        sqrt_price_x96,
        pool_metadata.token0.decimals,
        pool_metadata.token1.decimals,
    )
    
    validation_result = {
        'block_number': block_number,
        'onchain_data_available': True,
        'sqrt_price_x96': sqrt_price_x96,
        'validation_errors': [],
        'token0_price_comparison': {
            'reported': token0_price_reported,
            'onchain': token0_price_onchain,
        },
        'token1_price_comparison': {
            'reported': token1_price_reported,
            'onchain': token1_price_onchain,
        }
    }
    
    # Collect validation errors instead of asserting
    tolerance = 0.01  # 1% tolerance
    
    if token0_price_onchain > 0 and token0_price_reported > 0:
        token0_relative_diff = abs(token0_price_reported - token0_price_onchain) / token0_price_onchain
        if token0_relative_diff > tolerance:
            error_msg = f"Token0 price mismatch: reported={token0_price_reported:.10f}, on-chain={token0_price_onchain:.10f} (relative diff: {token0_relative_diff:.2%}, tolerance: {tolerance:.2%})"
            validation_result['validation_errors'].append(error_msg)
    
    if token1_price_onchain > 0 and token1_price_reported > 0:
        token1_relative_diff = abs(token1_price_reported - token1_price_onchain) / token1_price_onchain
        if token1_relative_diff > tolerance:
            error_msg = f"Token1 price mismatch: reported={token1_price_reported:.10f}, on-chain={token1_price_onchain:.10f} (relative diff: {token1_relative_diff:.2%}, tolerance: {tolerance:.2%})"
            validation_result['validation_errors'].append(error_msg)
    
    return validation_result


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

    validate_test_environment(app_config)
    
    # Load pool address from settings
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

    pool_metadata: Optional[UniswapPoolMetadata] = await get_pool_metadata(
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
    protocol_state_contract,
    app_config
):
    """Test the PairTotalReservesProcessor with active pools from Redis."""
    validate_test_environment(app_config)
    
    processor = PairTotalReservesProcessor()
    
    try:
        current_block_number = await rpc_helper.get_current_block_number()
    except Exception as e:
        pytest.fail(f"Failed to get current block number: {e}")

    block_offset_from_head = 1
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    from_block = current_block_number - block_offset_from_head
    to_block = from_block  # Single block test
    
    print(f"\nTesting PairTotalReservesProcessor at block: {from_block} (current head: {current_block_number})")

    if not await validate_block_availability(rpc_helper, from_block):
        pytest.skip(f"Skipping test: block {from_block} not available on configured RPC node.")

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
                    ('SOURCE_CHAIN_ID', [Web3.to_checksum_address(settings.data_market)]),
                ],
                contract_addr=protocol_state_contract.address,
                abi=protocol_state_contract.abi,
            )
            print(f"      ℹ️  Fetched source chain ID from contract: {source_chain_id}")
    except Exception as e:
        print(f"      ⚠️  Could not get source chain ID: {e}")
        print(f"      ⚠️  Defaulting to Ethereum mainnet (chain ID 1) for Etherscan API")
        pytest.fail("Could not get source chain ID")

    # Create epoch message
    epoch = SnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=from_block,
        timestamp=int(time.time())
    )

    active_pools = []
    pools_to_process = eval(os.getenv('POOLS_TO_TEST', '[]'))
    if pools_to_process:
        pools_to_process = [Web3.to_checksum_address(pool) for pool in pools_to_process]
        active_pools = pools_to_process

        results = []

        block_details_dict = await get_block_details_in_block_range(
            from_block,
            to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )

        for pool in pools_to_process:
            base_snapshot_data: Optional[UniswapBaseSnapshot] = await base_snapshot_from_block_range(
                pair_address=pool,
                from_block=from_block,
                to_block=to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                ipfs_reader=ipfs_reader,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                block_details_dict=block_details_dict,
            )

            results.append((f"baseSnapshot:{pool}:{app_config.namespace}", base_snapshot_data))

    else:
        # Get active pools from Redis for the test block
        active_pools = await get_active_pools_from_redis(redis_conn, from_block, app_config.namespace)

        if not active_pools:
            pytest.skip(f"No active pools found in Redis for block {from_block} and namespace {app_config.namespace}")

        print(f"Found {len(active_pools)} active pools in Redis for block {from_block}")
        for i, pool in enumerate(active_pools[:5]):  # Print first 5 pools
            print(f"  Pool {i+1}: {pool}")
        if len(active_pools) > 5:
            print(f"  ... and {len(active_pools) - 5} more pools")
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

    # Collect all warnings and validation errors from validation
    all_warnings = []
    all_validation_errors = []
    all_structural_errors = []
    validation_results = []
    
    # Validate each snapshot
    for i, (task_key, snapshot) in enumerate(results):
        print(f"\n📊 Validating snapshot {i+1}/{len(results)} for pool {snapshot.address}")
        
        snapshot_validation_errors = []
        snapshot_warnings = []
        
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
        
        # Validate trade data against Etherscan if API key is available
        print(f"  🔍 Validating trade data against Etherscan...")
        
        pool_metadata = await get_pool_metadata(
            pool_address=snapshot.address,
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
        )
        
        etherscan_validation_errors = []
        cmc_validation_errors = []
        
        if pool_metadata:
            trade_validation = await validate_trade_data_against_etherscan(
                snapshot=snapshot,
                pool_metadata=pool_metadata,
                block_number=from_block,
                source_chain_id=source_chain_id
            )
            
            # Collect Etherscan validation errors
            etherscan_validation_errors = trade_validation.get('validation_errors', [])
            
            # not that useful but a sanity check
            try:
                raw_price_validation = await validate_raw_token_prices_from_onchain_data(
                    snapshot=snapshot,
                    pool_metadata=pool_metadata,
                    block_number=from_block,
                    rpc_helper=rpc_helper
                )
                onchain_errors = raw_price_validation.get('validation_errors', [])
                if onchain_errors:
                    print(f"       ❌ On-chain price validation errors: {len(onchain_errors)}")
                    etherscan_validation_errors.extend(onchain_errors)
            except Exception as e:
                error_msg = f"On-chain price validation failed: {str(e)}"
                print(f"       ❌ {error_msg}")
                etherscan_validation_errors.append(error_msg)
            
            tolerance = os.getenv('COINMARKETCAP_API_PRICE_TOLERANCE', 2)
            tolerance = float(tolerance) / 100
            print(f"    🔍 Validating prices against CoinMarketCap with tolerance: {tolerance}")
            cmc_validation = await validate_prices_against_coinmarketcap(
                snapshot=snapshot,
                pool_metadata=pool_metadata,
                block_number=from_block,
                source_chain_id=source_chain_id,
                tolerance=tolerance
            )

            # Collect CMC validation results
            cmc_warnings = cmc_validation.get('warnings', [])
            cmc_validation_errors = cmc_validation.get('validation_errors', [])
            
            if cmc_warnings:
                print(f"     ⚠️  CMC VALIDATION WARNINGS ({len(cmc_warnings)} total):")
                for i, warning in enumerate(cmc_warnings, 1):
                    print(f"       {i}. {warning}")
                snapshot_warnings.extend(cmc_warnings)
            
            if cmc_validation_errors:
                print(f"     ❌ CMC VALIDATION ERRORS ({len(cmc_validation_errors)} total):")
                for i, error in enumerate(cmc_validation_errors, 1):
                    print(f"       {i}. {error}")
            
            if cmc_validation.get('cmc_available'):
                if not cmc_validation_errors and not cmc_warnings:
                    print(f"     ✅ CoinMarketCap validation passed")
            else:
                reason = cmc_validation.get('reason', 'Unknown')
                print(f"     ℹ️  CoinMarketCap validation skipped: {reason}")
            
            # Log Etherscan validation results
            if etherscan_validation_errors:
                print(f"     ❌ ETHERSCAN VALIDATION ERRORS ({len(etherscan_validation_errors)} total):")
                for i, error in enumerate(etherscan_validation_errors, 1):
                    print(f"       {i}. {error}")
            elif trade_validation.get('etherscan_available'):
                print(f"     ✅ Etherscan validation passed")
            else:
                reason = trade_validation.get('reason', 'Unknown')
                print(f"     ℹ️  Etherscan validation skipped: {reason}")
        else:
            etherscan_validation_errors.append("Could not fetch pool metadata for validation")
            print(f"     ❌ Could not fetch pool metadata for validation")
        
        # Store validation results for this snapshot
        validation_results.append({
            'pool_address': snapshot.address,
            'structural_errors': snapshot_validation_errors,
            'etherscan_errors': etherscan_validation_errors,
            'cmc_errors': cmc_validation_errors,
            'warnings': snapshot_warnings
        })
        
        # Collect all errors and warnings
        all_structural_errors.extend(snapshot_validation_errors)
        all_validation_errors.extend(etherscan_validation_errors)
        all_validation_errors.extend(cmc_validation_errors)
        all_warnings.extend(snapshot_warnings)

    # Validate that we processed some of the active pools
    processed_pool_addresses = {snapshot.address for _, snapshot in results}
    active_pool_addresses = {Web3.to_checksum_address(pool) for pool in active_pools}
    
    # Collect basic processing errors instead of asserting immediately
    processing_errors = []
    if not processed_pool_addresses.issubset(active_pool_addresses):
        extra_pools = processed_pool_addresses - active_pool_addresses
        processing_errors.append(f"Processed pools should be subset of active pools. Extra pools: {extra_pools}")
    
    coverage_ratio = len(processed_pool_addresses) / len(active_pool_addresses)
    print(f"\n📈 Processing Coverage: {len(processed_pool_addresses)}/{len(active_pool_addresses)} pools ({coverage_ratio:.1%})")
    
    if coverage_ratio < 1:
        print("⚠️  Low processing coverage. This could indicate:")
        print("    - Many pools lack sufficient metadata")
        print("    - Pools have no liquidity/events in this block")
        print("    - RPC or network issues during processing")

    # Comprehensive test result summary
    print(f"\n{'='*80}")
    print(f"🔍 COMPREHENSIVE VALIDATION SUMMARY")
    print(f"{'='*80}")
    
    print(f"📊 Snapshots Processed: {len(results)}")
    print(f"✅ Valid Snapshots: {len([r for r in validation_results if not r['structural_errors']])}")
    print(f"❌ Invalid Snapshots: {len([r for r in validation_results if r['structural_errors']])}")
    print(f"⚠️  Total Warnings: {len(all_warnings)}")
    print(f"🚨 Total Validation Errors: {len(all_validation_errors) + len(all_structural_errors) + len(processing_errors)}")
    
    # Show detailed breakdown by pool
    if validation_results:
        print(f"\n📋 Detailed Results by Pool:")
        for result in validation_results:
            pool = result['pool_address']
            structural_errors = len(result['structural_errors'])
            etherscan_errors = len(result['etherscan_errors'])
            cmc_errors = len(result['cmc_errors'])
            warnings = len(result['warnings'])
            
            status = "✅ PASS" if structural_errors == 0 and etherscan_errors == 0 and cmc_errors == 0 else "❌ FAIL"
            print(f"   {status} {pool}: {structural_errors} structural, {etherscan_errors} etherscan, {cmc_errors} cmc errors, {warnings} warnings")
    
    # Show all validation errors if any exist
    all_errors = all_structural_errors + all_validation_errors + processing_errors
    if all_errors:
        print(f"\n🚨 VALIDATION ERRORS ({len(all_errors)} total):")
        for i, error in enumerate(all_errors, 1):
            print(f"    {i}. {error}")
    
    # Show all warnings if any exist
    if all_warnings:
        print(f"\n⚠️  VALIDATION WARNINGS ({len(all_warnings)} total):")
        for i, warning in enumerate(all_warnings, 1):
            print(f"    {i}. {warning}")
        
        # Emit pytest warnings so they appear in final test output
        emit_pytest_warnings([f"Test completed with {len(all_warnings)} validation warnings"], "test_summary")
    
    print(f"\n{'='*80}")
    
    # Final assertions - test fails ONLY if there are validation errors
    if all_errors:
        print(f"❌ TEST FAILED: {len(all_errors)} validation errors found")
        print(f"   - {len(all_structural_errors)} structural errors")
        print(f"   - {len(all_validation_errors)} external validation errors") 
        print(f"   - {len(processing_errors)} processing errors")
        
        # Create a comprehensive error message
        error_summary = f"Test failed with {len(all_errors)} validation errors across {len(results)} snapshots"
        if all_warnings:
            error_summary += f" and {len(all_warnings)} warnings"
        
        # Assert at the end with all collected errors
        assert False, error_summary
    else:
        if all_warnings:
            print(f"✅ TEST PASSED WITH WARNINGS: {len(all_warnings)} warnings found but no validation errors")
        else:
            print(f"✅ TEST PASSED: No validation errors or warnings found")

    print("PASSED: test_pair_total_reserves_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")