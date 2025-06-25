import json
import time
from typing import Dict
import pytest
from redis import asyncio as aioredis

from computes.eth_price import EthPriceProcessor
from computes.settings.config import settings as computes_settings
from computes.utils.models.message_models import UniswapEthPriceSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage

"""
Test for EthPriceProcessor price validation.

This test validates the accuracy of ETH/USD prices retrieved from Redis by comparing them
against the authoritative Chainlink Oracle price feed.

Test Flow:
1. Retrieves price data from Redis for a specific block using the EthPriceProcessor
2. Fetches the latest price from Chainlink ETH/USD Oracle (0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419)
3. Compares the Redis price against the Chainlink price with a 0.5% tolerance
4. Warns if Chainlink data is older than 5 minutes (ETH/USD updates every 30min or on 0.5% price change)

Validation Criteria:
- Redis price must be within 0.5% of Chainlink price
- Chainlink data staleness warning if > 5 minutes old
- Ensures EthPriceProcessor correctly retrieves and processes Redis data

This test ensures that the price data stored in Redis is reasonably accurate when compared
to the official Chainlink oracle.
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


async def get_chainlink_price_data(rpc_helper, oracle_address: str = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419") -> Dict:
    """Get the latest price data from Chainlink Oracle"""
    try:
        # Chainlink Oracle ABI
        oracle_abi = [{"inputs":[{"internalType":"address","name":"_aggregator","type":"address"},{"internalType":"address","name":"_accessController","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"int256","name":"current","type":"int256"},{"indexed":True,"internalType":"uint256","name":"roundId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"updatedAt","type":"uint256"}],"name":"AnswerUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"uint256","name":"roundId","type":"uint256"},{"indexed":True,"internalType":"address","name":"startedBy","type":"address"},{"indexed":False,"internalType":"uint256","name":"startedAt","type":"uint256"}],"name":"NewRound","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"}],"name":"OwnershipTransferRequested","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"acceptOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"accessController","outputs":[{"internalType":"contract AccessControllerInterface","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"aggregator","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_aggregator","type":"address"}],"name":"confirmAggregator","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"description","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_roundId","type":"uint256"}],"name":"getAnswer","outputs":[{"internalType":"int256","name":"","type":"int256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint80","name":"_roundId","type":"uint80"}],"name":"getRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_roundId","type":"uint256"}],"name":"getTimestamp","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestAnswer","outputs":[{"internalType":"int256","name":"","type":"int256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestRound","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestTimestamp","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address payable","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint16","name":"","type":"uint16"}],"name":"phaseAggregators","outputs":[{"internalType":"contract AggregatorV2V3Interface","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"phaseId","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_aggregator","type":"address"}],"name":"proposeAggregator","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"proposedAggregator","outputs":[{"internalType":"contract AggregatorV2V3Interface","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint80","name":"_roundId","type":"uint80"}],"name":"proposedGetRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proposedLatestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_accessController","type":"address"}],"name":"setController","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_to","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
        
        # Get latest round data
        latest_round_data = await rpc_helper.web3_call(
            tasks=[("latestRoundData", [])],
            contract_addr=oracle_address,
            abi=oracle_abi
        )
        
        if not latest_round_data or not latest_round_data[0]:
            raise Exception("Failed to get latest round data from Chainlink Oracle")
        
        round_data = latest_round_data[0]
        round_id, answer, started_at, updated_at, answered_in_round = round_data
        
        # Get decimals
        decimals_result = await rpc_helper.web3_call(
            tasks=[("decimals", [])],
            contract_addr=oracle_address,
            abi=oracle_abi
        )
        
        decimals = decimals_result[0] if decimals_result else 8
        
        # Convert answer to price (Chainlink prices are typically in 8 decimals)
        price = float(answer) / (10 ** decimals)
        
        return {
            'round_id': round_id,
            'price': price,
            'started_at': started_at,
            'updated_at': updated_at,
            'answered_in_round': answered_in_round,
            'decimals': decimals
        }
        
    except Exception as e:
        logger.error(f"Error getting Chainlink price data: {e}")
        raise


async def verify_redis_price_data(redis_conn: aioredis.Redis, block_number: int) -> Dict:
    """Verify and return Redis price data for a specific block"""
    from computes.redis_keys import uniswap_eth_usd_price_zset
    
    # Get price data for the specific block
    price_data = await redis_conn.zrangebyscore(
        name=uniswap_eth_usd_price_zset,
        min=block_number,
        max=block_number,
    )
    
    if not price_data:
        pytest.skip(f"No Redis price data found for block {block_number}")
    
    # Parse the price data
    parsed_data = {}
    for price_entry in price_data:
        price_info = json.loads(price_entry.decode('utf-8'))
        block_height = price_info['blockHeight']
        price = price_info['price']
        parsed_data[block_height] = price
    
    return parsed_data


@pytest.mark.asyncio(loop_scope="module")
async def test_eth_price_processor(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test the EthPriceProcessor with normal operation against a historical block."""
    processor = EthPriceProcessor()
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

    # Verify Redis price data exists for this block
    redis_price_data = await verify_redis_price_data(redis_conn, from_block)
    assert redis_price_data, f"No Redis price data found for block {from_block}"
    
    print(f"\nRedis price data for block {from_block}: {redis_price_data}")

    # Create a test epoch message
    epoch = SnapshotProcessMessage(
        begin=from_block,
        end=from_block,
        epochId=1,
        timestamp=int(time.time())
    )

    # Process the epoch
    results = await processor.compute(
        epoch=epoch,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        task_type="price:ETH:{Namespace}"
    )

    assert len(results) == 1, "Should return one result tuple"
    task_type, snapshot = results[0]
    assert isinstance(snapshot, UniswapEthPriceSnapshot), "Should return UniswapEthPriceSnapshot"
    assert snapshot.epoch.begin == from_block, "Snapshot should have correct begin block"
    assert snapshot.epoch.end == from_block, "Snapshot should have correct end block"

    # Verify snapshot data matches Redis data
    assert snapshot.ethPrice == redis_price_data, "Snapshot price data should match Redis data"

    # Get the block timestamp
    block_data = await rpc_helper.eth_get_block(from_block)
    block_timestamp = int(block_data['timestamp'], 16)
    print(f"\nBlock {from_block} timestamp: {block_timestamp}")

    # Get Chainlink Oracle price data
    print(f"\n🔍 Getting Chainlink Oracle price data...")

    print(f"Using Chainlink Oracle address: {computes_settings.contract_addresses.chainlink_eth_usd_oracle}")
    chainlink_data = await get_chainlink_price_data(
        rpc_helper=rpc_helper, 
        oracle_address=computes_settings.contract_addresses.chainlink_eth_usd_oracle
    )
    
    print(f"Chainlink latest round data:")
    print(f"  Round ID: {chainlink_data['round_id']}")
    print(f"  Price: ${chainlink_data['price']:.2f}")
    print(f"  Started At: {chainlink_data['started_at']}")
    print(f"  Updated At: {chainlink_data['updated_at']}")
    print(f"  Answered In Round: {chainlink_data['answered_in_round']}")
    print(f"  Decimals: {chainlink_data['decimals']}")

    # Compare Redis price with Chainlink price
    redis_price = float(list(redis_price_data.values())[0])
    chainlink_price = chainlink_data['price']
    
    price_difference = abs(redis_price - chainlink_price)
    price_difference_percent = (price_difference / chainlink_price) * 100
    
    print(f"\n💰 Price comparison:")
    print(f"  Redis price: ${redis_price:.2f}")
    print(f"  Chainlink price: ${chainlink_price:.2f}")
    print(f"  Difference: ${price_difference:.2f} ({price_difference_percent:.2f}%)")

    # Check if Chainlink data is stale (older than 5 minutes)
    current_time = int(time.time())
    chainlink_data_age = current_time - chainlink_data['updated_at']
    stale_threshold_seconds = 5 * 60
    
    if chainlink_data_age > stale_threshold_seconds:
        print(f"\n⚠️  WARNING: Chainlink data is {chainlink_data_age} seconds old (stale threshold: {stale_threshold_seconds} seconds)")
        print(f"    Round {chainlink_data['round_id']} was updated at {chainlink_data['updated_at']} (current time: {current_time})")
        print(f"    This may indicate that the Chainlink oracle has not been updated recently.")
        print(f"    The price comparison may not reflect the most current market conditions.")
        print(f"    Note: ETH/USD price feed updates every 30 minutes or on a price change of 0.5%.")
    
    # Allow for some price difference (e.g., 0.5% tolerance)
    price_tolerance_percent = 0.5
    assert price_difference_percent <= price_tolerance_percent, f"Price difference {price_difference_percent:.2f}% exceeds tolerance of {price_tolerance_percent}%"

    print("PASSED: test_eth_price_processor")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
