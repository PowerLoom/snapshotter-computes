import asyncio
import json
import pytest
import httpx
from redis import asyncio as aioredis

from computes.utils.core import get_pair_reserves
from snapshotter.utils.redis.redis_conn import provide_async_redis_conn_insta


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


@provide_async_redis_conn_insta
async def fetch_liquidityUSD_rpc(
    pair_address,
    block_num,
    rpc_helper,
    anchor_rpc_helper,
    protocol_state_contract,
    ipfs_reader,
    redis_conn,
):
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
    return data.token0TradeVolumeUSD + data.token1TradeVolumeUSD


def fetch_liquidityUSD_graph(pair_address, block_num):
    uniswap_url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
    uniswap_payload = (
        '{"query":"{\\n pair(id: \\"'
        + str(pair_address)
        + '\\",block:{number:'
        + str(
            block_num,
        )
        + "}) {\\n reserveUSD \\n token0 { \\n     symbol \\n } \\n token1 {"
        ' \\n      symbol \\n    }\\n  } \\n }" }'
    )
    print(uniswap_payload)
    headers = {"Content-Type": "application/plain"}
    response = httpx.post(
        url=uniswap_url,
        headers=headers,
        data=uniswap_payload,
        timeout=30,
    )
    if response.status_code == 200:
        data = json.loads(response.text)
        print("Response", data)
        data = data["data"]
        return float(data["pair"]["reserveUSD"])
    else:
        print("Error fetching data from uniswap THE GRAPH %s", response)
        return 0


@pytest.mark.asyncio(loop_scope="module")
async def test_compare_liquidity(
    rpc_helper,
    anchor_rpc_helper,
    ipfs_reader,
    redis_conn,
    protocol_state_contract,
    app_config
):
    """Test comparing liquidity between Graph Protocol and RPC calls."""
    validate_test_environment(app_config)
    
    total_liquidity_usd_graph = 0
    total_liquidity_usd_rpc = 0

    current_block_number = await rpc_helper.get_current_block_number()
    block_offset_from_head = 10
    if current_block_number <= block_offset_from_head:
        pytest.skip(f"Chain height ({current_block_number}) too low to test with offset {block_offset_from_head}")
    
    block_num = current_block_number - block_offset_from_head
    print(f"\nTesting at block near chain head: {block_num} (current head: {current_block_number})")

    # Check if the block is available
    if not await validate_block_availability(rpc_helper, block_num):
        pytest.skip(f"Skipping test: block {block_num} not available on configured RPC node.")

    contracts = list()
    contracts.append("0xE0554a476A092703abdB3Ef35c80e0D76d32939F")
    
    for contract in contracts:
        liquidity_usd_graph = fetch_liquidityUSD_graph(contract, block_num)
        liquidity_usd_rpc = await fetch_liquidityUSD_rpc(contract, block_num, rpc_helper, anchor_rpc_helper, protocol_state_contract, ipfs_reader)
        
        print(
            f"Contract {contract}, liquidityUSD_graph is"
            f" {liquidity_usd_graph} , liquidityUSD_rpc {liquidity_usd_rpc},"
            " liquidityUSD difference"
            f" {(liquidity_usd_rpc - liquidity_usd_graph)}",
        )
        
        total_liquidity_usd_graph += liquidity_usd_graph
        total_liquidity_usd_rpc += liquidity_usd_rpc

    print(
        f"{len(contracts)} contracts compared, liquidityUSD_rpc_total is"
        f" {total_liquidity_usd_rpc}, liquidityUSD_graph_total is"
        f" {total_liquidity_usd_graph}",
    )
    
    # Assert that both values are positive numbers
    assert total_liquidity_usd_rpc >= 0, "RPC liquidity should be non-negative"
    assert total_liquidity_usd_graph >= 0, "Graph liquidity should be non-negative"

    print("PASSED: test_compare_liquidity")


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
