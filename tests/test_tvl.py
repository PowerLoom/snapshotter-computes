from typing import Optional, List, Dict, Tuple
from web3 import Web3
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import pytest

from computes.utils.models.message_models import UniswapPoolMetadata
from computes.utils.models.data_models import TickData


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
    from computes.utils import constants
    metadata_processor = MetadataProcessor()

    validate_test_environment(app_config)
    await constants.initialize_rpc(injected_rpc_helper=rpc_helper)
    # USDC-WETH pool
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


if __name__ == "__main__":
    print("To run these tests, use the `pytest` command from the project root.")
