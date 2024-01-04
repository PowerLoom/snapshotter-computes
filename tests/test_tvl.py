import os
from web3 import Web3
import asyncio
import sys

from ..utils.constants import erc20_abi
from ..total_value_locked import _load_abi, calculate_reserves, calculate_tvl_from_ticks, get_tick_info
from ..utils.helpers import get_pair_metadata
from snapshotter.settings.config import settings
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.rpc import RpcHelper
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

graph_endpoint = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
transport = AIOHTTPTransport(url=graph_endpoint)    
client = Client(transport=transport, fetch_schema_from_transport=True)
async def test_calculate_reserves():
    # Mock your parameters
    
    pair_address = Web3.to_checksum_address(
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    )
    from_block = 18931130

    rpc_helper = RpcHelper()
    aioredis_pool = RedisPoolCache()
    query = """{
    pool(id: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", block: {number: 18931130}) {
        ticks(first: 1000, where: {liquidityGross_not: 0}, orderBy: tickIdx, orderDirection: asc) {
        tickIdx
        liquidityNet
        }
    }
    }"""
    print(os.environ['RPC_URL'])
    w3 = Web3(Web3.HTTPProvider(os.environ['RPC_URL']))
    pair_contract = w3.eth.contract(
        address=pair_address,
        abi=_load_abi("snapshotter/modules/computes/static/abis/UniswapV3Pool.json"),
    
    )



    await aioredis_pool.populate()
    redis_conn = aioredis_pool._aioredis_pool
    pair_per_token_metadata = await get_pair_metadata(
        pair_address=pair_address, redis_conn=redis_conn, rpc_helper=rpc_helper
    )  # Replace with your data

    gql_response = await client.execute_async(gql(query))
    gql_ticks = [{'idx': int(tick['tickIdx']), 'liquidity_net': int(tick['liquidityNet'])} for tick in gql_response['pool']['ticks']]
    
    # Call your async function
    reserves = await calculate_reserves(
        pair_address, from_block, pair_per_token_metadata, rpc_helper, redis_conn
    )
    rpc_ticks, slot0 = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        redis_conn=redis_conn,
        pair_per_token_metadata=pair_per_token_metadata,
    )
    rpc_tick_len = len(rpc_ticks)

    print(rpc_tick_len, 'rpc ticks')
    first_idxs = set(item['idx'] for item in rpc_ticks)
    second_idxs = set(item['idx'] for item in gql_ticks)

    # Identify missing elements in each list
    missing_in_first = second_idxs - first_idxs
    missing_in_second = first_idxs - second_idxs

    # Print the missing elements
    print(missing_in_first)
    print(missing_in_second)
    print('missing')
    gql_reserves = calculate_tvl_from_ticks(
        gql_ticks,
        pair_per_token_metadata,
        slot0[0],   
    )
    print(gql_reserves, 'gql reserves including incorrectly indexed ticks')
    for idx in missing_in_first:
        print({'idx': idx, 'liquidity_net': next(item['liquidity_net'] for item in gql_ticks if item['idx'] == idx)})
        # double check that ticks in gql and not rpc are incorrectly indexed
        tick_resp = pair_contract.functions.ticks(idx).call(block_identifier=from_block)
        assert(tick_resp[1] == 0, 'tick is not retrieved via rpc')
        print(tick_resp, 'rpc tick')
    # calculate extra reserves from incorrectly indexed ticks
    extra_reserves = calculate_tvl_from_ticks(
        [{'idx': idx, 'liquidity_net': next(item['liquidity_net'] for item in gql_ticks if item['idx'] == idx)} for idx in missing_in_first],
        pair_per_token_metadata,
        slot0[0],
    )
    print(extra_reserves, 'extra reserves')
    # pop incorrectly indexed ticks
    gql_ticks = [tick for tick in gql_ticks if tick['idx'] not in missing_in_first]
    # verify that ticks from gql calculate the same as ours
    gql_reserves = calculate_tvl_from_ticks(
        gql_ticks,
        pair_per_token_metadata,
        slot0[0],   
    )
    

    print(gql_reserves, 'gql reserves')
    print(reserves, 'rpc reserves')
    
    assert reserves[0] == gql_reserves[0], 'reserves do not match'
    assert reserves[1] == gql_reserves[1], 'reserves do not match'
    # print("Elements missing in the first list:")
    # for idx in missing_in_first:
    #     print({'idx': idx, 'liquidityNet': next(item['liquidityNet'] for item in reserves[2] if item['idx'] == idx)})

    # print("\nElements missing in the second list:")
    # for idx in missing_in_second:
    #     print({'tickIdx': idx, 'liquidity_net': next(item['liquidity_net'] for item in gql_response['pool']['ticks'] if item['tickIdx'] == idx)})

    #     print(reserves[2], 'rpc ticks')
    #     print(gql_response['pool']['ticks'], 'gql ticks')
        


    # Check that it returns an array of correct form
    assert isinstance(reserves, list), "Should return a list"
    assert len(reserves) == 2, "Should have two elements"

    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(settings.rpc.full_nodes[0].url))
    contract = w3.eth.contract(
        address=pair_address,
        abi=_load_abi("snapshotter/modules/computes/static/abis/UniswapV3Pool.json"),
    )
    token0 = contract.functions.token0().call()
    token1 = contract.functions.token1().call()

    token0_contract = w3.eth.contract(address=token0, abi=erc20_abi)
    token1_contract = w3.eth.contract(address=token1, abi=erc20_abi)
    # Fetch actual reserves from blockchain
    # The function name 'getReserves' and the field names may differ based on the actual ABI
    token0_actual_reserve = token0_contract.functions.balanceOf(pair_address).call(block_identifier=from_block)
    token1_actual_reserve = token1_contract.functions.balanceOf(pair_address).call(block_identifier=from_block)

    print(reserves)
    print(token0_actual_reserve, token1_actual_reserve)

    # Compare them with returned reserves
    # our calculations should be less than or equal to token balance.
    #  assuming a maximum of 30%? of token balance is unpaid fees
    assert (
        reserves[0] >= token0_actual_reserve * 0.8
    ), "calculated reserve is lower than 90% token balance"
    assert (
        reserves[0] <= token0_actual_reserve
    ), "calculated reserve is higher than token balance"
    assert (
        reserves[1] >= token1_actual_reserve * 0.8
    ), "calculated reserve is lower than 90% token balance"
    assert (
        reserves[1] <= token1_actual_reserve
    ), "calculated reserve is higher than token balance"
    print("PASSED")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_calculate_reserves())




