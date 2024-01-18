import asyncio
from decimal import Decimal, getcontext
import functools
import json
from typing import Union

from eth_abi import abi
from eth_typing import Address
from eth_typing.evm import Address
from eth_typing.evm import ChecksumAddress
from web3 import Web3
from web3.contract import Contract

from .utils.constants import helper_contract
from .utils.constants import UNISWAP_TRADE_EVENT_SIGS
from .utils.constants import pair_contract_abi
from .utils.constants import override_address
from .utils.constants import univ3_helper_bytecode
from .utils.constants import UNISWAP_EVENTS_ABI
from .utils.constants import MAX_TICK, MIN_TICK
from .redis_keys import uniswap_cached_tick_data_block_height

from snapshotter.utils.rpc import RpcHelper, get_event_sig_and_abi
from snapshotter.utils.default_logger import logger
AddressLike = Union[Address, ChecksumAddress]
getcontext().prec = 36
tvl_logger = logger.bind(module='PowerLoom|UniswapTotalValueLocked')

def transform_tick_bytes_to_list(tickData: bytes):
    if tickData == b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00':
        return []
    # eth_abi decode tickdata as a bytes[]
    bytes_decoded_arr = abi.decode(
        ("bytes[]", "(int128,int24)"), tickData
    )
    ticks = [
        {
            "liquidity_net": int.from_bytes(i[:-3], "big", signed=True),
            "idx": int.from_bytes(i[-3:], "big", signed=True),
        }
        for i in bytes_decoded_arr[0]
    ]

    return ticks


def calculate_tvl_from_ticks(ticks, pair_metadata, sqrt_price):
    sqrt_price = Decimal(sqrt_price) / Decimal(2 ** 96)
    
    liquidity_total = Decimal(0)
    token0_liquidity = Decimal(0)
    token1_liquidity = Decimal(0)
    tick_spacing = 1

    if len(ticks) == 0:
        return (0, 0)

    int_fee = int(pair_metadata["pair"]["fee"])

    if int_fee == 3000:
        tick_spacing = Decimal(60)
    elif int_fee == 500:
        tick_spacing = Decimal(10)
    elif int_fee == 10000:
        tick_spacing = Decimal(200)
    # https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf
    

    for i in range(len(ticks)):
        tick = ticks[i]
        idx = Decimal(tick["idx"])
        nextIdx = Decimal(ticks[i + 1]["idx"]) \
        if i < len(ticks) - 1 \
        else idx + tick_spacing
        
        liquidity_net = Decimal(tick["liquidity_net"])
        liquidity_total += liquidity_net
        sqrtPriceLow = Decimal(1.0001) ** (idx / 2)
        sqrtPriceHigh = Decimal(1.0001) ** (nextIdx / 2)

        if sqrt_price <= sqrtPriceLow:
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
            
        elif sqrt_price >= sqrtPriceHigh:
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
            
        else: 

            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrt_price,
                sqrtPriceHigh
            )
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrt_price,
            )   
        
            
    return (int(token0_liquidity), int(token1_liquidity))


def get_token0_in_pool(
    liquidity: int,
    
    sqrtPriceLow: int,
    sqrtPriceHigh: int,
) -> int:
    
    return liquidity * (sqrtPriceHigh - sqrtPriceLow) / (sqrtPriceLow * sqrtPriceHigh) // 1


def get_token1_in_pool(
    liquidity: int,
    
    sqrtPriceLow: int,
    sqrtPriceHigh: int,
) -> int:
    
    return liquidity * (sqrtPriceHigh - sqrtPriceLow) // 1


async def get_events(
    pair_address: str,
    rpc: RpcHelper,
    from_block,
    to_block,

    redis_con,
):

    event_sig, event_abi = get_event_sig_and_abi(
        UNISWAP_TRADE_EVENT_SIGS,
        UNISWAP_EVENTS_ABI,
    )
    
    
    events = await rpc.get_events_logs(
        contract_address=pair_address,
        to_block=to_block,
        from_block=from_block,
        topics=[event_sig],
        event_abi=event_abi,
        redis_conn=redis_con,
        )
    
    return events


def _load_abi(path: str) -> str:
    with open(path) as f:
        abi: str = json.load(f)
    return abi


async def calculate_reserves(
    pair_address: str,
    from_block,
    pair_per_token_metadata,
    rpc_helper: RpcHelper,
    redis_conn,
):

    ticks_list, slot0 = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        redis_conn=redis_conn,
        pair_per_token_metadata=pair_per_token_metadata,

    )
    
    sqrt_price = slot0[0]

    t0_reserves, t1_reserves = calculate_tvl_from_ticks(
        ticks_list,
        pair_per_token_metadata,
        sqrt_price,
    )

    return [int(t0_reserves), int(t1_reserves)]


async def get_tick_info(
        rpc_helper: RpcHelper,
        pair_address: str,  
        from_block,
        redis_conn,
        pair_per_token_metadata,
    
):
    
    ticks_list = []
    slot0 = 0
    cached_tick_dict = await redis_conn.zrangebyscore(
        name=uniswap_cached_tick_data_block_height.format(
                Web3.to_checksum_address(pair_address),
        ),
        min=int(from_block),
        max=int(from_block),
    )

    if cached_tick_dict:
        tick_dict = json.loads(cached_tick_dict[0])
        return tick_dict["ticks_list"], tick_dict["slot0"]
    
        # get token price function takes care of its own rate limit
    try: 
        overrides = {
            override_address: {"code": univ3_helper_bytecode},
        }
        current_node = rpc_helper.get_current_node()
        pair_contract = current_node['web3_client'].eth.contract(address=pair_address, abi=pair_contract_abi)

        # batch rpc calls for tick data to prevent oog errors
        # step must be a divisor of 887272 * 2
        fee = pair_per_token_metadata["pair"]["fee"]

        # if fee is 100
        step = (MAX_TICK - MIN_TICK) // 16

        # we can cut down on rpc requests by increasing step size for higher fees
        if fee == 500:
            step = (MAX_TICK - MIN_TICK) // 4
        elif fee == 3000:
            step = MAX_TICK - MIN_TICK  // 2
        elif fee == 10000:
            step = MAX_TICK - MIN_TICK

        tick_tasks = []
    
        # getTicks() is inclusive for start and end ticks
        for idx in range(MIN_TICK, MAX_TICK+1, step):
            tick_tasks.append(
                helper_contract.functions.getTicks(
                    pair_address, idx, min(idx + step - 1, MAX_TICK),
                ),
            )
        
        slot0_tasks = [
            pair_contract.functions.slot0(),
        ]
        
        # cant batch these tasks due to implementation of web3_call re: state override
        tickDataResponse, slot0Response = await asyncio.gather(
            rpc_helper.web3_call(tick_tasks, redis_conn, overrides=overrides, block=from_block),
            rpc_helper.web3_call(slot0_tasks, redis_conn, block=from_block,),
            )
            
        for ticks in tickDataResponse:
            ticks_list.append(transform_tick_bytes_to_list(ticks))

        ticks_list = functools.reduce(lambda x, y: x + y, ticks_list)
        
        slot0 = slot0Response[0]
        

        # if len(ticks_list) > 0:
        #     redis_cache_mapping = {
        #         json.dumps({"blockHeight": from_block, "slot0": slot0, "ticks_list": ticks_list,}): int(from_block)
        #     }

        #     await asyncio.gather(
        #         redis_conn.zadd(
        #             name=uniswap_cached_tick_data_block_height.format(
        #                 Web3.to_checksum_address(pair_address),
        #             ),
        #             mapping=redis_cache_mapping,
        #         ),
        #         redis_conn.zremrangebyscore(
        #             name=uniswap_cached_tick_data_block_height.format(
        #                 Web3.to_checksum_address(pair_address),
        #             ),
        #             min=0,
        #             max=from_block - 20, # shouldn't need to keep all tick data in this implementation
        #         ),
        #     )
        return ticks_list, slot0
    except Exception as err:
        tvl_logger.warning('Failed to get tick data for pair {} at block {} with error {}', pair_address, from_block, err)
        # if we erred, set ticks list and slot0 to empty
        raise err