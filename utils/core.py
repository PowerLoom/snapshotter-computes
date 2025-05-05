import asyncio
from distutils import core
import json
from functools import reduce
import time
from typing import Dict, List, Optional

from redis import asyncio as aioredis
from computes.metadata import MetadataProcessor
from computes.utils.models.message_models import UniswapPoolMetadata
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import get_event_sig_and_abi
from rpc_helper.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.redis_keys import uniswap_pair_cached_block_height_reserves
from computes.total_value_locked import calculate_reserves
from computes.total_value_locked import get_tick_info
from computes.total_value_locked import get_token0_in_pool
from computes.total_value_locked import get_token1_in_pool
from computes.utils.constants import UNISWAP_EVENTS_ABI
from computes.utils.constants import UNISWAP_TRADE_EVENT_SIGS
from computes.utils.constants import UNISWAPV3_FEE_DIV
from computes.utils.helpers import get_events_from_cache, get_pair_metadata
from computes.utils.models.data_models import UniswapEvent, epoch_event_trade_data
from computes.utils.models.data_models import event_trade_data
from computes.utils.models.data_models import trade_data
from computes.utils.pricing import get_token_price_in_block_range

core_logger = logger.bind(module='PowerLoom|UniswapCore')


async def get_pair_reserves(
    pair_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_details_dict: dict = dict(),
):
    """
    Fetch and calculate token0 and token1 pair reserves for a given Uniswap V3 pool contract address over a block range.

    Args:
        pair_address (str): The address of the Uniswap pair contract.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        ipfs_reader (AsyncIPFSClient): IPFS reader for metadata.
        protocol_state_contract: Protocol state contract instance.
        block_details_dict (dict, optional): Pre-fetched block details. If None, will fetch them.

    Returns:
        dict: A dictionary containing pair reserves data for each block in the range.
    """
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Starting token0 and token1 reserves computation | Wall time: {}",
        from_block,
        to_block,
        pair_address,
        time.time()
    )

    pair_address = Web3.to_checksum_address(pair_address)
    
    # Only fetch block details if not provided
    if not block_details_dict:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block,
                to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                "[Epoch {}-{}] Pool {} | Failed to fetch block details: {}",
                from_block,
                to_block,
                pair_address,
                err
            )
            return None
        else:
            core_logger.debug(
                "[Epoch {}-{}] Pool {} | Block details fetched successfully",
                from_block,
                to_block,
                pair_address
            )

    metadata_processor = MetadataProcessor()
    pair_per_token_metadata: Optional[UniswapPoolMetadata] = await metadata_processor.get_pool_metadata(
        pool_address=pair_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )

    if not pair_per_token_metadata:
        core_logger.error(
            "[Epoch {}-{}] Pool {} | Failed to fetch pair metadata",
            from_block,
            to_block,
            pair_address
        )
        raise Exception(f'Error attempting to get pair metadata for: {pair_address}')

    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token0.dict(),
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token1.dict(),
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
        return_exceptions=True
    )
    core_logger.debug('Epoch {}-{} | Pool {} | Token prices fetch results: {}', from_block, to_block, pair_address, [token0_price_map, token1_price_map])

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Token prices fetched successfully",
        from_block,
        to_block,
        pair_address
    )

    # attempt to fetch previous epoch end block reserves from redis
    cached_reserves_dict = await redis_conn.zrangebyscore(
        name=uniswap_pair_cached_block_height_reserves.format(
            Web3.to_checksum_address(pair_address),
        ),
        min=int(from_block - 1),
        max=int(from_block - 1),
    )

    if cached_reserves_dict:
        loaded_dict = json.loads(cached_reserves_dict[0])
        initial_reserves = [int(loaded_dict['token0_reserves']), int(loaded_dict['token1_reserves'])]
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Using cached reserves: token0={}, token1={}",
            from_block,
            to_block,
            pair_address,
            initial_reserves[0],
            initial_reserves[1]
        )
    else:
        initial_reserves = await calculate_reserves(
            pair_address,
            from_block - 1,
            pair_per_token_metadata,
            rpc_helper,
            redis_conn,
        )
        core_logger.info(
            "[Epoch {}-{}] Pool {} | Calculated initial reserves: token0={}, token1={}",
            from_block,
            to_block,
            pair_address,
            initial_reserves[0],
            initial_reserves[1]
        )

        if any(x == 0 for x in initial_reserves):
            core_logger.error(
                "[Epoch {}-{}] Pool {} | Failed to calculate initial reserves",
                from_block,
                to_block,
                pair_address
            )
            return None
        
    # grab mint/burn events in range
    events: Dict[int, List[UniswapEvent]] = await get_events_from_cache(
        pool_address=pair_address,
        from_block=from_block if cached_reserves_dict else from_block + 1,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Found {} events to process",
        from_block,
        to_block,
        pair_address,
        len(events)
    )

    # sum burn and mint each block
    token0Amount = initial_reserves[0]
    token1Amount = initial_reserves[1]
    

    pair_reserves_dict = dict()
    for block_num in range(from_block, to_block + 1):
        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))

        token0USD = token0Amount * token0_price_map.get(from_block, 0) * \
            (10 ** -int(pair_per_token_metadata.token0.decimals))
        token1USD = token1Amount * token1_price_map.get(from_block, 0) * \
            (10 ** -int(pair_per_token_metadata.token1.decimals))
        pair_reserves_dict[block_num] = {
            'token0': token0AmountNormalized,
            'token1': token1AmountNormalized,
            'token0TokenAmt': token0Amount,
            'token1TokenAmt': token1Amount,
            'token0USD': token0USD,
            'token1USD': token1USD,
            'token0Price': token0_price_map.get(from_block, 0),
            'token1Price': token1_price_map.get(from_block, 0),
            'timestamp': block_details_dict.get(from_block, {}).get('timestamp', 0),
        }
    # sort access by block number
    for block_num in sorted(events.keys()):
        event_list = events[block_num]
        # Swap events use ints and mint events are positive, so only need to subtract burn events.
        token0Amount += reduce(
            lambda acc, event: acc - event.args['amount0']
            if event.eventName == 'Burn'
            else acc + event.args['amount0'], event_list, 0,
        )
        token1Amount += reduce(
            lambda acc, event: acc - event.args['amount1']
            if event.eventName == 'Burn'
            else acc + event.args['amount1'], event_list, 0,
        )

        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))

        token0USD = token0Amount * token0_price_map.get(block_num, 0) * \
            (10 ** -int(pair_per_token_metadata.token0.decimals))
        token1USD = token1Amount * token1_price_map.get(block_num, 0) * \
            (10 ** -int(pair_per_token_metadata.token1.decimals))

        token0Price = token0_price_map.get(block_num, 0)
        token1Price = token1_price_map.get(block_num, 0)

        current_block_details = block_details_dict.get(block_num, None)

        timestamp = (
            current_block_details.get(
                'timestamp',
                None,
            )
            if current_block_details
            else None
        )

        pair_reserves_dict[block_num] = {
            'token0': token0AmountNormalized,
            'token1': token1AmountNormalized,
            'token0TokenAmt': token0Amount,
            'token1TokenAmt': token1Amount,
            'token0USD': round(token0USD, 2),
            'token1USD': round(token1USD, 2),
            'token0Price': token0Price,
            'token1Price': token1Price,
            'timestamp': timestamp,
        }
        # set same price for next blocks
        if block_num < to_block:
            for block_num in range(block_num + 1, to_block + 1):
                pair_reserves_dict[block_num] = {
                'token0': token0AmountNormalized,
                'token1': token1AmountNormalized,
                'token0TokenAmt': token0Amount,
                'token1TokenAmt': token1Amount,
                'token0USD': round(token0USD, 2),
                'token1USD': round(token1USD, 2),
                'token0Price': token0Price,
                'token1Price': token1Price,
                'timestamp': timestamp,
            }

    core_logger.debug(
        'Calculated pair total reserves for epoch-range: {} - {} | pair_contract: {}',
        from_block,
        to_block,
        pair_address
    )

    # here we store the final block in the epoch reserves in redis so they may be used as
    # a starting point in the next epoch
    end_block = pair_reserves_dict.get(to_block, None)

    if end_block:
        redis_cache_mapping = {
            json.dumps({'blockHeight': to_block, 'token0_reserves': end_block['token0TokenAmt'], 'token1_reserves': end_block['token1TokenAmt']}): int(to_block),
        }
        pipeline = redis_conn.pipeline()
        pipeline.zadd(
            name=uniswap_pair_cached_block_height_reserves.format(Web3.to_checksum_address(pair_address)),
            mapping=redis_cache_mapping,
        )
        pipeline.zremrangebyscore(
            name=uniswap_pair_cached_block_height_reserves.format(
                Web3.to_checksum_address(pair_address),
            ),
            min=0,
            max=to_block - 20,
        )
        await pipeline.execute()

    else:
        core_logger.error(
            (
                'Error attempting to set end block pair total reserves for pair_contract:'
                f' {pair_address} | epoch: {from_block} - {to_block}'
            ),
        )
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Pair reserves computed: {}",
        from_block,
        to_block,
        pair_address,
        pair_reserves_dict
    )
    return pair_reserves_dict


def extract_trade_volume_log(
    event_name,
    log,
    pair_per_token_metadata,
    token0_price_map,
    token1_price_map,
    block_details_dict,
):
    """
    Extract trade volume information from a single event log.

    Args:
        event_name (str): The name of the event (Swap, Mint, or Burn).
        log (dict): The event log data.
        pair_per_token_metadata (dict): Metadata for the token pair.
        token0_price_map (dict): Price map for token0.
        token1_price_map (dict): Price map for token1.
        block_details_dict (dict): Block details including timestamps.

    Returns:
        tuple: A tuple containing trade_data and processed log information.
    """
    token0_amount = 0
    token1_amount = 0
    token0_amount_usd = 0
    token1_amount_usd = 0

    def token_native_and_usd_amount(token, token_type, token_price_map):
        if log['args'].get(token_type) == 0:
            return 0, 0

        token_amount = log['args'].get(token_type) / 10 ** int(
            pair_per_token_metadata[token]['decimals'],
        )
        token_usd_amount = token_amount * token_price_map.get(
            log.get('blockNumber'),
            0,
        )
        return token_amount, token_usd_amount

    if event_name == 'Swap':
        amount0, amount0_usd = token_native_and_usd_amount(
            token='token0',
            token_type='amount0',
            token_price_map=token0_price_map,
        )
        amount1, amount1_usd = token_native_and_usd_amount(
            token='token1',
            token_type='amount1',
            token_price_map=token1_price_map,
        )

        token0_amount = abs(amount0)
        token1_amount = abs(amount1)

        token0_amount_usd = abs(amount0_usd)
        token1_amount_usd = abs(amount1_usd)

    elif event_name == 'Mint' or event_name == 'Burn':
        token0_amount, token0_amount_usd = token_native_and_usd_amount(
            token='token0',
            token_type='amount0',
            token_price_map=token0_price_map,
        )
        token1_amount, token1_amount_usd = token_native_and_usd_amount(
            token='token1',
            token_type='amount1',
            token_price_map=token1_price_map,
        )

    trade_volume_usd = 0
    trade_fee_usd = 0
    fee = int(pair_per_token_metadata['pair']['fee']) / UNISWAPV3_FEE_DIV

    block_details = block_details_dict.get(int(log.get('blockNumber', 0)), {})
    log = json.loads(Web3.to_json(log))
    log['token0_amount'] = token0_amount
    log['token1_amount'] = token1_amount
    log['timestamp'] = block_details.get('timestamp', '')
    # pop unused log props
    log.pop('blockHash', None)
    log.pop('transactionIndex', None)

    # if event is 'Swap' then only add single token in total volume calculation
    if event_name == 'Swap':
        # set one side token value in swap case
        if token1_amount_usd and token0_amount_usd:
            trade_volume_usd = (
                token1_amount_usd
                if token1_amount_usd > token0_amount_usd
                else token0_amount_usd
            )
        else:
            trade_volume_usd = (
                token1_amount_usd if token1_amount_usd else token0_amount_usd
            )

        # calculate uniswap LP fee
        trade_fee_usd = (
            token1_amount_usd * fee
            if token1_amount_usd
            else token0_amount_usd * fee
        )  # uniswap LP fee rate

        # set final usd amount for swap
        log['trade_amount_usd'] = trade_volume_usd

        return (
            trade_data(
                totalTradesUSD=trade_volume_usd,
                totalFeeUSD=trade_fee_usd,
                token0TradeVolume=token0_amount,
                token1TradeVolume=token1_amount,
                token0TradeVolumeUSD=token0_amount_usd,
                token1TradeVolumeUSD=token1_amount_usd,
            ),
            log,
        )

    trade_volume_usd = token0_amount_usd + token1_amount_usd

    # set final usd amount for other events
    log['trade_amount_usd'] = trade_volume_usd

    return (
        trade_data(
            totalTradesUSD=trade_volume_usd,
            totalFeeUSD=trade_fee_usd,
            token0TradeVolume=token0_amount,
            token1TradeVolume=token1_amount,
            token0TradeVolumeUSD=token0_amount_usd,
            token1TradeVolumeUSD=token1_amount_usd,
        ),
        log,
    )


# asynchronously get trades on a pair contract
async def get_pair_trade_volume(
    data_source_contract_address,
    min_chain_height,
    max_chain_height,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_details_dict: dict = dict(),
):
    """
    Fetch and calculate trade volume for a given Uniswap V3 pool contract address over a block range.
    """
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Starting trade volume computation",
        min_chain_height,
        max_chain_height,
        data_source_contract_address
    )

    # Only fetch block details if not provided
    if not block_details_dict:
        try:
            block_details_dict = await get_block_details_in_block_range(
                min_chain_height,
                max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                "[Epoch {}-{}] Pool {} | Failed to fetch block details: {}",
                min_chain_height,
                max_chain_height,
                data_source_contract_address,
                err
            )
            raise err

        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Block details fetched successfully",
            min_chain_height,
            max_chain_height,
            data_source_contract_address
        )

    metadata_processor = MetadataProcessor()
    pair_per_token_metadata = await metadata_processor.get_pool_metadata(
        pool_address=data_source_contract_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )

    if not pair_per_token_metadata:
        core_logger.error(
            "[Epoch {}-{}] Pool {} | Failed to fetch pair metadata",
            min_chain_height,
            max_chain_height,
            data_source_contract_address
        )
        raise Exception(f'Error attempting to get pair metadata for: {data_source_contract_address}')

    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token0.dict(),
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token1.dict(),
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
    )

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Token prices fetched successfully",
        min_chain_height,
        max_chain_height,
        data_source_contract_address
    )

    events = await get_events(
        pair_address=data_source_contract_address,
        rpc=rpc_helper,
        from_block=min_chain_height,
        to_block=max_chain_height,
        redis_con=redis_conn,
    )

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Found {} trade events to process",
        min_chain_height,
        max_chain_height,
        data_source_contract_address,
        len(events)
    )

    # Process events and calculate trade volumes
    trade_data_list = []
    for event in events:
        trade_data = extract_trade_volume_log(
            event_name=event['event'],
            log=event,
            pair_per_token_metadata=pair_per_token_metadata,
            token0_price_map=token0_price_map,
            token1_price_map=token1_price_map,
            block_details_dict=block_details_dict,
        )
        if trade_data:
            trade_data_list.append(trade_data)

    core_logger.info(
        "[Epoch {}-{}] Pool {} | Trade volume computation completed | Processed {} trades",
        min_chain_height,
        max_chain_height,
        data_source_contract_address,
        len(trade_data_list)
    )

    return {
        "Trades": {
            "totalTradesUSD": sum(trade['trade_volume_usd'] for trade in trade_data_list),
            "totalFeeUSD": sum(trade['fee_usd'] for trade in trade_data_list),
            "token0TradeVolume": sum(trade['token0_amount'] for trade in trade_data_list),
            "token1TradeVolume": sum(trade['token1_amount'] for trade in trade_data_list),
            "token0TradeVolumeUSD": sum(trade['token0_amount_usd'] for trade in trade_data_list),
            "token1TradeVolumeUSD": sum(trade['token1_amount_usd'] for trade in trade_data_list),
        },
        "timestamp": block_details_dict[max_chain_height]['timestamp'] if fetch_timestamp else None,
    }


async def get_liquidity_depth(
    pair_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    fetch_timestamp=False,
):
    """
    Calculate liquidity depth for a Uniswap pair over a block range.

    Args:
        pair_address (str): The address of the Uniswap pair contract.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn (aioredis.Redis): Redis connection for caching.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        fetch_timestamp (bool): Whether to fetch block timestamps.

    Returns:
        dict: A dictionary containing liquidity depth data for each block in the range.
    """
    liquidity_depth_dict = dict()
    core_logger.debug(
        f'Starting liquidity depth query for: {pair_address}',
    )
    if fetch_timestamp:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block,
                to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                (
                    'Error attempting to get block details of block-range'
                    ' {}-{}: {}, retrying again'
                ),
                from_block,
                to_block,
                err,
            )
            raise err
    else:
        block_details_dict = dict()

    pair_address = Web3.to_checksum_address(pair_address)
    metadata_processor = MetadataProcessor()
    pair_per_token_metadata = await metadata_processor.get_pool_metadata(
        pool_address=pair_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )
    if not pair_per_token_metadata:
        core_logger.error(
            "[Epoch {}-{}] Pool {} | Failed to fetch pair metadata",
            from_block,
            to_block,
            pair_address
        )
        raise Exception(f'Error attempting to get pair metadata for: {pair_address}')

    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token0.dict(),
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,

        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata.token1.dict(),
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,

        ),
    )

    core_logger.debug('grabbed pair per token metadata for liquidity depth')
    # grab ticks in range and calculate initial liquidity depth

    ticks_list, slot0 = await get_tick_info(
        rpc_helper=rpc_helper,
        pair_address=pair_address,
        from_block=from_block,
        redis_conn=redis_conn,
        pair_per_token_metadata=pair_per_token_metadata
    )

    liquidity_depth_initial = calculate_liquidity_depth(
        ticks_list,
        slot0[0],
        pair_per_token_metadata,
    )

    for block_num in range(from_block, to_block + 1):
        liquidity_depth_dict[block_num] = liquidity_depth_initial
        liquidity_depth_dict[block_num]['prices'] = {
            'token0': token0_price_map.get(block_num, 0),
            'token1': token1_price_map.get(block_num, 0),
        }

    events_by_block = await get_events_from_cache(
        pool_address=pair_address,
        redis_conn=redis_conn,
        from_block=from_block,
        to_block=to_block,
    )

    core_logger.debug(
        f'Events fetched for liquidity depth: {events_by_block}',
    )

    for block_num in range(from_block + 1, to_block + 1):

        events = events_by_block.get(block_num, [])
        for event in events:
            amount0 = event.args['amount0']
            amount1 = event.args['amount1']
            if event.eventName == 'Mint':
                liquidity_depth_dict[block_num]['token0']['amount'] += amount0
                liquidity_depth_dict[block_num]['token1']['amount'] += amount1
            else:
                liquidity_depth_dict[block_num]['token0']['amount'] -= amount0
                liquidity_depth_dict[block_num]['token1']['amount'] -= amount1

        current_block_details = block_details_dict.get(block_num, None)

        timestamp = (
            current_block_details.get(
                'timestamp',
                None,
            )
            if current_block_details
            else None
        )
        liquidity_depth_dict[block_num]['timestamp'] = timestamp

    return liquidity_depth_dict


def calculate_liquidity_depth(
    ticks,
    sqrt_price,
    pair_metadata,
):
    """
    Calculate liquidity depth based on ticks and current price.

    Args:
        ticks (list): List of tick data.
        sqrt_price (int): Current square root price.
        pair_metadata (dict): Metadata for the token pair.

    Returns:
        dict: A dictionary containing liquidity depth information.
    """
    liquidity_depth_dict = dict()
    sqrt_price = sqrt_price / 2 ** 96
    liquidity_total = 0
    token0_liquidity = 0
    token1_liquidity = 0
    tick_spacing = 10

    if len(ticks) == 0:
        return (0, 0)

    if pair_metadata['pair']['fee'] == 3000:
        tick_spacing = 60
    elif pair_metadata['pair']['fee'] == 10000:
        tick_spacing = 200
# https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf
    for i in range(len(ticks)):
        tick = ticks[i]
        liquidity_net = tick['liquidity_net']
        idx = tick['idx']
        next_idx = idx + ticks[i + 1]['idx'] if i < len(ticks) - 1 else idx + tick_spacing
        liquidity_total += liquidity_net
        sqrtPriceLow = 1.0001 ** (idx // 2)
        sqrtPriceHigh = 1.0001 ** ((next_idx) // 2)
        if sqrt_price <= sqrtPriceLow:
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
            liquidity_depth_dict[idx] = {
                'token0': {
                    'address': pair_metadata['token0']['address'],
                    'amount': abs(token0_liquidity),
                    'decimals': pair_metadata['token0']['decimals'],
                },
                'token1': {
                    'address': pair_metadata['token1']['address'],
                    'amount': 0,
                    'decimals': pair_metadata['token1']['decimals'],
                },

            }
        elif sqrt_price >= sqrtPriceHigh:
            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrtPriceHigh,
            )
            liquidity_depth_dict[idx] = {
                'token0': {
                    'token': pair_metadata['token1']['address'],
                    'amount': abs(token1_liquidity),
                    'decimals': pair_metadata['token1']['decimals'],
                },
                'token1': {
                    'token': pair_metadata['token0']['address'],
                    'amount': 0,
                    'decimals': pair_metadata['token0']['decimals'],
                },

            }
        else:
            token0_liquidity += get_token0_in_pool(
                liquidity_total,
                sqrt_price,
                sqrtPriceHigh,
            )

            token1_liquidity += get_token1_in_pool(
                liquidity_total,
                sqrtPriceLow,
                sqrt_price,
            )

            liquidity_depth_dict[idx] = {
                'token0': {
                    'token': pair_metadata['token0']['address'],
                    'amount': abs(token0_liquidity),
                    'decimals': pair_metadata['token0']['decimals'],
                },
                'token1': {
                    'token': pair_metadata['token1']['address'],
                    'amount': abs(token1_liquidity),
                    'decimals': pair_metadata['token1']['decimals'],
                },

            }

    return liquidity_depth_dict