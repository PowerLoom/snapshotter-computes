import asyncio
from distutils import core
import json
from functools import reduce
import time
from typing import Dict, List, Optional, Any, Tuple

from redis import asyncio as aioredis
from computes.metadata import MetadataProcessor
from computes.utils.models.message_models import UniswapBaseSnapshot, UniswapPoolMetadata, EpochBaseSnapshot
from snapshotter.utils.default_logger import logger
from rpc_helper.rpc import get_event_sig_and_abi
from rpc_helper.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.redis_keys import uniswap_pair_cached_block_height_reserves
from computes.total_value_locked import calculate_reserves
from computes.total_value_locked import get_slot0_data_for_block_range
from computes.total_value_locked import get_tick_info
from computes.total_value_locked import get_token0_in_pool
from computes.total_value_locked import get_token1_in_pool
from computes.utils.constants import UNISWAP_EVENTS_ABI
from computes.utils.constants import UNISWAP_TRADE_EVENT_SIGS
from computes.utils.constants import UNISWAPV3_FEE_DIV
from computes.utils.helpers import get_events_from_cache, get_pair_metadata
from computes.utils.models.data_models import UniswapEvent, epoch_event_trade_data, UniswapProcessedLog
from computes.utils.models.data_models import event_trade_data
from computes.utils.models.data_models import PairBlockDetail
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
) -> Optional[UniswapBaseSnapshot]:
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
        Optional[UniswapBaseSnapshot]: A snapshot containing pair reserves data for each block in the range.
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

    # grab mint/burn events in range
    events: Dict[int, List[UniswapEvent]] = await get_events_from_cache(
        pool_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    core_logger.info(
        "[Epoch {}-{}] Pool {} | Found {} events to process",
        from_block,
        to_block,
        pair_address,
        len(events)
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

        # Not used for now, here as an example
        slot0_data_dict = await get_slot0_data_for_block_range(
            rpc_helper=rpc_helper,
            pair_address=pair_address,
            from_block=from_block,
            to_block=to_block,
        )
    else:
        # Calculate reserves at the end of the previous block (from_block - 1)
        initial_reserves, slot0_data_dict = await calculate_reserves(
            pair_address=pair_address,
            from_block=from_block - 1,
            to_block=to_block,
            pair_per_token_metadata=pair_per_token_metadata,
            rpc_helper=rpc_helper,
        )
        core_logger.info(
            "[Epoch {}-{}] Pool {} | Calculated initial reserves: token0={}, token1={}",
            from_block,
            to_block,
            pair_address,
            initial_reserves[0],
            initial_reserves[1]
        )

        if not initial_reserves or any(x == 0 for x in initial_reserves):
            core_logger.error(
                "[Epoch {}-{}] Pool {} | Failed to calculate initial reserves",
                from_block,
                to_block,
                pair_address
            )
            return None
    
    # TODO: decide best way to handle failed slot0 data fetch
    if not slot0_data_dict:
        core_logger.error(
            "[Epoch {}-{}] Pool {} | Failed to fetch slot0 data",
            from_block,
            to_block,
            pair_address
        )

    # Initialize accumulators for epoch-wide trade data
    epoch_total_trade_data = trade_data(
        totalTradesUSD=0.0, totalFeeUSD=0.0, 
        token0TradeVolume=0.0, token1TradeVolume=0.0,
        token0TradeVolumeUSD=0.0, token1TradeVolumeUSD=0.0
    )

    # sum burn and mint each block
    token0Amount = initial_reserves[0]
    token1Amount = initial_reserves[1]
    

    pair_reserves_dict = dict()
    for block_num in range(from_block, to_block + 1):
        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))

        current_token0_usd_price = token0_price_map.get(block_num, 0)
        current_token1_usd_price = token1_price_map.get(block_num, 0)

        token0ReservesUSD = token0AmountNormalized * current_token0_usd_price
        token1ReservesUSD = token1AmountNormalized * current_token1_usd_price
        
        token0_price_in_token1 = current_token0_usd_price / current_token1_usd_price if current_token1_usd_price != 0 else 0.0
        token1_price_in_token0 = current_token1_usd_price / current_token0_usd_price if current_token0_usd_price != 0 else 0.0

        pair_reserves_dict[block_num] = PairBlockDetail(
            token0ReservesNormalized=token0AmountNormalized,
            token1ReservesNormalized=token1AmountNormalized,
            token0Reserves=token0Amount,
            token1Reserves=token1Amount,
            token0ReservesUSD=token0ReservesUSD,
            token1ReservesUSD=token1ReservesUSD,
            token0Price=current_token0_usd_price,
            token1Price=current_token1_usd_price,
            token0PriceInToken1=token0_price_in_token1,
            token1PriceInToken0=token1_price_in_token0,
            timestamp=block_details_dict.get(block_num, {}).get('timestamp', 0),
        )

    # sort access by block number
    for block_num in sorted(events.keys()):
        event_list = events.get(block_num, [])
        if not event_list:
            core_logger.info(
                "[Epoch {}-{}] Pool {} | Block {} | No events found in cache in get_pair_reserves",
                from_block,
                to_block,
                pair_address,
                block_num
            )
            continue
        else:
            core_logger.info(
                "[Epoch {}-{}] Pool {} | Block {} | Found {} events in cache in get_pair_reserves",
                from_block,
                to_block,
                pair_address,
                block_num,
                len(event_list)
            )
        
        # Process each event for trade volume and update reserve accumulators for the current block
        block_delta_token0 = 0
        block_delta_token1 = 0

        for event_data_obj in event_list:
            # Extract trade volume for this event and add to epoch total
            try:
                current_event_trade_data, _ = extract_trade_volume_log(
                    event_name=event_data_obj.eventName,
                    log=event_data_obj,
                    pair_per_token_metadata=pair_per_token_metadata,
                    token0_price_map=token0_price_map,
                    token1_price_map=token1_price_map,
                    block_details_dict=block_details_dict,
                )
                if current_event_trade_data:
                    epoch_total_trade_data += current_event_trade_data # Uses the __add__ method
            except Exception as e_extract:
                core_logger.opt(exception=True).error(
                    "[Epoch {}-{}] Pool {} | Block {} | Error during extract_trade_volume_log for event {}: {}",
                    from_block, to_block, pair_address, block_num, event_data_obj.txHash, e_extract
                )

            # Accumulate reserve changes from events for this block
            if event_data_obj.eventName == 'Burn':
                block_delta_token0 -= event_data_obj.args['amount0']
                block_delta_token1 -= event_data_obj.args['amount1']
            else:
                # Mint and Swap events are handled the same way
                # Swap events use a negative value for the token that was removed from the pool
                block_delta_token0 += event_data_obj.args['amount0']
                block_delta_token1 += event_data_obj.args['amount1']

        # Apply accumulated deltas for this block to total reserves
        token0Amount += block_delta_token0
        token1Amount += block_delta_token1
        
        # Update the pair_reserves_dict for the current block_num with the new totals
        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata.token0.decimals))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata.token1.decimals))

        # USD prices for the current block_num
        # These are the same variables as used in the update dict below, but defined here for clarity
        current_block_token0_usd_price = token0_price_map.get(block_num, 0) 
        current_block_token1_usd_price = token1_price_map.get(block_num, 0)

        token0ReservesUSD = token0AmountNormalized * current_block_token0_usd_price
        token1ReservesUSD = token1AmountNormalized * current_block_token1_usd_price

        # Calculate cross prices for the current block_num
        token0_price_in_t1 = current_block_token0_usd_price / current_block_token1_usd_price if current_block_token1_usd_price != 0 else 0.0
        token1_price_in_t0 = current_block_token1_usd_price / current_block_token0_usd_price if current_block_token0_usd_price != 0 else 0.0

        current_block_details = block_details_dict.get(block_num, None)

        timestamp = (
            current_block_details.get(
                'timestamp',
                None,
            )
            if current_block_details
            else None
        )

        pair_reserves_dict[block_num] = PairBlockDetail(
            token0ReservesNormalized=token0AmountNormalized,
            token1ReservesNormalized=token1AmountNormalized,
            token0Reserves=token0Amount,
            token1Reserves=token1Amount,
            token0ReservesUSD=token0ReservesUSD,
            token1ReservesUSD=token1ReservesUSD,
            token0Price=current_block_token0_usd_price,
            token1Price=current_block_token1_usd_price,
            token0PriceInToken1=token0_price_in_t1,
            token1PriceInToken0=token1_price_in_t0,
            timestamp=timestamp,
        )
        

        # set same price for next blocks
        if block_num < to_block:
            for block_num_ffill in range(block_num + 1, to_block + 1):
                # Not a completely necessary check, but it's a sanity check
                if block_num_ffill not in events:
                    pair_reserves_dict[block_num_ffill] = PairBlockDetail(
                        token0ReservesNormalized=token0AmountNormalized,
                        token1ReservesNormalized=token1AmountNormalized,
                        token0Reserves=token0Amount,
                        token1Reserves=token1Amount,
                        token0ReservesUSD=token0ReservesUSD,
                        token1ReservesUSD=token1ReservesUSD,
                        token0Price=current_block_token0_usd_price,
                        token1Price=current_block_token1_usd_price,
                        token0PriceInToken1=token0_price_in_t1,
                        token1PriceInToken0=token1_price_in_t0,
                        timestamp=timestamp,
                    )

    core_logger.debug(
        'Calculated pair total reserves for epoch-range: {} - {} | pair_contract: {}',
        from_block,
        to_block,
        pair_address
    )

    # here we store the final block in the epoch reserves in redis so they may be used as
    # a starting point in the next epoch
    end_block_data_for_cache = pair_reserves_dict.get(to_block, None)

    if end_block_data_for_cache:
        redis_cache_mapping = {
            json.dumps({
                'blockHeight': to_block, 
                'token0_reserves': end_block_data_for_cache.token0Reserves, 
                'token1_reserves': end_block_data_for_cache.token1Reserves
            }): int(to_block),
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
        "Data available in returned snapshot." 
    )

    # Populate per-block data for the snapshot
    token0ReservesSnap = {}
    token1ReservesSnap = {}
    token0ReservesUSDSnap = {}
    token1ReservesUSDSnap = {}
    token0PricesSnap = {}
    token1PricesSnap = {}
    token0PricesUSDSnap = {}
    token1PricesUSDSnap = {}
    timestampsSnap = {}

    for block_num_snap in range(from_block, to_block + 1):
        block_data_obj: Optional[PairBlockDetail] = pair_reserves_dict.get(block_num_snap, {})

        token0ReservesSnap[block_num_snap] = block_data_obj.token0ReservesNormalized
        token1ReservesSnap[block_num_snap] = block_data_obj.token1ReservesNormalized
        token0ReservesUSDSnap[block_num_snap] = block_data_obj.token0ReservesUSD
        token1ReservesUSDSnap[block_num_snap] = block_data_obj.token1ReservesUSD
        token0PricesSnap[block_num_snap] = block_data_obj.token0PriceInToken1
        token1PricesSnap[block_num_snap] = block_data_obj.token1PriceInToken0
        token0PricesUSDSnap[block_num_snap] = block_data_obj.token0Price # USD price of token0
        token1PricesUSDSnap[block_num_snap] = block_data_obj.token1Price # USD price of token1
        timestampsSnap[block_num_snap] = block_data_obj.timestamp

    snapshot_timestamp = 0  # Default timestamp
    end_block_data: Optional[PairBlockDetail] = pair_reserves_dict.get(to_block)
    if end_block_data and end_block_data.timestamp is not None:
        snapshot_timestamp = end_block_data.timestamp
    elif to_block in block_details_dict: # Fallback 1 if end_block_data or its timestamp is None/0
        snapshot_timestamp = block_details_dict[to_block].get('timestamp', 0)
    
    if not snapshot_timestamp: # Final fallback if still 0
        snapshot_timestamp = int(time.time())
        core_logger.warning(f"[Epoch {from_block}-{to_block}] Pool {pair_address} | Snapshot timestamp defaulted to current time.")


    base_reserves_snapshot = UniswapBaseSnapshot(
        address=pair_address,
        epoch=EpochBaseSnapshot(
            begin=from_block,
            end=to_block,
        ),
        timestamps=snapshot_timestamp,
        token0=Web3.to_checksum_address(pair_per_token_metadata.token0.address),
        token1=Web3.to_checksum_address(pair_per_token_metadata.token1.address),
        token0Reserves=token0ReservesSnap,
        token1Reserves=token1ReservesSnap,
        token0ReservesUSD=token0ReservesUSDSnap,
        token1ReservesUSD=token1ReservesUSDSnap,
        token0Prices=token0PricesSnap,
        token1Prices=token1PricesSnap,
        token0PricesUSD=token0PricesUSDSnap,
        token1PricesUSD=token1PricesUSDSnap,
        # Add aggregated trade volume data
        totalTrade=epoch_total_trade_data.totalTradesUSD,
        totalFee=epoch_total_trade_data.totalFeeUSD,
        token0TradeVolume=epoch_total_trade_data.token0TradeVolume,
        token1TradeVolume=epoch_total_trade_data.token1TradeVolume,
        token0TradeVolumeUSD=epoch_total_trade_data.token0TradeVolumeUSD,
        token1TradeVolumeUSD=epoch_total_trade_data.token1TradeVolumeUSD,
        timestamp=snapshot_timestamp,
    )
    return base_reserves_snapshot


def extract_trade_volume_log(
    event_name: str,
    log: UniswapEvent,
    pair_per_token_metadata: UniswapPoolMetadata,
    token0_price_map: Dict[int, float],
    token1_price_map: Dict[int, float],
    block_details_dict: Dict[int, Dict[str, Any]],
) -> Tuple[trade_data, UniswapProcessedLog]:
    """
    Extract trade volume information from a single event log.

    Args:
        event_name (str): The name of the event (Swap, Mint, or Burn).
        log (UniswapEvent): The event log data as a Pydantic model.
        pair_per_token_metadata (UniswapPoolMetadata): Metadata for the token pair.
        token0_price_map (dict): Price map for token0.
        token1_price_map (dict): Price map for token1.
        block_details_dict (dict): Block details including timestamps.

    Returns:
        tuple: A tuple containing trade_data and UniswapProcessedLog model instance.
    """
    token0_amount = 0.0
    token1_amount = 0.0
    token0_amount_usd = 0.0
    token1_amount_usd = 0.0

    def token_native_and_usd_amount(token_key: str, token_type: str, current_token_price_map: Dict[int, float]):
        if log.args.get(token_type) == 0:
            return 0.0, 0.0

        token_specific_metadata = getattr(pair_per_token_metadata, token_key)

        amount = log.args.get(token_type) / 10 ** int(
            token_specific_metadata.decimals,
        )
        usd_amount = amount * current_token_price_map.get(
            log.blockNumber,
            0,
        )
        return amount, usd_amount

    if event_name == 'Swap':
        amount0, amount0_usd = token_native_and_usd_amount(
            token_key='token0',
            token_type='amount0',
            current_token_price_map=token0_price_map,
        )
        amount1, amount1_usd = token_native_and_usd_amount(
            token_key='token1',
            token_type='amount1',
            current_token_price_map=token1_price_map,
        )

        token0_amount = abs(amount0)
        token1_amount = abs(amount1)

        token0_amount_usd = abs(amount0_usd)
        token1_amount_usd = abs(amount1_usd)

    elif event_name == 'Mint' or event_name == 'Burn':
        token0_amount, token0_amount_usd = token_native_and_usd_amount(
            token_key='token0',
            token_type='amount0',
            current_token_price_map=token0_price_map,
        )
        token1_amount, token1_amount_usd = token_native_and_usd_amount(
            token_key='token1',
            token_type='amount1',
            current_token_price_map=token1_price_map,
        )

    trade_volume_usd = 0.0
    trade_fee_usd = 0.0
    fee = int(pair_per_token_metadata.fee) / UNISWAPV3_FEE_DIV

    block_details = block_details_dict.get(log.blockNumber, {})
    current_timestamp = block_details.get('timestamp', None)

    # Determine trade_volume_usd based on event type
    if event_name == 'Swap':
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
        trade_fee_usd = (
            token1_amount_usd * fee
            if token1_amount_usd
            else token0_amount_usd * fee
        )
    else: # Mint or Burn
        trade_volume_usd = token0_amount_usd + token1_amount_usd
        # trade_fee_usd remains 0 for Mint/Burn as per original logic

    # Create the UniswapProcessedLog instance
    # Prepare data for UniswapProcessedLog using Pydantic V2 method
    processed_log_data_for_init = log.model_dump(by_alias=True)

    processed_log = UniswapProcessedLog(
        **processed_log_data_for_init,
        token0_amount=token0_amount,
        token1_amount=token1_amount,
        timestamp=current_timestamp,
        trade_amount_usd=trade_volume_usd
    )

    return (
        trade_data(
            totalTradesUSD=trade_volume_usd,
            totalFeeUSD=trade_fee_usd,
            token0TradeVolume=token0_amount,
            token1TradeVolume=token1_amount,
            token0TradeVolumeUSD=token0_amount_usd,
            token1TradeVolumeUSD=token1_amount_usd,
        ),
        processed_log,
    )


# asynchronously get trades on a pair contract
async def get_pair_trade_volume(
    pair_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    metadata_processor: Optional[MetadataProcessor] = None,
    block_details_dict: dict = dict(),
):
    """
    Fetch and calculate trade volume for a given Uniswap V3 pool contract address over a block range.
    """
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Starting trade volume computation",
        from_block,
        to_block,
        pair_address
    )

    # Ensure consistent address casing for cache lookups and further processing
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
            raise err

        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Block details fetched successfully",
            from_block,
            to_block,
            pair_address
        )

    if not metadata_processor:
        metadata_processor = MetadataProcessor()

    pair_per_token_metadata = await metadata_processor.get_pool_metadata(
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
    )

    core_logger.debug(
        "[Epoch {}-{}] Pool {} | Token prices fetched successfully",
        from_block,
        to_block,
        pair_address
    )

    events_by_block = await get_events_from_cache(
        pool_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Found {} events_by_block entries to process",
        from_block,
        to_block,
        pair_address,
        len(events_by_block)
    )

    # Process events and calculate trade volumes
    processed_trades_list: List[UniswapProcessedLog] = []
    total_events_attempted = 0
    for block_num, events_in_block in events_by_block.items(): 
        core_logger.debug(
            "[Epoch {}-{}] Pool {} | Block {} | Found {} events in this block to attempt processing.",
            from_block, to_block, pair_address, block_num, len(events_in_block)
        )
        total_events_attempted += len(events_in_block)
        for event_to_process in events_in_block: 
            core_logger.debug(
                "[Epoch {}-{}] Pool {} | Block {} | Processing event: {}",
                from_block, to_block, pair_address, block_num, event_to_process.model_dump_json(indent=2)
            )
            try:
                returned_trade_data, processed_log_event_candidate = extract_trade_volume_log(
                    event_name=event_to_process.eventName,
                    log=event_to_process,
                    pair_per_token_metadata=pair_per_token_metadata,
                    token0_price_map=token0_price_map,
                    token1_price_map=token1_price_map,
                    block_details_dict=block_details_dict,
                )
                core_logger.debug(
                    "[Epoch {}-{}] Pool {} | Block {} | extract_trade_volume_log returned: trade_data={}, processed_log_event={}",
                    from_block, to_block, pair_address, block_num,
                    returned_trade_data.model_dump_json(indent=2) if returned_trade_data else "None",
                    processed_log_event_candidate.model_dump_json(indent=2) if processed_log_event_candidate else "None"
                )

                if processed_log_event_candidate:
                    processed_trades_list.append(processed_log_event_candidate)
                else:
                    core_logger.warning(
                        "[Epoch {}-{}] Pool {} | Block {} | Event {} with name '{}' processed by extract_trade_volume_log but resulted in no UniswapProcessedLog object (was None). Skipping.",
                        from_block, to_block, pair_address, block_num, event_to_process.txHash, event_to_process.eventName
                    )
            except Exception as e_extract:
                core_logger.opt(exception=True).error(
                    "[Epoch {}-{}] Pool {} | Block {} | Exception during extract_trade_volume_log for event {} with name '{}': {}",
                    from_block, to_block, pair_address, block_num, event_to_process.txHash, event_to_process.eventName, e_extract
                )

    # More precise initial log based on total events found across all blocks in events_by_block
    if total_events_attempted > 0 and not processed_trades_list:
        core_logger.warning(
            "[Epoch {}-{}] Pool {} | Attempted to process {} events from cache, but none resulted in a UniswapProcessedLog.",
            from_block, to_block, pair_address, total_events_attempted
        )
    
    core_logger.info(
        "[Epoch {}-{}] Pool {} | Trade volume computation completed | Processed {} trades (from {} events attempted in cache)",
        from_block,
        to_block,
        pair_address,
        len(processed_trades_list),
        total_events_attempted
    )

    return {
        "address": pair_address,
        "epoch": {"begin": from_block, "end": to_block},
        "trades": processed_trades_list,
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