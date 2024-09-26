import asyncio
import json

from redis import asyncio as aioredis
from web3 import Web3

from computes.utils.constants import pair_contract_abi
from computes.utils.constants import UNISWAP_EVENTS_ABI
from computes.utils.constants import UNISWAP_TRADE_EVENT_SIGS
from computes.utils.helpers import get_pair_metadata
from computes.utils.models.data_models import epoch_event_trade_data
from computes.utils.models.data_models import event_trade_data
from computes.utils.models.data_models import trade_data
from computes.utils.pricing import (
    get_token_price_in_block_range,
)
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)

core_logger = logger.bind(module='PowerLoom|UniswapCore')


async def get_pair_reserves(
    pair_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    fetch_timestamp=False,
):
    """
    Fetch and calculate pair reserves for a given pair address and block range.

    Args:
        pair_address (str): The address of the pair contract.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for blockchain interactions.
        fetch_timestamp (bool): Whether to fetch block timestamps.

    Returns:
        dict: A dictionary containing pair reserves data for each block in the range.
    """
    core_logger.debug(
        f'Starting pair total reserves query for: {pair_address}',
    )
    pair_address = Web3.to_checksum_address(pair_address)

    # Fetch block details if timestamp is required
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

    # Fetch pair metadata
    pair_per_token_metadata = await get_pair_metadata(
        pair_address=pair_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    core_logger.debug(
        (
            'total pair reserves fetched block details for epoch for:'
            f' {pair_address}'
        ),
    )

    # Fetch token prices for both tokens in the pair
    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token0'],
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token1'],
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
    )

    core_logger.debug(
        f'Total reserves fetched token prices for: {pair_address}',
    )

    # Create dictionary of ABI {function_name -> {signature, abi, input, output}}
    pair_abi_dict = get_contract_abi_dict(pair_contract_abi)

    # Fetch reserves for the block range
    reserves_array = await rpc_helper.batch_eth_call_on_block_range(
        abi_dict=pair_abi_dict,
        function_name='getReserves',
        contract_address=pair_address,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    core_logger.debug(
        f'Total reserves fetched getReserves results: {pair_address}',
    )
    token0_decimals = pair_per_token_metadata['token0']['decimals']
    token1_decimals = pair_per_token_metadata['token1']['decimals']

    # Calculate and store pair reserves for each block
    pair_reserves_arr = dict()
    block_count = 0
    for block_num in range(from_block, to_block + 1):
        token0Amount = (
            reserves_array[block_count][0] / 10 ** int(token0_decimals)
            if reserves_array[block_count][0]
            else 0
        )
        token1Amount = (
            reserves_array[block_count][1] / 10 ** int(token1_decimals)
            if reserves_array[block_count][1]
            else 0
        )

        token0USD = token0Amount * token0_price_map.get(block_num, 0)
        token1USD = token1Amount * token1_price_map.get(block_num, 0)

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

        pair_reserves_arr[block_num] = {
            'token0': token0Amount,
            'token1': token1Amount,
            'token0USD': token0USD,
            'token1USD': token1USD,
            'token0Price': token0Price,
            'token1Price': token1Price,
            'timestamp': timestamp,
        }
        block_count += 1

    core_logger.debug(
        (
            'Calculated pair total reserves for epoch-range:'
            f' {from_block} - {to_block} | pair_contract: {pair_address}'
        ),
    )
    return pair_reserves_arr


def extract_trade_volume_log(
    event_name,
    log,
    pair_per_token_metadata,
    token0_price_map,
    token1_price_map,
    block_details_dict,
):
    """
    Extract trade volume information from a log entry.

    Args:
        event_name (str): The name of the event (Swap, Mint, or Burn).
        log (dict): The log entry containing event data.
        pair_per_token_metadata (dict): Metadata for the token pair.
        token0_price_map (dict): Price map for token0.
        token1_price_map (dict): Price map for token1.
        block_details_dict (dict): Block details dictionary.

    Returns:
        tuple: A tuple containing trade_data and processed log.
    """
    token0_amount = 0
    token1_amount = 0
    token0_amount_usd = 0
    token1_amount_usd = 0

    def token_native_and_usd_amount(token, token_type, token_price_map):
        """Helper function to calculate token amount and USD value."""
        if log.args.get(token_type) <= 0:
            return 0, 0

        token_amount = log.args.get(token_type) / 10 ** int(
            pair_per_token_metadata[token]['decimals'],
        )
        token_usd_amount = token_amount * token_price_map.get(
            log.get('blockNumber'), 0,
        )
        return token_amount, token_usd_amount

    if event_name == 'Swap':
        # Calculate amounts for Swap event
        amount0In, amount0In_usd = token_native_and_usd_amount(
            token='token0',
            token_type='amount0In',
            token_price_map=token0_price_map,
        )
        amount0Out, amount0Out_usd = token_native_and_usd_amount(
            token='token0',
            token_type='amount0Out',
            token_price_map=token0_price_map,
        )
        amount1In, amount1In_usd = token_native_and_usd_amount(
            token='token1',
            token_type='amount1In',
            token_price_map=token1_price_map,
        )
        amount1Out, amount1Out_usd = token_native_and_usd_amount(
            token='token1',
            token_type='amount1Out',
            token_price_map=token1_price_map,
        )

        token0_amount = abs(amount0Out - amount0In)
        token1_amount = abs(amount1Out - amount1In)

        token0_amount_usd = abs(amount0Out_usd - amount0In_usd)
        token1_amount_usd = abs(amount1Out_usd - amount1In_usd)

    elif event_name == 'Mint' or event_name == 'Burn':
        # Calculate amounts for Mint or Burn event
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

    block_details = block_details_dict.get(int(log.get('blockNumber', 0)), {})
    log = json.loads(Web3.to_json(log))
    log['token0_amount'] = token0_amount
    log['token1_amount'] = token1_amount
    log['timestamp'] = block_details.get('timestamp', '')
    # Remove unused log properties
    log.pop('blockHash', None)
    log.pop('transactionIndex', None)

    # Calculate trade volume and fee for Swap event
    if event_name == 'Swap':
        # Set one side token value in swap case
        if token1_amount_usd and token0_amount_usd:
            trade_volume_usd = max(token1_amount_usd, token0_amount_usd)
        else:
            trade_volume_usd = token1_amount_usd or token0_amount_usd

        # Calculate Uniswap LP fee (0.3%)
        trade_fee_usd = trade_volume_usd * 0.003

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

    # Calculate trade volume for Mint and Burn events
    trade_volume_usd = token0_amount_usd + token1_amount_usd
    log['trade_amount_usd'] = trade_volume_usd

    return (
        trade_data(
            totalTradesUSD=trade_volume_usd,
            totalFeeUSD=0.0,
            token0TradeVolume=token0_amount,
            token1TradeVolume=token1_amount,
            token0TradeVolumeUSD=token0_amount_usd,
            token1TradeVolumeUSD=token1_amount_usd,
        ),
        log,
    )


async def get_pair_trade_volume(
    data_source_contract_address,
    min_chain_height,
    max_chain_height,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    fetch_timestamp=True,
):
    """
    Fetch and calculate trade volume for a pair contract within a specified block range.

    Args:
        data_source_contract_address (str): The address of the pair contract.
        min_chain_height (int): The starting block number.
        max_chain_height (int): The ending block number.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object for blockchain interactions.
        fetch_timestamp (bool): Whether to fetch block timestamps.

    Returns:
        dict: A dictionary containing trade volume data for the specified block range.
    """

    data_source_contract_address = Web3.to_checksum_address(
        data_source_contract_address,
    )
    block_details_dict = dict()

    # Fetch block details if timestamp is required
    if fetch_timestamp:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                (
                    'Error attempting to get block details of to_block {}:'
                    ' {}, retrying again'
                ),
                max_chain_height,
                err,
            )
            raise err

    # Fetch pair metadata and token prices
    pair_per_token_metadata = await get_pair_metadata(
        pair_address=data_source_contract_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )
    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token0'],
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token1'],
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            debug_log=False,
        ),
    )

    # Fetch event signature and ABI
    event_sig, event_abi = get_event_sig_and_abi(
        UNISWAP_TRADE_EVENT_SIGS,
        UNISWAP_EVENTS_ABI,
    )

    # Fetch logs for swap, mint & burn events
    events_log = await rpc_helper.get_events_logs(
        **{
            'contract_address': data_source_contract_address,
            'to_block': max_chain_height,
            'from_block': min_chain_height,
            'topics': [event_sig],
            'event_abi': event_abi,
            'redis_conn': redis_conn,
        },
    )

    # Group logs by transaction hashes
    grouped_by_tx = dict()
    for log in events_log:
        tx_hash = log.transactionHash.hex()
        if tx_hash in grouped_by_tx:
            grouped_by_tx[tx_hash].append(log)
        else:
            grouped_by_tx[tx_hash] = [log]

    # Initialize data models with empty/0 values
    epoch_results = epoch_event_trade_data(
        Swap=event_trade_data(
            logs=[],
            trades=trade_data(
                totalTradesUSD=float(),
                totalFeeUSD=float(),
                token0TradeVolume=float(),
                token1TradeVolume=float(),
                token0TradeVolumeUSD=float(),
                token1TradeVolumeUSD=float(),
                recent_transaction_logs=list(),
            ),
        ),
        Mint=event_trade_data(
            logs=[],
            trades=trade_data(
                totalTradesUSD=float(),
                totalFeeUSD=float(),
                token0TradeVolume=float(),
                token1TradeVolume=float(),
                token0TradeVolumeUSD=float(),
                token1TradeVolumeUSD=float(),
                recent_transaction_logs=list(),
            ),
        ),
        Burn=event_trade_data(
            logs=[],
            trades=trade_data(
                totalTradesUSD=float(),
                totalFeeUSD=float(),
                token0TradeVolume=float(),
                token1TradeVolume=float(),
                token0TradeVolumeUSD=float(),
                token1TradeVolumeUSD=float(),
                recent_transaction_logs=list(),
            ),
        ),
        Trades=trade_data(
            totalTradesUSD=float(),
            totalFeeUSD=float(),
            token0TradeVolume=float(),
            token1TradeVolume=float(),
            token0TradeVolumeUSD=float(),
            token1TradeVolumeUSD=float(),
            recent_transaction_logs=list(),
        ),
    )

    # Process logs and calculate trade volumes
    for tx_hash, logs in grouped_by_tx.items():
        # Initialize temporary trade object to track trades at txHash level
        tx_hash_trades = trade_data(
            totalTradesUSD=float(),
            totalFeeUSD=float(),
            token0TradeVolume=float(),
            token1TradeVolume=float(),
            token0TradeVolumeUSD=float(),
            token1TradeVolumeUSD=float(),
            recent_transaction_logs=list(),
        )
        # Sort logs to process Burn events last
        logs = sorted(logs, key=lambda x: x.event, reverse=True)

        # Process each log in the transaction
        for log in logs:
            trades_result, processed_log = extract_trade_volume_log(
                event_name=log.event,
                log=log,
                pair_per_token_metadata=pair_per_token_metadata,
                token0_price_map=token0_price_map,
                token1_price_map=token1_price_map,
                block_details_dict=block_details_dict,
            )

            if log.event == 'Swap':
                epoch_results.Swap.logs.append(processed_log)
                epoch_results.Swap.trades += trades_result
                tx_hash_trades += trades_result  # Add swap to transaction total
            elif log.event == 'Mint':
                epoch_results.Mint.logs.append(processed_log)
                epoch_results.Mint.trades += trades_result
            elif log.event == 'Burn':
                epoch_results.Burn.logs.append(processed_log)
                epoch_results.Burn.trades += trades_result

        # Add absolute value of transaction trades to total trades
        epoch_results.Trades += abs(tx_hash_trades)

    # Prepare final output
    epoch_trade_logs = epoch_results.dict()
    max_block_details = block_details_dict.get(max_chain_height, {})
    max_block_timestamp = max_block_details.get('timestamp', None)
    epoch_trade_logs.update({'timestamp': max_block_timestamp})
    return epoch_trade_logs
