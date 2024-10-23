import asyncio
import json
from functools import reduce

from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range
from web3 import Web3

from snapshotter.modules.computes.utils.total_value_locked import calculate_reserves
from snapshotter.modules.computes.utils.total_value_locked import get_events
from snapshotter.modules.computes.utils.constants import UNISWAP_EVENTS_ABI
from snapshotter.modules.computes.utils.constants import UNISWAP_TRADE_EVENT_SIGS
from snapshotter.modules.computes.utils.constants import UNISWAPV3_FEE_DIV
from snapshotter.modules.computes.utils.helpers import get_pair_metadata
from snapshotter.modules.computes.utils.models.data_models import epoch_event_trade_data
from snapshotter.modules.computes.utils.models.data_models import event_trade_data
from snapshotter.modules.computes.utils.models.data_models import trade_data
from snapshotter.modules.computes.utils.pricing import get_token_price_in_block_range

core_logger = logger.bind(module='PowerLoom|UniswapCore')


async def get_pair_reserves(
    pair_address,
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    eth_price_dict: dict,
):
    """
    Fetch and calculate pair reserves for a given Uniswap pair over a block range.

    Args:
        pair_address (str): The address of the Uniswap pair contract.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        fetch_timestamp (bool): Whether to fetch block timestamps.

    Returns:
        dict: A dictionary containing pair reserves data for each block in the range.
    """
    core_logger.debug(
        f'Starting pair total reserves query for: {pair_address}',
    )
    pair_address = Web3.to_checksum_address(pair_address)

    try:
        block_details_dict = await get_block_details_in_block_range(
            from_block,
            to_block,
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

    pair_per_token_metadata = await get_pair_metadata(
        pair_address=pair_address,
        rpc_helper=rpc_helper,
    )

    core_logger.debug(
        ('total pair reserves fetched block details for epoch for:' f' {pair_address}'),
    )

    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token0'],
            from_block=from_block,
            to_block=to_block,
            rpc_helper=rpc_helper,
            eth_price_dict=eth_price_dict,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token1'],
            from_block=from_block,
            to_block=to_block,
            rpc_helper=rpc_helper,
            eth_price_dict=eth_price_dict,
            debug_log=False,
        ),
    )

    core_logger.debug(
        f'Total reserves fetched token prices for: {pair_address}',
    )

    initial_reserves = await calculate_reserves(
        pair_address,
        from_block,
        pair_per_token_metadata,
        rpc_helper,
    )

    core_logger.debug(
        f'Total reserves fetched tick data for {pair_address} of {initial_reserves} for block {from_block}',
    )

    # grab mint/burn events in range
    events = await get_events(
        pair_address=pair_address,
        rpc=rpc_helper,
        from_block=from_block,
        to_block=to_block,
    )

    core_logger.debug(
        f'Total reserves fetched event data for : {pair_address}',
    )
    # sum burn and mint each block

    token0Amount = initial_reserves[0]
    token1Amount = initial_reserves[1]

    pair_reserves_dict = dict()

    block_event_dict = dict()

    for block_num in range(from_block, to_block + 1):
        block_event_dict[block_num] = list(filter(lambda x: x if x.get('blockNumber') == block_num else None, events))

    for block_num, events in block_event_dict.items():
        events_in_block = block_event_dict.get(block_num, [])

        # Swap events use ints and mint events are positive, so only need to subtract burn events.

        token0Amount += reduce(
            lambda acc, event: acc - event['args']['amount0']
            if event['event'] == 'Burn'
            else acc + event['args']['amount0'], events_in_block, 0,
        )
        token1Amount += reduce(
            lambda acc, event: acc - event['args']['amount1']
            if event['event'] == 'Burn'
            else acc + event['args']['amount1'], events_in_block, 0,
        )

        token0AmountNormalized = token0Amount / (10 ** int(pair_per_token_metadata['token0']['decimals']))
        token1AmountNormalized = token1Amount / (10 ** int(pair_per_token_metadata['token1']['decimals']))

        token0USD = token0Amount * token0_price_map.get(block_num, 0) * \
            (10 ** -int(pair_per_token_metadata['token0']['decimals']))
        token1USD = token1Amount * token1_price_map.get(block_num, 0) * \
            (10 ** -int(pair_per_token_metadata['token1']['decimals']))

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

    core_logger.debug(
        (
            'Calculated pair total reserves for epoch-range:'
            f' {from_block} - {to_block} | pair_contract: {pair_address}'
        ),
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
    rpc_helper: RpcHelper,
    eth_price_dict: dict,
):
    """
    Fetch and calculate trade volume for a Uniswap pair over a block range.

    Args:
        data_source_contract_address (str): The address of the Uniswap pair contract.
        min_chain_height (int): The starting block number.
        max_chain_height (int): The ending block number.
        rpc_helper (RpcHelper): RPC helper for blockchain interactions.
        fetch_timestamp (bool): Whether to fetch block timestamps.

    Returns:
        dict: A dictionary containing trade volume data for the specified block range.
    """
    data_source_contract_address = Web3.to_checksum_address(
        data_source_contract_address,
    )
    try:
        block_details_dict = await get_block_details_in_block_range(
            from_block=min_chain_height,
            to_block=max_chain_height,
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

    pair_per_token_metadata = await get_pair_metadata(
        pair_address=data_source_contract_address,
        rpc_helper=rpc_helper,
    )
    token0_price_map, token1_price_map = await asyncio.gather(
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token0'],
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
            eth_price_dict=eth_price_dict,
            debug_log=False,
        ),
        get_token_price_in_block_range(
            token_metadata=pair_per_token_metadata['token1'],
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
            eth_price_dict=eth_price_dict,
            debug_log=False,
        ),
    )

    # fetch logs for swap, mint & burn
    event_sig, event_abi = get_event_sig_and_abi(
        UNISWAP_TRADE_EVENT_SIGS,
        UNISWAP_EVENTS_ABI,
    )

    events_log = await rpc_helper.get_events_logs(
        **{
            'contract_address': data_source_contract_address,
            'to_block': max_chain_height,
            'from_block': min_chain_height,
            'topics': [event_sig],
            'event_abi': event_abi,
        },
    )

    # group logs by txHashs ==> {txHash: [logs], ...}
    grouped_by_tx = dict()
    [
        grouped_by_tx[log['transactionHash'].hex()].append(log)
        if log['transactionHash'].hex() in grouped_by_tx
        else grouped_by_tx.update({log['transactionHash'].hex(): [log]})
        for log in events_log
    ]

    # init data models with empty/0 values
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

    # prepare final trade logs structure
    for tx_hash, logs in grouped_by_tx.items():
        # init temporary trade object to track trades at txHash level
        tx_hash_trades = trade_data(
            totalTradesUSD=float(),
            totalFeeUSD=float(),
            token0TradeVolume=float(),
            token1TradeVolume=float(),
            token0TradeVolumeUSD=float(),
            token1TradeVolumeUSD=float(),
            recent_transaction_logs=list(),
        )
        # shift Burn logs in end of list to check if equal size of mint already exist
        # and then cancel out burn with mint
        logs = sorted(logs, key=lambda x: x['event'], reverse=True)

        # iterate over each txHash logs
        for log in logs:
            # fetch trade value fog log
            trades_result, processed_log = extract_trade_volume_log(
                event_name=log['event'],
                log=log,
                pair_per_token_metadata=pair_per_token_metadata,
                token0_price_map=token0_price_map,
                token1_price_map=token1_price_map,
                block_details_dict=block_details_dict,
            )

            if log['event'] == 'Swap':
                epoch_results.Swap.logs.append(processed_log)
                epoch_results.Swap.trades += trades_result
                tx_hash_trades += trades_result  # swap in single txHash should be added

            elif log['event'] == 'Mint':
                epoch_results.Mint.logs.append(processed_log)
                epoch_results.Mint.trades += trades_result

            elif log['event'] == 'Burn':
                epoch_results.Burn.logs.append(processed_log)
                epoch_results.Burn.trades += trades_result

        # At the end of txHash logs we must normalize trade values, so it does not affect result of other txHash logs
        epoch_results.Trades += abs(tx_hash_trades)
    epoch_trade_logs = epoch_results.dict()
    max_block_details = block_details_dict.get(max_chain_height, dict())
    max_block_timestamp = max_block_details.get('timestamp', None)
    epoch_trade_logs.update({'timestamp': max_block_timestamp})
    return epoch_trade_logs
