import asyncio

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from .constants import AAVE_CORE_EVENTS
from .constants import pool_data_provider_contract_obj
from .constants import ray
from .constants import seconds_in_year
from .helpers import get_asset_metadata
from .helpers import get_supply_events
from .models.data_models import data_provider_reserve_data
from .pricing import get_token_price_in_block_range

core_logger = logger.bind(module='PowerLoom|AaveCore')


async def get_asset_supply_and_debt(
    asset_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    fetch_timestamp=False,
):
    core_logger.debug(
        f'Starting asset total supply query for: {asset_address}',
    )
    asset_address = Web3.toChecksumAddress(asset_address)

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

        # aave supply is computed using block timestamps
        # if we are fetching timestamps, we can save rpc calls by computing event data
        # fetching timestamps is better for multi-asset projects as assets can share block data
        # otherwise, batching calls is more efficient

        asset_data: list = await calculate_asset_supply_timestamped(
            rpc_helper=rpc_helper,
            redis_conn=redis_conn,
            from_block=from_block,
            to_block=to_block,
            asset_address=asset_address,
            block_details_dict=block_details_dict,
        )

    else:
        block_details_dict = dict()
        asset_data = list()

    core_logger.debug(
        (
            'get asset supply fetched block details for epoch for:'
            f' {asset_address}'
        ),
    )

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    asset_price_map = await get_token_price_in_block_range(
        token_metadata=asset_metadata,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
        debug_log=False,
    )

    if not asset_data:

        data_contract_abi_dict = get_contract_abi_dict(pool_data_provider_contract_obj.abi)

        asset_data = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=data_contract_abi_dict,
            function_name='getReserveData',
            contract_address=pool_data_provider_contract_obj.address,
            from_block=from_block,
            to_block=to_block,
            redis_conn=redis_conn,
            params=[asset_address],
        )

        asset_data = [data_provider_reserve_data(*data) for data in asset_data]

    asset_supply_debt_dict = dict()

    for i, block_num in enumerate(range(from_block, to_block + 1)):
        total_supply = asset_data[i].totalAToken / (10 ** int(asset_metadata['decimals']))
        total_supply_usd = total_supply * asset_price_map.get(block_num, 0)

        total_stable_debt = asset_data[i].totalStableDebt / (10 ** int(asset_metadata['decimals']))
        total_variable_debt = asset_data[i].totalVariableDebt / (10 ** int(asset_metadata['decimals']))
        total_stable_debt_usd = total_stable_debt * asset_price_map.get(block_num, 0)
        total_variable_debt_usd = total_variable_debt * asset_price_map.get(block_num, 0)

        current_block_details = block_details_dict.get(from_block, None)

        asset_supply_debt_dict[block_num] = {
            'total_supply': {'token_supply': total_supply, 'usd_supply': total_supply_usd},
            'total_stable_debt': {'token_debt': total_stable_debt, 'usd_debt': total_stable_debt_usd},
            'total_variable_debt': {'token_debt': total_variable_debt, 'usd_debt': total_variable_debt_usd},
            'liquidity_rate': asset_data[i].liquidityRate,
            'liquidity_index': asset_data[i].liquidityIndex,
            'variable_borrow_rate': asset_data[i].variableBorrowRate,
            'stable_borrow_rate': asset_data[i].stableBorrowRate,
            'variable_borrow_index': asset_data[i].variableBorrowIndex,
            'last_update_timestamp': int(asset_data[i].lastUpdateTimestamp),
            'timestamp': asset_data[i].timestamp,
        }

    core_logger.debug(
        (
            'Calculated asset total supply and debt for epoch-range:'
            f' {from_block} - {to_block} | asset_contract: {asset_address}'
        ),
    )

    return asset_supply_debt_dict


# TODO: add debt calculation, add unbacked calculation
async def calculate_asset_supply_timestamped(
    rpc_helper: RpcHelper,
    redis_conn: aioredis.Redis,
    from_block: int,
    to_block: int,
    asset_address: str,
    block_details_dict: dict,
):
    pool_contract_abi_dict = get_contract_abi_dict(pool_data_provider_contract_obj.abi)

    initial_data, events = await asyncio.gather(
        # get initial asset supply data from the AavePoolV3 contract
        rpc_helper.batch_eth_call_on_block_range(
            abi_dict=pool_contract_abi_dict,
            function_name='getReserveData',
            contract_address=pool_data_provider_contract_obj.address,
            from_block=from_block,
            to_block=from_block,
            redis_conn=redis_conn,
            params=[asset_address],
        ),
        # get all events from the pool contract ignoring initial block
        get_supply_events(
            rpc_helper=rpc_helper,
            from_block=from_block + 1,
            to_block=to_block,
            redis_conn=redis_conn,
        ),
    )

    initial_data = data_provider_reserve_data(*initial_data[0])

    # filter events to core events containing the asset address
    supply_events = list(
        filter(
            lambda x:
            x['args']['reserve'] == asset_address and
            x['event'] in AAVE_CORE_EVENTS,
            events,
        ),
    )
    data_events = filter(
        lambda x:
        x['args']['reserve'] == asset_address and
        x['event'] == 'ReserveDataUpdated',
        events,
    )

    data_events = {event['blockNumber']: {event['transactionIndex']: event} for event in data_events}

    liquidity_rate = initial_data.liquidityRate
    liquidity_index = initial_data.liquidityIndex
    last_update = initial_data.lastUpdateTimestamp
    supply = initial_data.totalAToken

    current_block_details = block_details_dict.get(from_block, None)

    scaled_supply = calculate_initial_scaled_supply(
        supply=supply,
        current_block_details=current_block_details,
        last_update=last_update,
        liquidity_rate=liquidity_rate,
        liquidity_index=liquidity_index,
    )

    computed_supply_debt_list = list()

    # add known from_block data to return list
    computed_supply_debt_list.append({
        'totalAToken': supply,
        'liquidityRate': liquidity_rate,
        'liquidityIndex': liquidity_index,
        'totalStableDebt': 0,
        'totalVariableDebt': 0,
        'variableBorrowRate': 0,
        'stableBorrowRate': 0,
        'variableBorrowIndex': 0,
        'lastUpdateTimestamp': last_update,
        'timestamp': current_block_details.get('timestamp', None),
        'unbacked': initial_data.unbacked,  # using initial data for unbacked until calculation implemented
        'accruedToTreasuryScaled': 0,  # not used in current snapshot
        'averageStableBorrowRate': 0,  # not used in current snapshot
    })

    # calculate supply for each block excluding from block
    for block_num in range(from_block + 1, to_block + 1):
        current_block_details = block_details_dict.get(block_num, None)

        (
            supply,
            scaled_supply,
            last_update,
            liquidity_rate,
            liquidity_index,
        ) = calculate_supply_data(
            supply=supply,
            scaled_supply=scaled_supply,
            current_block_details=current_block_details,
            last_update=last_update,
            liquidity_rate=liquidity_rate,
            liquidity_index=liquidity_index,
            supply_events=supply_events,
            data_events=data_events,
        )

        computed_supply_debt_list.append({
            'totalAToken': supply,
            'liquidityRate': liquidity_rate,
            'liquidityIndex': liquidity_index,
            'totalStableDebt': 0,
            'totalVariableDebt': 0,
            'variableBorrowRate': 0,
            'stableBorrowRate': 0,
            'variableBorrowIndex': 0,
            'lastUpdateTimestamp': last_update,
            'timestamp': current_block_details.get('timestamp', None),
            'unbacked': initial_data.unbacked,
            'accruedToTreasuryScaled': 0,
            'averageStableBorrowRate': 0,
        })

    core_logger.debug(
        (
            'Calculated asset total supply for epoch-range:'
            f' {from_block} - {to_block} | asset_contract: {asset_address}'
        ),
    )

    return [data_provider_reserve_data(**data) for data in computed_supply_debt_list]


def calculate_supply_data(
    supply,
    scaled_supply,
    current_block_details,
    last_update,
    liquidity_rate,
    liquidity_index,
    supply_events,
    data_events,
):
    block_num = current_block_details.get('number', None)
    block_events = filter(lambda x: x.get('blockNumber') == block_num, supply_events)
    timestamp = current_block_details.get('timestamp', None)
    update_scaled_flag = False

    supply_adjustment = 0

    # adjust supply data with retrieved events
    # TODO: Test with multiple events in same block
    for event in block_events:
        if event['event'] == 'Withdraw':
            supply_adjustment -= event['args']['amount']
        elif event['event'] == 'Supply':
            supply_adjustment += event['args']['amount']

        # ReserveDataUpdated events are always emitted alongside core supply events
        data_update = data_events[block_num][event['transactionIndex']]
        liquidity_rate = data_update['args']['liquidityRate']
        liquidity_index = data_update['args']['liquidityIndex']
        last_update = timestamp

        update_scaled_flag = True

    # calculate linear interest for supply
    # https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/MathUtils.sol
    time_dif = timestamp - last_update
    interest = ((liquidity_rate * time_dif) / seconds_in_year) + ray

    supply = calculate_supply_from_scaled(scaled_supply, interest, liquidity_index) + supply_adjustment

    # scaled supply must be updated after supply is calculated
    if update_scaled_flag:
        scaled_supply = calculate_scaled_from_supply(supply, interest, liquidity_index)

    return (supply, scaled_supply, last_update, liquidity_rate, liquidity_index)


def calculate_initial_scaled_supply(
    supply: int,
    current_block_details: dict,
    last_update: int,
    liquidity_rate: int,
    liquidity_index: int,
) -> int:
    timestamp = current_block_details.get('timestamp', None)
    time_dif = timestamp - last_update
    interest = ((liquidity_rate * time_dif) / seconds_in_year) + ray
    return calculate_scaled_from_supply(supply, interest, liquidity_index)


def calculate_scaled_from_supply(supply: int, interest: int, liquidity_index: int) -> int:
    return int(round((supply / (interest * liquidity_index) * ray ** 2), 0))


def calculate_supply_from_scaled(scaled_supply: int, interest: int, liquidity_index: int) -> int:
    return int(round((scaled_supply * interest * liquidity_index) / ray ** 2, 0))
