import asyncio
import json
from decimal import Decimal

from eth_abi import abi
from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..redis_keys import aave_asset_contract_data
from ..redis_keys import aave_cached_block_height_asset_data
from ..redis_keys import aave_cached_block_height_burn_mint_data
from ..redis_keys import aave_cached_block_height_core_event_data
from ..redis_keys import aave_pool_asset_list_data
from ..settings.config import settings as worker_settings
from .constants import AAVE_EVENT_SIGS
from .constants import AAVE_EVENTS_ABI
from .constants import current_node
from .constants import erc20_abi
from .constants import HALF_RAY
from .constants import pool_contract_obj
from .constants import pool_data_provider_contract_obj
from .constants import RAY
from .constants import SECONDS_IN_YEAR
from .constants import STABLE_BURN_MINT_EVENT_ABI
from .constants import STABLE_BURN_MINT_EVENT_SIGS
from .constants import ui_pool_data_provider_contract_obj
from .constants import VARIABLE_BURN_MINT_EVENT_ABI
from .constants import VARIABLE_BURN_MINT_EVENT_SIGS


helper_logger = logger.bind(module='PowerLoom|Aave|Helpers')


async def get_asset_metadata(
    asset_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    try:
        asset_address = Web3.toChecksumAddress(asset_address)

        # check if cache exist
        asset_data_cache = await redis_conn.hgetall(
            aave_asset_contract_data.format(asset_address),
        )

        if asset_data_cache:
            asset_decimals = asset_data_cache[b'asset_decimals'].decode(
                'utf-8',
            )
            asset_symbol = asset_data_cache[b'asset_symbol'].decode(
                'utf-8',
            )
            asset_name = asset_data_cache[b'asset_name'].decode(
                'utf-8',
            )
            reserve_address_dict = json.loads(
                asset_data_cache[b'reserve_addresses'].decode(
                    'utf-8',
                ),
            )

        else:

            asset_contract_obj = current_node['web3_client'].eth.contract(
                address=Web3.toChecksumAddress(asset_address),
                abi=erc20_abi,
            )

            tasks = []

            if Web3.toChecksumAddress(
                worker_settings.contract_addresses.MAKER,
            ) == Web3.toChecksumAddress(asset_address):
                asset_name = get_maker_pair_data('name')
                asset_symbol = get_maker_pair_data('symbol')
                tasks.append(asset_contract_obj.functions.decimals())
                tasks.append(pool_data_provider_contract_obj.functions.getReserveTokensAddresses(asset_address))

                [
                    asset_decimals,
                    reserve_addresses,
                ] = await rpc_helper.web3_call(tasks, redis_conn=redis_conn)

            else:
                tasks.append(asset_contract_obj.functions.decimals())
                tasks.append(asset_contract_obj.functions.symbol())
                tasks.append(asset_contract_obj.functions.name())
                tasks.append(pool_data_provider_contract_obj.functions.getReserveTokensAddresses(asset_address))
                [
                    asset_decimals,
                    asset_symbol,
                    asset_name,
                    reserve_addresses,
                ] = await rpc_helper.web3_call(tasks, redis_conn=redis_conn)

            reserve_address_dict = {
                'a_token': reserve_addresses[0],
                'stable_debt_token': reserve_addresses[1],
                'variable_debt_token': reserve_addresses[2],
            }

            await redis_conn.hset(
                name=aave_asset_contract_data.format(asset_address),
                mapping={
                    'asset_decimals': asset_decimals,
                    'asset_symbol': asset_symbol,
                    'asset_name': asset_name,
                    'reserve_addresses': json.dumps(reserve_address_dict),
                },
            )

        return {
            'address': asset_address,
            'decimals': asset_decimals,
            'symbol': asset_symbol,
            'name': asset_name,
            'reserve_addresses': reserve_address_dict,
        }

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetcing metadata for asset {asset_address},'
                f' error_msg:{err}'
            ),
        )
        raise err


async def get_pool_supply_events(
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,

):
    try:

        cached_event_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_core_event_data,
            min=int(from_block),
            max=int(to_block),
        )

        if cached_event_dict:
            event_dict = {
                json.loads(event.decode('utf-8'))['blockHeight']:
                [event for event in json.loads(event.decode('utf-8'))['events']]
                for event in cached_event_dict
            }

            return event_dict

        else:
            event_sig, event_abi = get_event_sig_and_abi(
                AAVE_EVENT_SIGS,
                AAVE_EVENTS_ABI,
            )

            events = await rpc_helper.get_events_logs(
                contract_address=worker_settings.contract_addresses.aave_v3_pool,
                to_block=to_block,
                from_block=from_block,
                topics=[event_sig],
                event_abi=event_abi,
                redis_conn=redis_conn,
            )

            event_dict = {}

            for block_num in range(from_block, to_block + 1):
                block_events = filter(lambda x: x['blockNumber'] == block_num, events)
                event_dict[block_num] = [dict(event) for event in block_events]

            if len(event_dict) > 0:

                redis_cache_mapping = {
                    Web3.to_json({'blockHeight': height, 'events': events}): int(height)
                    for height, events in event_dict.items()
                }

                source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

                await asyncio.gather(
                    redis_conn.zadd(
                        name=aave_cached_block_height_core_event_data,
                        mapping=redis_cache_mapping,
                    ),
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_core_event_data,
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                )

            return event_dict

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'Error while fetcing Aave supply events in block range {from_block} : {to_block}'
            ),
        )
        raise err


async def get_debt_burn_mint_events(
    asset_address: str,
    asset_metadata: dict,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
    redis_conn: aioredis.Redis,
):
    try:

        cached_event_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_burn_mint_data.format(asset_address),
            min=int(from_block),
            max=int(to_block),
        )

        if cached_event_dict:
            event_dict = {
                json.loads(
                    event.decode(
                        'utf-8',
                    ),
                )['blockHeight']: json.loads(
                    event.decode('utf-8'),
                )['events']
                for event in cached_event_dict
            }
            return event_dict

        variable_event_sig, variable_event_abi = get_event_sig_and_abi(
            VARIABLE_BURN_MINT_EVENT_SIGS,
            VARIABLE_BURN_MINT_EVENT_ABI,
        )

        variable_events = await rpc_helper.get_events_logs(
            contract_address=asset_metadata['reserve_addresses']['variable_debt_token'],
            to_block=to_block,
            from_block=from_block,
            topics=[variable_event_sig],
            event_abi=variable_event_abi,
            redis_conn=redis_conn,
        )

        event_dict = {}

        for block_num in range(from_block, to_block + 1):
            event_dict[block_num] = list(filter(lambda x: x['blockNumber'] == block_num, variable_events))

        stable_event_sig, stable_event_abi = get_event_sig_and_abi(
            STABLE_BURN_MINT_EVENT_SIGS,
            STABLE_BURN_MINT_EVENT_ABI,
        )

        stable_events = await rpc_helper.get_events_logs(
            contract_address=asset_metadata['reserve_addresses']['stable_debt_token'],
            to_block=to_block,
            from_block=from_block,
            topics=[stable_event_sig],
            event_abi=stable_event_abi,
            redis_conn=redis_conn,
        )

        for event in stable_events:
            event_dict[event['blockNumber']].append(event)

        if len(event_dict) > 0:

            redis_cache_mapping = {
                Web3.to_json({'blockHeight': height, 'events': events}): int(height)
                for height, events in event_dict.items()
            }

            source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

            await asyncio.gather(
                redis_conn.zadd(
                    name=aave_cached_block_height_burn_mint_data.format(asset_address),
                    mapping=redis_cache_mapping,
                ),
                redis_conn.zremrangebyscore(
                    name=aave_cached_block_height_burn_mint_data.format(asset_address),
                    min=0,
                    max=from_block - source_chain_epoch_size * 3,
                ),
            )

        return event_dict

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'Error while fetcing Aave variable mint and burn for {asset_address} '
                f'in block range {from_block} : {to_block}'
            ),
        )
        raise err


def get_maker_pair_data(prop):
    prop = prop.lower()
    if prop.lower() == 'name':
        return 'Maker'
    elif prop.lower() == 'symbol':
        return 'MKR'
    else:
        return 'Maker'

async def get_bulk_asset_data(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):
    try:

        # check if asset list cache exist
        asset_list_data_cache = await redis_conn.lrange(
            aave_pool_asset_list_data, 0, -1,
        )

        if asset_list_data_cache:
            asset_list = [asset.decode('utf-8') for asset in reversed(asset_list_data_cache)]
        else:
            [asset_list] = await rpc_helper.web3_call(
                tasks=[pool_contract_obj.functions.getReservesList()],
                redis_conn=redis_conn,
            )

            await redis_conn.lpush(
                aave_pool_asset_list_data, *asset_list,
            )

        param = Web3.toChecksumAddress(worker_settings.contract_addresses.pool_address_provider)
        function = ui_pool_data_provider_contract_obj.functions.getReservesData(param)
        source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

        abi_dict = get_contract_abi_dict(
            abi=ui_pool_data_provider_contract_obj.abi,
        )

        asset_prices_bulk = await rpc_helper.batch_eth_call_on_block_range_hex_data(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.ui_pool_data_provider,
            from_block=from_block,
            to_block=to_block,
            function_name='getReservesData',
            params=[param],
            redis_conn=redis_conn,
        )

        output_type = [
            str(
                tuple(
                    component['type']
                    for component in output['components']
                ),
            ).replace(' ', '').replace("'", '')
            for output in function.abi['outputs']
        ]

        type_str = output_type[0]+'[]'

        all_assets_data_dict = {Web3.toChecksumAddress(asset): {} for asset in asset_list}

        for i, block_num in enumerate(range(from_block, to_block + 1)):
            decoded_assets_data = abi.decode(
                (type_str, output_type[1]), asset_prices_bulk[i],
            )

            for asset_data in decoded_assets_data[0]:

                asset = Web3.toChecksumAddress(asset_data[0])

                data_dict = {
                    'liquidityIndex': asset_data[13],
                    'variableBorrowIndex': asset_data[14],
                    'liquidityRate': asset_data[15],
                    'variableBorrowRate': asset_data[16],
                    'stableBorrowRate': asset_data[17],
                    'lastUpdateTimestamp': asset_data[18],
                    'availableLiquidity': asset_data[23],
                    'totalPrincipalStableDebt': asset_data[24],
                    'averageStableRate': asset_data[25],
                    'stableDebtLastUpdateTimestamp': asset_data[26],
                    'totalScaledVariableDebt': asset_data[27],
                    'priceInMarketReferenceCurrency': asset_data[28],
                    'accruedToTreasury': asset_data[39],
                    'assetDetails': {
                        'ltv': asset_data[4],
                        'liqThreshold': asset_data[5],
                        'liqBonus': asset_data[6],
                        'resFactor': asset_data[7],
                        'borrowCap': asset_data[46],
                        'supplyCap': asset_data[47],
                        'eLtv': asset_data[48],
                        'eliqThreshold': asset_data[49],
                        'eliqBonus': asset_data[50],
                        'optimalRate': asset_data[36],
                    },
                }

                all_assets_data_dict[asset][block_num] = data_dict

        # cache each price dict for later retrieval by snapshotter
        for address, data_dict in all_assets_data_dict.items():
            # cache price at height
            if len(data_dict) > 0:
                redis_cache_mapping = {
                    json.dumps({'blockHeight': height, 'data': data}): int(
                        height,
                    )
                    for height, data in data_dict.items()
                }

                await asyncio.gather(
                    redis_conn.zadd(
                        name=aave_cached_block_height_asset_data.format(
                            Web3.to_checksum_address(address),
                        ),
                        mapping=redis_cache_mapping,
                    ),
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_asset_data.format(
                            Web3.to_checksum_address(address),
                        ),
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                )

        return all_assets_data_dict

    except Exception as err:
        # this will be retried in next cycle
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetcing bulk asset data,'
                f' error_msg:{err}'
            ),
        )
        raise err

def calculate_initial_scaled_supply(
    supply: int,
    current_timestamp: dict,
    last_update: int,
    liquidity_rate: int,
    liquidity_index: int,
) -> int:
    interest = calculate_linear_interest(
        last_update_timestamp=last_update,
        current_timestamp=current_timestamp,
        liquidity_rate=liquidity_rate,
    )
    normalized = calculate_normalized_value(
        interest=interest,
        index=liquidity_index,
    )
    return rayDiv(supply, normalized)


def calculate_initial_scaled_variable(
    variable_debt: int,
    variable_rate: int,
    variable_index: int,
    current_timestamp: int,
    last_update: int,
) -> int:
    variable_interest = calculate_compound_interest(
        rate=variable_rate,
        current_timestamp=current_timestamp,
        last_update_timestamp=last_update,
    )
    normalized_variable_debt = calculate_normalized_value(
        interest=variable_interest,
        index=variable_index,
    )
    return rayDiv(variable_debt, normalized_variable_debt)


def calculate_initial_scaled_stable(
    stable_debt: int,
    avg_stable_rate: int,
    current_timestamp: int,
    last_update: int,
) -> int:
    stable_interest = calculate_compound_interest(
        rate=avg_stable_rate,
        current_timestamp=current_timestamp,
        last_update_timestamp=last_update,
    )
    return rayDiv(stable_debt, stable_interest)


def calculate_scaled_from_current(current_value: int, interest: int, index: int) -> int:
    normalized = calculate_normalized_value(
        interest=interest,
        index=index,
    )
    return rayDiv(current_value, normalized)


def calculate_current_from_scaled(scaled_value: int, interest: int, index: int) -> int:
    normalized = calculate_normalized_value(
        interest=interest,
        index=index,
    )
    return rayMul(scaled_value, normalized)


def rayMul(a: int, b: int):
    x = Decimal(a) * Decimal(b)
    y = x + Decimal(HALF_RAY)
    z = y / Decimal(RAY)
    return int(z)


def rayDiv(a: int, b: int):
    x = Decimal(b) / 2
    y = Decimal(a) * Decimal(RAY)
    z = (x + y) / b
    return int(z)


def calculate_normalized_value(interest: int, index: int) -> int:
    return rayMul(interest, index)

# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/MathUtils.sol#L23
def calculate_linear_interest(
    last_update_timestamp: int,
    current_timestamp: int,
    liquidity_rate: int,
):
    result = Decimal(liquidity_rate) * Decimal(current_timestamp - last_update_timestamp)
    result = result / SECONDS_IN_YEAR

    return Decimal(RAY) + result

# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/MathUtils.sol#L50
def calculate_compound_interest(rate: int, current_timestamp: int, last_update_timestamp: int) -> Decimal:
    exp = current_timestamp - last_update_timestamp
    base = Decimal(rate) / Decimal(SECONDS_IN_YEAR)

    if exp == 0:
        return int(RAY)

    expMinusOne = exp - 1
    expMinusTwo = max(0, exp - 2)

    basePowerTwo = rayMul(base, base)
    basePowerThree = rayMul(basePowerTwo, base)

    firstTerm = exp * base
    secondTerm = Decimal(exp * expMinusOne * basePowerTwo) / Decimal(2)
    thirdTerm = Decimal(exp * expMinusOne * expMinusTwo * basePowerThree) / Decimal(6)

    interest = Decimal(RAY) + firstTerm + secondTerm + thirdTerm

    return interest
