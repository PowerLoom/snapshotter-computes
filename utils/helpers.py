import asyncio
import json
from decimal import Decimal
from decimal import localcontext
import math
from eth_abi import abi
from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from computes.redis_keys import aave_asset_contract_data
from computes.redis_keys import aave_cached_block_height_asset_data
from computes.redis_keys import aave_cached_block_height_asset_details
from computes.redis_keys import aave_cached_block_height_asset_rate_details
from computes.redis_keys import aave_cached_block_height_assets_prices
from computes.redis_keys import aave_cached_block_height_core_event_data
from computes.redis_keys import aave_pool_asset_set_data
from computes.settings.config import settings as worker_settings
from computes.utils.constants import AAVE_EVENT_SIGS
from computes.utils.constants import AAVE_EVENTS_ABI
from computes.utils.constants import current_node
from computes.utils.constants import erc20_abi
from computes.utils.constants import HALF_RAY
from computes.utils.constants import pool_contract_obj
from computes.utils.constants import RAY
from computes.utils.constants import SECONDS_IN_YEAR
from computes.utils.constants import ui_pool_data_provider_contract_obj


helper_logger = logger.bind(module='PowerLoom|Aave|Helpers')

async def get_asset_metadata(
    asset_address: str,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Retrieves metadata for a given asset.

    Args:
        asset_address (str): The address of the asset.
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object.

    Returns:
        dict: A dictionary containing asset metadata (address, decimals, symbol, name).
    """
    try:
        asset_address = Web3.toChecksumAddress(asset_address)

        # Check if cache exists
        asset_data_cache = await redis_conn.hgetall(
            aave_asset_contract_data.format(asset_address),
        )

        if asset_data_cache:
            # Retrieve data from cache
            asset_decimals = asset_data_cache[b'asset_decimals'].decode('utf-8')
            asset_symbol = asset_data_cache[b'asset_symbol'].decode('utf-8')
            asset_name = asset_data_cache[b'asset_name'].decode('utf-8')
        else:
            # Fetch data from blockchain if not cached
            asset_contract_obj = current_node['web3_client'].eth.contract(
                address=Web3.toChecksumAddress(asset_address),
                abi=erc20_abi,
            )

            tasks = []

            # Special handling for MakerDAO token
            if Web3.toChecksumAddress(
                worker_settings.contract_addresses.MAKER,
            ) == Web3.toChecksumAddress(asset_address):
                asset_name = get_maker_pair_data('name')
                asset_symbol = get_maker_pair_data('symbol')
                tasks.append(('decimals', []))

                [asset_decimals] = await rpc_helper.web3_call(
                    tasks=tasks,
                    contract_addr=asset_contract_obj.address,
                    abi=asset_contract_obj.abi,
                )
            else:
                tasks.extend([('decimals', []), ('symbol', []), ('name', [])])
                [
                    asset_decimals,
                    asset_symbol,
                    asset_name,
                ] = await rpc_helper.web3_call(
                    tasks=tasks,
                    contract_addr=asset_contract_obj.address,
                    abi=asset_contract_obj.abi,
                )

            # Cache the fetched data
            await redis_conn.hset(
                name=aave_asset_contract_data.format(asset_address),
                mapping={
                    'asset_decimals': asset_decimals,
                    'asset_symbol': asset_symbol,
                    'asset_name': asset_name,
                },
            )

        return {
            'address': asset_address,
            'decimals': asset_decimals,
            'symbol': asset_symbol,
            'name': asset_name,
        }

    except Exception as err:
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetching metadata for asset {asset_address},'
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
    """
    Retrieves pool supply events for a given block range.

    Args:
        rpc_helper (RpcHelper): RPC helper object.
        from_block (int): Starting block number.
        to_block (int): Ending block number.
        redis_conn (aioredis.Redis): Redis connection object.

    Returns:
        dict: A dictionary of events indexed by block number.
    """
    try:
        # Check if events are cached
        cached_event_dict = await redis_conn.zrangebyscore(
            name=aave_cached_block_height_core_event_data,
            min=int(from_block),
            max=int(to_block),
        )

        if cached_event_dict:
            # Return cached events if available
            event_dict = {
                json.loads(event.decode('utf-8'))['blockHeight']:
                [event for event in json.loads(event.decode('utf-8'))['events']]
                for event in cached_event_dict
            }

            return event_dict

        else:
            # Fetch events from blockchain if not cached
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
            )

            event_dict = {}

            for block_num in range(from_block, to_block + 1):
                block_events = filter(lambda x: x['blockNumber'] == block_num, events)
                event_dict[block_num] = [dict(event) for event in block_events]

            if len(event_dict) > 0:
                # Cache the fetched events
                redis_cache_mapping = {
                    Web3.to_json({'blockHeight': height, 'events': events}): int(height)
                    for height, events in event_dict.items()
                }

                source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

                # Save all assets' event data in redis and remove stale events
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
        helper_logger.opt(exception=True).error(
            (
                f'Error while fetching Aave supply events in block range {from_block} : {to_block}'
            ),
        )
        raise err


def get_maker_pair_data(prop):
    """
    Returns specific data for the Maker token.

    Args:
        prop (str): The property to retrieve ('name' or 'symbol').

    Returns:
        str: The requested property value.
    """
    prop = prop.lower()
    if prop == 'name':
        return 'Maker'
    elif prop == 'symbol':
        return 'MKR'
    else:
        return 'Maker'


async def get_bulk_asset_data(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):
    """
    Retrieves bulk asset data for all assets in the Aave pool.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        rpc_helper (RpcHelper): RPC helper object.
        from_block (int): Starting block number.
        to_block (int): Ending block number.

    Returns:
        dict: A dictionary containing asset data for all assets in the pool.
    """
    try:
        # Check if asset set cache exists
        asset_list_set_cache = await redis_conn.smembers(
            aave_pool_asset_set_data,
        )

        if asset_list_set_cache:
            asset_set = {Web3.toChecksumAddress(asset.decode('utf-8')) for asset in asset_list_set_cache}
        else:
            # if asset set does not exist, fetch it from the pool contract
            # https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/pool/Pool.sol#L516
            [asset_list] = await rpc_helper.web3_call(
                tasks=[('getReservesList', [])],
                contract_addr=pool_contract_obj.address,
                abi=pool_contract_obj.abi,
            )

            # Save the asset set in redis for use in future epochs
            await redis_conn.sadd(
                aave_pool_asset_set_data, *asset_list,
            )
            asset_set = set(asset_list)

        source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))

        # PoolAddressProvider contract serves as a registry for the Aave protocol's core contracts
        # to be consumed by the Aave UI and the protocol's contracts
        param = Web3.toChecksumAddress(worker_settings.contract_addresses.pool_address_provider)
        function = ui_pool_data_provider_contract_obj.functions.getReservesData(param)

        # Generate types for abi decoding
        output_type = [
            str(
                tuple(
                    component['type']
                    for component in output['components']
                ),
            ).replace(' ', '').replace("'", '')
            for output in function.abi['outputs']
        ]

        type_string = output_type[0]+'[]'

        abi_dict = get_contract_abi_dict(
            abi=ui_pool_data_provider_contract_obj.abi,
        )

        # retrieve bulk asset data using the Aave UiPoolDataProviderV3 contract
        # https://docs.aave.com/developers/periphery-contracts/uipooldataproviderv3#getreservesdata
        asset_data_bulk = await rpc_helper.batch_eth_call_on_block_range_hex_data(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.ui_pool_data_provider,
            from_block=from_block,
            to_block=to_block,
            function_name='getReservesData',
            params=[param],
        )

        all_assets_data_dict = {asset: {} for asset in asset_set}
        all_assets_price_dict = {block_num: {} for block_num in range(from_block, to_block + 1)}

        # Iterate over the bulk asset data response and decode the data
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            decoded_assets_data = abi.decode(
                (type_string, output_type[1]), asset_data_bulk[i],
            )

            # Each data point in the response array represents a single asset
            for data in decoded_assets_data[0]:
                asset = Web3.toChecksumAddress(data[0])

                # full response interface can be found in the following github repo:
                # https://github.com/aave/aave-v3-periphery/blob/master/contracts/misc/interfaces/IUiPoolDataProviderV3.sol#L17
                asset_data = {
                    'liquidityIndex': data[13],
                    'variableBorrowIndex': data[14],
                    'liquidityRate': data[15],
                    'variableBorrowRate': data[16],
                    'stableBorrowRate': data[17],
                    'lastUpdateTimestamp': data[18],
                    'availableLiquidity': data[23],
                    'totalPrincipalStableDebt': data[24],
                    'averageStableRate': data[25],
                    'stableDebtLastUpdateTimestamp': data[26],
                    'totalScaledVariableDebt': data[27],
                    'priceInMarketReferenceCurrency': data[28],
                    'accruedToTreasury': data[39],
                    'isolationModeTotalDebt': data[41],
                }

                asset_details = {
                    'ltv': data[4],
                    'liqThreshold': data[5],
                    'liqBonus': data[6],
                    'resFactor': data[7],
                    'borrowCap': data[46],
                    'supplyCap': data[47],
                    'eLtv': data[48],
                    'eliqThreshold': data[49],
                    'eliqBonus': data[50],
                }

                rate_details = {
                    'varRateSlope1': data[30],
                    'varRateSlope2': data[31],
                    'stableRateSlope1': data[32],
                    'stableRateSlope2': data[33],
                    'baseStableRate': data[34],
                    'baseVarRate': data[35],
                    'optimalRate': data[36],
                }

                data_dict = {
                    'asset_data': asset_data,
                    'asset_details': asset_details,
                    'rate_details': rate_details,
                }

                # Account for new assets being added after the initial asset list is retrieved
                if asset in asset_set:
                    all_assets_data_dict[asset][block_num] = data_dict
                    all_assets_price_dict[block_num][asset] = asset_data['priceInMarketReferenceCurrency']
                else:
                    await redis_conn.sadd(
                        aave_pool_asset_set_data, asset,
                    )
                    asset_set.add(asset)
                    all_assets_data_dict[asset] = {}
                    all_assets_data_dict[asset][block_num] = data_dict
                    all_assets_price_dict[block_num][asset] = asset_data['priceInMarketReferenceCurrency']

        # Cache each data dict for later retrieval by snapshotter during compute
        for address, data_dict in all_assets_data_dict.items():
            if len(data_dict) > 0:
                redis_data_cache_mapping = {
                    json.dumps({'blockHeight': height, 'data': data['asset_data']}): int(
                        height,
                    )
                    for height, data in data_dict.items()
                }

                redis_details_cache_mapping = {
                    json.dumps({'blockHeight': height, 'data': data['asset_details']}): int(
                        height,
                    )
                    for height, data in data_dict.items()
                }

                redis_rate_cache_mapping = {
                    json.dumps({'blockHeight': height, 'data': data['rate_details']}): int(
                        height,
                    )
                    for height, data in data_dict.items()
                }

                asset_address = Web3.to_checksum_address(address)

                await asyncio.gather(
                    redis_conn.zadd(
                        name=aave_cached_block_height_asset_data.format(
                            asset_address,
                        ),
                        mapping=redis_data_cache_mapping,
                    ),
                    redis_conn.zadd(
                        name=aave_cached_block_height_asset_details.format(
                            asset_address,
                        ),
                        mapping=redis_details_cache_mapping,
                    ),
                    redis_conn.zadd(
                        name=aave_cached_block_height_asset_rate_details.format(
                            asset_address,
                        ),
                        mapping=redis_rate_cache_mapping,
                    ),
                )

                await asyncio.gather(
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_asset_data.format(
                            asset_address,
                        ),
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_asset_details.format(
                            asset_address,
                        ),
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                    redis_conn.zremrangebyscore(
                        name=aave_cached_block_height_asset_rate_details.format(
                            asset_address,
                        ),
                        min=0,
                        max=from_block - source_chain_epoch_size * 3,
                    ),
                )

        # Cache asset prices by block number
        redis_data_cache_mapping = {
            json.dumps({'blockHeight': height, 'data': asset_prices}): int(
                height,
            )
            for height, asset_prices in all_assets_price_dict.items() if len(asset_prices) > 0
        }

        await asyncio.gather(
            redis_conn.zadd(
                name=aave_cached_block_height_assets_prices,
                mapping=redis_data_cache_mapping,
            ),
            redis_conn.zremrangebyscore(
                name=aave_cached_block_height_assets_prices,
                min=0,
                max=from_block - source_chain_epoch_size * 3,
            ),
        )

        return all_assets_data_dict

    except Exception as err:
        helper_logger.opt(exception=True).error(
            (
                f'RPC error while fetching bulk asset data,'
                f' error_msg:{err}'
            ),
        )
        raise err


# Normalizes the interest rate using the given index, and then applies the rate to the scaled value
def calculate_current_from_scaled(scaled_value: int, interest_rate: int, index: int) -> int:
    """
    Calculates the current value from a scaled value using the given interest rate and index.

    Args:
        scaled_value (int): The scaled value.
        interest_rate (int): The interest rate.
        index (int): The index value.

    Returns:
        int: The calculated current value.
    """
    normalized = calculate_normalized_value(
        interest_rate=interest_rate,
        index=index,
    )
    return rayMul(scaled_value, normalized)


# Multiply two ray values, rounding half up to the nearest ray
# On-chain implementation here:
# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/WadRayMath.sol#L65
def rayMul(a: int, b: int) -> int:
    x = Decimal(str(a)) * Decimal(str(b))
    y = x + Decimal(str(HALF_RAY))
    z = y / Decimal(str(RAY))
    return int(z)


# Divides two ray values, rounding half up to the nearest ray
# On-chain implementation here:
# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/WadRayMath.sol#L83
def rayDiv(a: int, b: int) -> int:
    x = Decimal(str(b)) / Decimal(2)
    y = Decimal(str(a)) * Decimal(RAY)
    z = (x + y) / b
    return int(z)


# Calculates the normalized interest rate value by multiplying the interest rate by the current rate index
# Example here: https://github.com/aave/aave-utilities/blob/master/packages/math-utils/src/pool-math.ts#L51
def calculate_normalized_value(interest_rate: int, index: int) -> int:
    return rayMul(interest_rate, index)


# Aave uses a binomial approximation to calculate compound interest in V3 to save on gas costs
# The approximation follows the formula: (1+x)^n ~= 1 + n*x + [n/2 * (n-1)] * x^2 + [n/6 * (n-1) * (n-2) * x^3]
# This implementation is based on the following Aave backend utility library:
# https://github.com/aave/aave-utilities/blob/master/packages/math-utils/src/ray.math.ts#L52
# The on-chain implementation can be found here:
# https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/math/MathUtils.sol#L50
def calculate_compound_interest_rate(rate: int, current_timestamp: int, last_update_timestamp: int) -> int:

    # Get the time elapsed in seconds since last update, n in the formula
    exp = current_timestamp - last_update_timestamp

    # Get the annualized rate per second, x in the formula
    base = Decimal(str(rate)) / Decimal(SECONDS_IN_YEAR)

    # If the time elapsed is 0, return the base rate of 1
    if exp == 0:
        return int(RAY)

    # (n - 1)
    expMinusOne = exp - 1
    # (n - 2)
    expMinusTwo = max(0, exp - 2)

    # Pre-calculate base^2, equivalent to x^2 in the formula: (rate / SECONDS_IN_YEAR)^2
    basePowerTwo = rayMul(rate, rate) / Decimal(SECONDS_IN_YEAR * SECONDS_IN_YEAR)

    # Pre-calculate base^3, equivalent to x^3 in the formula
    basePowerThree = rayMul(basePowerTwo, base)

    # Calculate the first, second, and third terms of the binomial approximation
    # n*x
    firstTerm = exp * base
    firstTerm = Decimal(str(firstTerm))

    # [n/2 * (n-1)] * x^2
    secondTerm = exp * expMinusOne * basePowerTwo
    secondTerm = Decimal(str(secondTerm)) / Decimal('2')

    # [n/6 * (n-1) * (n-2)] * x^3
    thirdTerm = exp * expMinusOne * expMinusTwo * basePowerThree
    thirdTerm = Decimal(str(thirdTerm)) / Decimal('6')

    # Calculate the total interest using the binomial approximation
    interest = Decimal(str(RAY)) + firstTerm + secondTerm + thirdTerm

    return int(interest)


# Converts a ray value to a float, rounding to 16 decimal places
def convert_from_ray(value: int) -> float:
    with localcontext() as ctx:
        ctx.prec = 16
        conv = Decimal(str(value)) / Decimal(RAY)
        return float(conv)


def truncate(number, decimals=5):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor