import asyncio
from decimal import Decimal
from decimal import localcontext
from eth_abi import abi
from web3 import Web3

from computes.utils.models.data_models import AssetEModeData
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
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper

helper_logger = logger.bind(module='PowerLoom|Aave|Helpers')

def gen_data_source_idx_to_compute(msg_obj: SnapshotProcessMessage):
    monitored_pairs = worker_settings.initial_pools
    current_epoch = msg_obj.epochId
    snapshotter_int_value = int(settings.instance_id.lower(), 16)
    current_day = msg_obj.day

    return (current_epoch + snapshotter_int_value + settings.slot_id + current_day) % len(monitored_pairs)

async def get_asset_metadata(
    asset_address: str,
    rpc_helper: RpcHelper,
):
    """
    Retrieves metadata for a given asset.

    Args:
        asset_address (str): The address of the asset.
        rpc_helper (RpcHelper): RPC helper object.

    Returns:
        dict: A dictionary containing asset metadata (address, decimals, symbol, name).
    """
    try:
        asset_address = Web3.to_checksum_address(asset_address)

        if asset_address in worker_settings.metadata_cache:
            return worker_settings.metadata_cache[asset_address]

        asset_contract_obj = current_node['web3_client'].eth.contract(
            address=Web3.to_checksum_address(asset_address),
            abi=erc20_abi,
        )

        tasks = []

        # Special handling for MakerDAO token
        if Web3.to_checksum_address(
            worker_settings.contract_addresses.MAKER,
        ) == Web3.to_checksum_address(asset_address):
            asset_name = get_maker_pair_data('name')
            asset_symbol = get_maker_pair_data('symbol')
            tasks.append(asset_contract_obj.functions.decimals())

            [asset_decimals] = await rpc_helper.web3_call(
                tasks=tasks,
            )
        else:
            tasks.extend(
                [
                    asset_contract_obj.functions.decimals(),
                    asset_contract_obj.functions.symbol(),
                    asset_contract_obj.functions.name(),
                ]
            )
            [
                asset_decimals,
                asset_symbol,
                asset_name,
            ] = await rpc_helper.web3_call(
                tasks=tasks,
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
):
    """
    Retrieves pool supply events for a given block range.

    Args:
        rpc_helper (RpcHelper): RPC helper object.
        from_block (int): Starting block number.
        to_block (int): Ending block number.

    Returns:
        dict: A dictionary of events indexed by block number.
    """
    try:
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
    rpc_helper: RpcHelper,
    from_block: int,
    to_block: int,
):
    """
    Retrieves bulk asset data for all assets in the Aave pool.

    Args:
        rpc_helper (RpcHelper): RPC helper object.
        from_block (int): Starting block number.
        to_block (int): Ending block number.

    Returns:
        tuple: A tuple containing two dictionaries:
               1. all_assets_data_dict: Asset data for all assets in the pool.
               2. all_assets_price_dict: Asset prices for all assets in the pool.
    """
    try:
        [asset_list] = await rpc_helper.web3_call(
            tasks=[pool_contract_obj.functions.getReservesList()],
        )

        asset_set = set(asset_list)

        param = Web3.to_checksum_address(worker_settings.contract_addresses.pool_address_provider)
        reserve_data_function = ui_pool_data_provider_contract_obj.functions.getReservesData(param)
        e_mode_category_function = ui_pool_data_provider_contract_obj.functions.getEModes(param)

        # Generate types for abi decoding
        reserve_data_output_type = [
            f"({get_tuple_type_string(output['components'])})"
            for output in reserve_data_function.abi['outputs']
        ]

        e_mode_category_output_type = [
            f"({get_tuple_type_string(output['components'])})"
            for output in e_mode_category_function.abi['outputs']
        ]

        reserve_data_type_string = reserve_data_output_type[0]+'[]'
        e_mode_category_type_string = e_mode_category_output_type[0]+'[]'

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

        e_mode_data_bulk = await rpc_helper.batch_eth_call_on_block_range_hex_data(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.ui_pool_data_provider,
            from_block=from_block,
            to_block=to_block,
            function_name='getEModes',
            params=[param],
        )

        all_assets_data_dict = {asset: {} for asset in asset_set}
        all_assets_price_dict = {block_num: {} for block_num in range(from_block, to_block + 1)}

        # Iterate over the bulk asset data response and decode the data
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            decoded_assets_data = abi.decode(
                (reserve_data_type_string, reserve_data_output_type[1]), asset_data_bulk[i],
            )

            decoded_e_mode_category = abi.decode(
                [e_mode_category_type_string], e_mode_data_bulk[i],
            )[0]

            # Process each asset
            for i, data in enumerate(decoded_assets_data[0]):
                asset = Web3.to_checksum_address(data[0])

                # Process e-mode data for the asset
                asset_e_mode_data = []
                for e_mode in decoded_e_mode_category:
                    e_mode_data = AssetEModeData()
                    if is_reserve_enabled_on_bitmap(e_mode[1][3], i):
                        e_mode_data.collateralEnabled = True
                    if is_reserve_enabled_on_bitmap(e_mode[1][5], i):
                        e_mode_data.borrowEnabled = True
                    if e_mode_data.collateralEnabled or e_mode_data.borrowEnabled:
                        e_mode_data.eLtv = e_mode[1][0]
                        e_mode_data.eliqThreshold = e_mode[1][1]
                        e_mode_data.eliqBonus = e_mode[1][2]
                        e_mode_data.label = e_mode[1][4]
                        asset_e_mode_data.append(e_mode_data.dict())

                # Updated indices based on full node implementation
                asset_data = {
                    'liquidityIndex': data[12],
                    'variableBorrowIndex': data[13],
                    'liquidityRate': data[14],
                    'variableBorrowRate': data[15],
                    'lastUpdateTimestamp': data[16],
                    'availableLiquidity': data[20],
                    'totalScaledVariableDebt': data[21],
                    'priceInMarketReferenceCurrency': data[22],
                    'accruedToTreasury': data[30],
                    'isolationModeTotalDebt': data[32],
                }

                asset_details = {
                    'ltv': data[4],
                    'liqThreshold': data[5],
                    'liqBonus': data[6],
                    'resFactor': data[7],
                    'borrowCap': data[36],
                    'supplyCap': data[37],
                    'eModeData': asset_e_mode_data,
                }

                rate_details = {
                    'varRateSlope1': data[24],
                    'varRateSlope2': data[25],
                    'baseVarRate': data[26],
                    'optimalRate': data[27],
                }

                # Update asset details with e-mode data if available
                if asset_e_mode_data:
                    e_mode_data = asset_e_mode_data[0]
                    asset_details['eLtv'] = e_mode_data['eLtv']
                    asset_details['eliqThreshold'] = e_mode_data['eliqThreshold']
                    asset_details['eliqBonus'] = e_mode_data['eliqBonus']

                data_dict = {
                    'asset_data': asset_data,
                    'asset_details': asset_details,
                    'rate_details': rate_details,
                }

                if asset in asset_set:
                    all_assets_data_dict[asset][block_num] = data_dict
                    all_assets_price_dict[block_num][asset] = asset_data['priceInMarketReferenceCurrency']
                else:
                    asset_set.add(asset)
                    all_assets_data_dict[asset] = {}
                    all_assets_data_dict[asset][block_num] = data_dict
                    all_assets_price_dict[block_num][asset] = asset_data['priceInMarketReferenceCurrency']

        return all_assets_data_dict, all_assets_price_dict

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
    
def get_tuple_type_string(components):
    types = []
    for component in components:
        if component['type'] == 'tuple':
            # Recursively handle nested tuples
            nested_types = get_tuple_type_string(component['components'])
            types.append(f"({nested_types})")
        else:
            types.append(component['type'])
    return ','.join(types)

def is_reserve_enabled_on_bitmap(bitmap: int, reserve_index: int) -> bool:
    """
    Checks if a reserve is enabled by checking the bit at reserve_index in bitmap.
    
    Args:
        bitmap (int): The bitmap containing reserve states
        reserve_index (int): The index of the reserve to check
        
    Returns:
        bool: True if the reserve is enabled, False otherwise
        
    Raises:
        ValueError: If reserve_index is >= 128 (MAX_RESERVES_COUNT)
    """
    MAX_RESERVES_COUNT = 128  # This matches Aave's MAX_RESERVES_COUNT
    
    if reserve_index >= MAX_RESERVES_COUNT:
        raise ValueError("Invalid reserve index")
        
    return ((bitmap >> reserve_index) & 1) != 0
