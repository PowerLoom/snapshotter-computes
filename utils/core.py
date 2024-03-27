import asyncio
import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from ..redis_keys import aave_cached_block_height_asset_data
from ..redis_keys import aave_cached_block_height_asset_details
from ..redis_keys import aave_cached_block_height_asset_rate_details
from .constants import AAVE_CORE_EVENTS
from .constants import DETAILS_BASIS
from .constants import ORACLE_DECIMALS
from .helpers import calculate_compound_interest_rate
from .helpers import calculate_current_from_scaled
from .helpers import convert_from_ray
from .helpers import get_asset_metadata
from .helpers import get_pool_supply_events
from .helpers import rayMul
from .models.data_models import AaveDebtData
from .models.data_models import AaveSupplyData
from .models.data_models import AssetDetailsData
from .models.data_models import AssetTotalData
from .models.data_models import epochEventVolumeData
from .models.data_models import eventLiquidationData
from .models.data_models import eventVolumeData
from .models.data_models import liquidationData
from .models.data_models import RateDetailsData
from .models.data_models import UiDataProviderReserveData
from .models.data_models import volumeData
from .pricing import get_all_asset_prices

core_logger = logger.bind(module='PowerLoom|AaveCore')


async def get_asset_supply_and_debt_bulk(
    asset_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    fetch_timestamp=True,
):
    core_logger.debug(
        f'Starting bulk asset total supply query for: {asset_address}',
    )
    asset_address = Web3.toChecksumAddress(asset_address)

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
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

    core_logger.debug(
        (
            'get asset supply bulk fetched block details for epoch for:'
            f' {asset_address}'
        ),
    )

    asset_data_dict = {}
    asset_details_dict = {}
    asset_rates_dict = {}

    # get cached asset data from redis
    # data is retrieved in bulk by the asset_data preloader and is saved to the cache on epoch release
    [
        cached_asset_data_dict,
        cached_asset_details_dict,
        cached_asset_rates_dict,
    ] = await asyncio.gather(
        redis_conn.zrangebyscore(
            name=aave_cached_block_height_asset_data.format(
                asset_address,
            ),
            min=int(from_block),
            max=int(to_block),
        ),
        redis_conn.zrangebyscore(
            name=aave_cached_block_height_asset_details.format(
                asset_address,
            ),
            min=int(from_block),
            max=int(to_block),
        ),
        redis_conn.zrangebyscore(
            name=aave_cached_block_height_asset_rate_details.format(
                asset_address,
            ),
            min=int(from_block),
            max=int(to_block),
        ),
    )

    # decode and parse the cached data if it exists
    if cached_asset_data_dict and len(cached_asset_data_dict) == to_block - (from_block - 1):
        asset_data_dict = {
            json.loads(
                data.decode(
                    'utf-8',
                ),
            )['blockHeight']: json.loads(
                data.decode('utf-8'),
            )['data']
            for data in cached_asset_data_dict
        }

        asset_details_dict = {
            json.loads(
                data.decode(
                    'utf-8',
                ),
            )['blockHeight']: json.loads(
                data.decode('utf-8'),
            )['data']
            for data in cached_asset_details_dict
        }

        asset_rates_dict = {
            json.loads(
                data.decode(
                    'utf-8',
                ),
            )['blockHeight']: json.loads(
                data.decode('utf-8'),
            )['data']
            for data in cached_asset_rates_dict
        }

    # TODO: add fallback if cached data does not exist

    asset_supply_debt_dict = dict()

    for block_num in range(from_block, to_block + 1):
        current_block_details = block_details_dict.get(block_num, None)
        timestamp = current_block_details.get('timestamp')

        # get the asset data, details and rate details for the current block
        asset_data = asset_data_dict.get(block_num, None)
        asset_details = asset_details_dict.get(block_num, None)
        asset_rate_details = asset_rates_dict.get(block_num, None)

        # initialize the data models using the retrieved data
        asset_data = UiDataProviderReserveData.parse_obj(asset_data)
        asset_details = AssetDetailsData.parse_obj(asset_details)
        asset_rate_details = RateDetailsData.parse_obj(asset_rate_details)

        # Calculate the accrued interest for the asset from the last update timestamp to the current block timestamp.
        # Last update timestamp is updated when an action (borrow, supply, etc.) is taken on-chain, but interest 
        # continues to accrue in the supply and debt token contracts between actions.
        variable_interest = calculate_compound_interest_rate(
            rate=asset_data.variableBorrowRate,
            current_timestamp=timestamp,
            last_update_timestamp=asset_data.lastUpdateTimestamp,

        )

        stable_interest = calculate_compound_interest_rate(
            rate=asset_data.averageStableRate,
            current_timestamp=timestamp,
            last_update_timestamp=asset_data.stableDebtLastUpdateTimestamp,

        )

        # Apply the interest rate to the scaled variable debt to get the current variable debt
        # The scaled debt value is the value of the debt in ray at the last update timestamp
        total_variable_debt = calculate_current_from_scaled(
            scaled_value=asset_data.totalScaledVariableDebt,
            index=asset_data.variableBorrowIndex,
            interest_rate=variable_interest,
        )

        # stable debt is not scaled, so we can directly apply the interest rate to the stable debt
        total_stable_debt = rayMul(asset_data.totalPrincipalStableDebt, stable_interest)

        total_supply = asset_data.availableLiquidity + total_variable_debt + total_stable_debt

        asset_usd_price = asset_data.priceInMarketReferenceCurrency * (10 ** -ORACLE_DECIMALS)

        total_supply_usd = (total_supply * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        total_variable_debt_usd = (total_variable_debt * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        total_stable_debt_usd = (total_stable_debt * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        available_liquidity_usd = (asset_data.availableLiquidity * asset_usd_price) / \
            (10 ** int(asset_metadata['decimals']))

        # Normalize asset detail rates, rates are returned in 5 decimal format
        asset_details.ltv = (asset_details.ltv / DETAILS_BASIS) * 100
        asset_details.liqThreshold = (asset_details.liqThreshold / DETAILS_BASIS) * 100
        asset_details.resFactor = (asset_details.resFactor / DETAILS_BASIS) * 100
        asset_details.liqBonus = ((asset_details.liqBonus / DETAILS_BASIS) * 100) - 100
        asset_details.eLtv = (asset_details.eLtv / DETAILS_BASIS) * 100
        asset_details.eliqThreshold = (asset_details.eliqThreshold / DETAILS_BASIS) * 100
        asset_details.eliqBonus = ((asset_details.eliqBonus / DETAILS_BASIS) * 100) - 100

        # Normalize rate detail rates, rates and slopes are return in RAY format
        asset_rate_details.utilRate = total_variable_debt / total_supply
        asset_rate_details.varRateSlope1 = convert_from_ray(asset_rate_details.varRateSlope1)
        asset_rate_details.varRateSlope2 = convert_from_ray(asset_rate_details.varRateSlope2)
        asset_rate_details.baseVarRate = convert_from_ray(asset_rate_details.baseVarRate)
        asset_rate_details.stableRateSlope1 = convert_from_ray(asset_rate_details.stableRateSlope1)
        asset_rate_details.stableRateSlope2 = convert_from_ray(asset_rate_details.stableRateSlope2)
        asset_rate_details.baseStableRate = convert_from_ray(asset_rate_details.baseStableRate)
        asset_rate_details.optimalRate = convert_from_ray(asset_rate_details.optimalRate)

        total_asset_data = AssetTotalData(
            totalSupply=AaveSupplyData(
                token_supply=total_supply,
                usd_supply=total_supply_usd,
            ),
            availableLiquidity=AaveSupplyData(
                token_supply=asset_data.availableLiquidity,
                usd_supply=available_liquidity_usd,
            ),
            totalStableDebt=AaveDebtData(
                token_debt=total_stable_debt,
                usd_debt=total_stable_debt_usd,
            ),
            totalVariableDebt=AaveDebtData(
                token_debt=total_variable_debt,
                usd_debt=total_variable_debt_usd,
            ),
            liquidityRate=asset_data.liquidityRate,
            liquidityIndex=asset_data.liquidityIndex,
            variableBorrowRate=asset_data.variableBorrowRate,
            stableBorrowRate=asset_data.stableBorrowRate,
            variableBorrowIndex=asset_data.variableBorrowIndex,
            lastUpdateTimestamp=asset_data.lastUpdateTimestamp,
            isolationModeTotalDebt=asset_data.isolationModeTotalDebt,
            assetDetails=asset_details,
            rateDetails=asset_rate_details,
            timestamp=timestamp,
        )

        asset_supply_debt_dict[block_num] = total_asset_data

    core_logger.debug(
        (
            'Calculated asset total supply and debt for epoch-range:'
            f' {from_block} - {to_block} | asset_contract: {asset_address}'
        ),
    )

    return asset_supply_debt_dict


async def get_asset_trade_volume(
    asset_address,
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    fetch_timestamp=True,
):
    asset_address = Web3.toChecksumAddress(
        asset_address,
    )
    block_details_dict = dict()

    if fetch_timestamp:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block=from_block,
                to_block=to_block,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                (
                    'Error attempting to get block details of to_block {}:'
                    ' {}, retrying again'
                ),
                to_block,
                err,
            )
            raise err

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address,
        redis_conn=redis_conn,
        rpc_helper=rpc_helper,
    )

    # fetch pre-cached prices for all assets in the pool
    # Used to calculate the USD value of any debt that may be repaid during
    # the liquidation of the current asset, along with the USD value of the asset itself
    price_dict = await get_all_asset_prices(
        from_block,
        to_block,
        redis_conn,
        rpc_helper,
    )

    # fetch events for all assets in the pool from redis if cached
    # event data is retrieved and cached in the volume_events preloader
    supply_events = await get_pool_supply_events(
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    # TODO: Filter events by address in get_pool_data_events?
    asset_supply_events = {
        key: filter(
            lambda x:
            x['args'].get('reserve', '') == asset_address or
            x['args'].get('collateralAsset', '') == asset_address,
            value,
        )
        for key, value in supply_events.items()
    }

    # init data models with empty/0 values
    epoch_results = epochEventVolumeData(
        borrow=eventVolumeData(
            logs=[],
            totals=volumeData(
                totalUSD=float(),
                totalToken=int(),
            ),
        ),
        repay=eventVolumeData(
            logs=[],
            totals=volumeData(
                totalUSD=float(),
                totalToken=int(),
            ),
        ),
        supply=eventVolumeData(
            logs=[],
            totals=volumeData(
                totalUSD=float(),
                totalToken=int(),
            ),
        ),
        withdraw=eventVolumeData(
            logs=[],
            totals=volumeData(
                totalUSD=float(),
                totalToken=int(),
            ),
        ),
        liquidation=eventLiquidationData(
            logs=[],
            totalLiquidatedCollateral=volumeData(
                totalUSD=float(),
                totalToken=int(),
            ),
            liquidations=[],
        ),
    )

    for block_num in range(from_block, to_block + 1):

        # get the asset price for the current block
        block_all_asset_prices = price_dict.get(block_num, None)
        asset_usd_price = block_all_asset_prices.get(asset_address, 0)
        asset_usd_price = asset_usd_price * (10 ** -ORACLE_DECIMALS) / (10 ** int(asset_metadata['decimals']))

        # iterate over the current block's events and update the respective volume data
        for event in asset_supply_events.get(block_num, None):
            if event['event'] in AAVE_CORE_EVENTS:
                amount = event['args']['amount']
                volume = volumeData(
                    totalToken=amount,
                    totalUSD=amount * asset_usd_price,
                )

                if event['event'] == 'Borrow':
                    epoch_results.borrow.logs.append(event)
                    epoch_results.borrow.totals += volume
                elif event['event'] == 'Repay':
                    epoch_results.repay.logs.append(event)
                    epoch_results.repay.totals += volume
                elif event['event'] == 'Supply':
                    epoch_results.supply.logs.append(event)
                    epoch_results.supply.totals += volume
                elif event['event'] == 'Withdraw':
                    epoch_results.withdraw.logs.append(event)
                    epoch_results.withdraw.totals += volume

            # if the event is not in AAVE_CORE_EVENTS, then the event is a LiquidationCall
            else:
                liquidated_collateral = event['args']['liquidatedCollateralAmount']
                debt_to_cover = event['args']['debtToCover']
                debt_asset = event['args']['debtAsset']

                # get the price for the repaid debt asset
                debt_usd_price = block_all_asset_prices.get(Web3.to_checksum_address(debt_asset), 0)

                # fetch decimal data for the debt asset
                debt_asset_metadata = await get_asset_metadata(
                    asset_address=debt_asset,
                    redis_conn=redis_conn,
                    rpc_helper=rpc_helper,
                )

                debt_usd_price = debt_usd_price * (10 ** -ORACLE_DECIMALS) / \
                    (10 ** int(debt_asset_metadata['decimals']))

                liq_data = liquidationData(
                    collateralAsset=asset_address,
                    debtAsset=debt_asset,
                    debtToCover=volumeData(
                        totalToken=debt_to_cover,
                        totalUSD=debt_to_cover * debt_usd_price,
                    ),
                    liquidatedCollateral=volumeData(
                        totalToken=liquidated_collateral,
                        totalUSD=liquidated_collateral * asset_usd_price,
                    ),
                    blockNumber=block_num,
                )

                epoch_results.liquidation.logs.append(event)
                epoch_results.liquidation.liquidations.append(liq_data)
                epoch_results.liquidation.totalLiquidatedCollateral.totalToken += liquidated_collateral
                epoch_results.liquidation.totalLiquidatedCollateral.totalUSD += liquidated_collateral * asset_usd_price

    epoch_volume_logs = epoch_results.dict()
    max_block_details = block_details_dict.get(to_block, {})
    max_block_timestamp = max_block_details.get('timestamp', None)
    epoch_volume_logs.update({'timestamp': max_block_timestamp})

    core_logger.debug(
        (
            'Calculated asset supply and debt volume for epoch-range:'
            f' {from_block} - {to_block} | asset_contract: {asset_address}'
        ),
    )

    return epoch_volume_logs
