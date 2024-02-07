import json

from redis import asyncio as aioredis
from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from ..redis_keys import aave_cached_block_height_asset_data
from .constants import DETAILS_BASIS
from .constants import ORACLE_DECIMALS
from .constants import RAY
from .helpers import calculate_compound_interest
from .helpers import calculate_current_from_scaled
from .helpers import get_asset_metadata
from .helpers import get_pool_supply_events
from .helpers import rayMul
from .models.data_models import AaveDebtData
from .models.data_models import AaveSupplyData
from .models.data_models import AssetTotalData
from .models.data_models import epochEventVolumeData
from .models.data_models import eventVolumeData
from .models.data_models import UiDataProviderReserveData
from .models.data_models import volumeData
from .pricing import get_asset_price_in_block_range

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
        asset_data = list()

    core_logger.debug(
        (
            'get asset supply bulk fetched block details for epoch for:'
            f' {asset_address}'
        ),
    )

    data_dict = {}
    # get cached asset data from redis
    cached_data_dict = await redis_conn.zrangebyscore(
        name=aave_cached_block_height_asset_data.format(
            asset_address,
        ),
        min=int(from_block),
        max=int(to_block),
    )

    if cached_data_dict and len(cached_data_dict) == to_block - (from_block - 1):
        data_dict = {
            json.loads(
                data.decode(
                    'utf-8',
                ),
            )['blockHeight']: json.loads(
                data.decode('utf-8'),
            )['data']
            for data in cached_data_dict
        }

    asset_supply_debt_dict = dict()

    for block_num in range(from_block, to_block + 1):
        current_block_details = block_details_dict.get(block_num, None)
        timestamp = current_block_details.get('timestamp')
        asset_data = data_dict.get(block_num, None)
        asset_data = UiDataProviderReserveData(
            *asset_data.values(),
        )

        variable_interest = calculate_compound_interest(
            rate=asset_data.variableBorrowRate,
            current_timestamp=timestamp,
            last_update_timestamp=asset_data.lastUpdateTimestamp,

        )

        stable_interest = calculate_compound_interest(
            rate=asset_data.averageStableRate,
            current_timestamp=timestamp,
            last_update_timestamp=asset_data.stableDebtLastUpdateTimestamp,

        )

        total_variable_debt = calculate_current_from_scaled(
            scaled_value=asset_data.totalScaledVariableDebt,
            index=asset_data.variableBorrowIndex,
            interest=variable_interest,
        )

        total_stable_debt = rayMul(asset_data.totalPrincipalStableDebt, stable_interest)
        total_supply = asset_data.availableLiquidity + total_variable_debt + total_stable_debt

        asset_usd_price = asset_data.priceInMarketReferenceCurrency * (10 ** -ORACLE_DECIMALS)

        total_supply_usd = (total_supply * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        total_variable_debt_usd = (total_variable_debt * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        total_stable_debt_usd = (total_stable_debt * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        available_liquidity_usd = (asset_data.availableLiquidity * asset_usd_price) / \
            (10 ** int(asset_metadata['decimals']))

        # Normalize detail rates
        asset_data.assetDetails.ltv = (asset_data.assetDetails.ltv / DETAILS_BASIS) * 100
        asset_data.assetDetails.liqThreshold = (asset_data.assetDetails.liqThreshold / DETAILS_BASIS) * 100
        asset_data.assetDetails.resFactor = (asset_data.assetDetails.resFactor / DETAILS_BASIS) * 100
        asset_data.assetDetails.liqBonus = ((asset_data.assetDetails.liqBonus / DETAILS_BASIS) * 100) - 100
        asset_data.assetDetails.eLtv = (asset_data.assetDetails.eLtv / DETAILS_BASIS) * 100
        asset_data.assetDetails.eliqThreshold = (asset_data.assetDetails.eliqThreshold / DETAILS_BASIS) * 100
        asset_data.assetDetails.eliqBonus = ((asset_data.assetDetails.eliqBonus / DETAILS_BASIS) * 100) - 100
        asset_data.assetDetails.optimalRate = round(asset_data.assetDetails.optimalRate / RAY, 2)

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
            assetDetails=asset_data.assetDetails,
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

    data_dict = {}
    # get cached asset data from redis
    cached_data_dict = await redis_conn.zrangebyscore(
        name=aave_cached_block_height_asset_data.format(
            asset_address,
        ),
        min=int(from_block),
        max=int(to_block),
    )

    if cached_data_dict and len(cached_data_dict) == to_block - (from_block - 1):
        data_dict = {
            json.loads(
                data.decode(
                    'utf-8',
                ),
            )['blockHeight']: json.loads(
                data.decode('utf-8'),
            )['data']
            for data in cached_data_dict
        }

    if data_dict:
        block_usd_prices = {
            key: data['priceInMarketReferenceCurrency'] * (10 ** -ORACLE_DECIMALS)
            for key, data in data_dict.items()
        }
    else:
        block_usd_prices = await get_asset_price_in_block_range(
            asset_metadata,
            from_block,
            to_block,
            redis_conn,
            rpc_helper,
        )

    supply_events = await get_pool_supply_events(
        rpc_helper=rpc_helper,
        from_block=from_block,
        to_block=to_block,
        redis_conn=redis_conn,
    )

    # TODO: Filter events by address in get_pool_data_events?
    asset_supply_events = {
        key: filter(lambda x: x['args']['reserve'] == asset_address, value)
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
    )

    for block_num in range(from_block, to_block + 1):
        asset_usd_price = block_usd_prices.get(block_num, 0)
        asset_usd_price = asset_usd_price / (10 ** int(asset_metadata['decimals']))

        for event in asset_supply_events.get(block_num, None):
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
