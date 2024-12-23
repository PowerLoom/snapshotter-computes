from typing import Optional

from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import (
    get_block_details_in_block_range,
)
from web3 import Web3

from computes.utils.constants import AAVE_CORE_EVENTS
from computes.utils.constants import DETAILS_BASIS
from computes.utils.constants import ORACLE_DECIMALS
from computes.utils.helpers import calculate_compound_interest_rate
from computes.utils.helpers import calculate_current_from_scaled
from computes.utils.helpers import convert_from_ray
from computes.utils.helpers import get_asset_metadata
from computes.utils.helpers import get_bulk_asset_data
from computes.utils.helpers import get_pool_supply_events
from computes.utils.helpers import rayMul
from computes.utils.models.data_models import AaveDebtData
from computes.utils.models.data_models import AaveSupplyData
from computes.utils.models.data_models import AssetDetailsData
from computes.utils.models.data_models import AssetTotalData
from computes.utils.models.data_models import epochEventVolumeData
from computes.utils.models.data_models import eventLiquidationData
from computes.utils.models.data_models import eventVolumeData
from computes.utils.models.data_models import liquidationData
from computes.utils.models.data_models import RateDetailsData
from computes.utils.models.data_models import UiDataProviderReserveData
from computes.utils.models.data_models import volumeData
from computes.utils.pricing import get_all_asset_prices

core_logger = logger.bind(module='PowerLoom|AaveCore')


async def get_asset_supply_and_debt_bulk(
    asset_address,
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    all_assets_data_dict: Optional[dict] = {},
    all_assets_price_dict: Optional[dict] = {},
    block_details_dict: Optional[dict] = {}
):
    """
    Retrieves the supply and debt data for a specific asset over a range of blocks.

    Args:
        asset_address (str): The address of the asset.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        rpc_helper (RpcHelper): RPC helper object.
        asset_metadata (dict): Metadata for the asset.
        block_details_dict (dict): Dictionary of block details.
        all_assets_data_dict (dict): Cached asset data for all assets.
        all_assets_price_dict (dict): Cached asset prices for all assets.

    Returns:
        dict: A dictionary containing supply and debt data for each block in the range.
    """
    core_logger.debug(
        f'Starting bulk asset total supply query for: {asset_address}',
    )
    asset_address = Web3.to_checksum_address(asset_address)

    # Fetch block details if not provided
    if not block_details_dict:
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

    core_logger.debug(
        (
            'get asset supply bulk fetched block details for epoch for:'
            f' {asset_address}'
        ),
    )

    # Fetch asset data if not provided
    if not all_assets_data_dict or not all_assets_price_dict:
        all_assets_data_dict, all_assets_price_dict = await get_bulk_asset_data(
            rpc_helper=rpc_helper,
            from_block=from_block,
            to_block=to_block,
        )
    
    asset_metadata = await get_asset_metadata(
        asset_address=asset_address,
        rpc_helper=rpc_helper,
    )

    # Get asset-specific data
    asset_data_dict = all_assets_data_dict.get(asset_address, {})

    asset_supply_debt_dict = dict()

    # Process data for each block in the range
    for block_num in range(from_block, to_block + 1):
        current_block_details = block_details_dict.get(block_num, None)
        timestamp = current_block_details.get('timestamp')

        # Get the asset data, details and rate details for the current block
        block_data = asset_data_dict.get(block_num, None)
        if not block_data:
            continue

        asset_data = UiDataProviderReserveData.parse_obj(block_data['asset_data'])
        asset_details = AssetDetailsData.parse_obj(block_data['asset_details'])
        asset_rate_details = RateDetailsData.parse_obj(block_data['rate_details'])

        # Calculate the accrued interest for the asset from the last update timestamp to the current block timestamp.
        # Last update timestamp is updated when an action (borrow, supply, etc.) is taken on-chain, but interest 
        # continues to accrue in the supply and debt token contracts between actions.
        variable_interest = calculate_compound_interest_rate(
            rate=asset_data.variableBorrowRate,
            current_timestamp=timestamp,
            last_update_timestamp=asset_data.lastUpdateTimestamp,
        )

        # Calculate current debt values
        total_variable_debt = calculate_current_from_scaled(
            scaled_value=asset_data.totalScaledVariableDebt,
            index=asset_data.variableBorrowIndex,
            interest_rate=variable_interest,
        )

        # Calculate total supply and USD values
        total_supply = asset_data.availableLiquidity + total_variable_debt
        asset_usd_price = asset_data.priceInMarketReferenceCurrency * (10 ** -ORACLE_DECIMALS)
        total_supply_usd = (total_supply * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        total_variable_debt_usd = (total_variable_debt * asset_usd_price) / (10 ** int(asset_metadata['decimals']))
        available_liquidity_usd = (asset_data.availableLiquidity * asset_usd_price) / \
            (10 ** int(asset_metadata['decimals']))

        # Normalize asset detail rates
        asset_details.ltv = (asset_details.ltv / DETAILS_BASIS) * 100
        asset_details.liqThreshold = (asset_details.liqThreshold / DETAILS_BASIS) * 100
        asset_details.resFactor = (asset_details.resFactor / DETAILS_BASIS) * 100
        asset_details.liqBonus = ((asset_details.liqBonus / DETAILS_BASIS) * 100) - 100
        
        # Normalize e-mode data
        for e_mode_data in asset_details.eModeData:
            e_mode_data.eLtv = (e_mode_data.eLtv / DETAILS_BASIS) * 100
            e_mode_data.eliqThreshold = (e_mode_data.eliqThreshold / DETAILS_BASIS) * 100
            e_mode_data.eliqBonus = ((e_mode_data.eliqBonus / DETAILS_BASIS) * 100) - 100

        # Normalize rate detail rates, rates and slopes are return in RAY format
        asset_rate_details.utilRate = total_variable_debt / total_supply
        asset_rate_details.varRateSlope1 = convert_from_ray(asset_rate_details.varRateSlope1)
        asset_rate_details.varRateSlope2 = convert_from_ray(asset_rate_details.varRateSlope2)
        asset_rate_details.baseVarRate = convert_from_ray(asset_rate_details.baseVarRate)
        asset_rate_details.optimalRate = convert_from_ray(asset_rate_details.optimalRate)

        # Create AssetTotalData object with all calculated values
        total_asset_data = AssetTotalData(
            totalSupply=AaveSupplyData(
                token_supply=total_supply,
                usd_supply=total_supply_usd,
            ),
            availableLiquidity=AaveSupplyData(
                token_supply=asset_data.availableLiquidity,
                usd_supply=available_liquidity_usd,
            ),
            totalVariableDebt=AaveDebtData(
                token_debt=total_variable_debt,
                usd_debt=total_variable_debt_usd,
            ),
            liquidityRate=asset_data.liquidityRate,
            liquidityIndex=asset_data.liquidityIndex,
            variableBorrowRate=asset_data.variableBorrowRate,
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
    rpc_helper: RpcHelper,
    all_assets_price_dict: Optional[dict] = {},
    all_assets_events_dict: Optional[dict] = {},
    block_details_dict: Optional[dict] = {},
):
    """
    Retrieves the trade volume data for a specific asset over a range of blocks.

    Args:
        asset_address (str): The address of the asset.
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        rpc_helper (RpcHelper): RPC helper object.
        block_details_dict (dict): Dictionary of block details.
        asset_metadata (dict): Metadata for the asset.
        all_assets_price_dict (dict): Dictionary of asset prices for all assets.
        all_assets_events_dict (dict): Dictionary of events for all assets.  # Updated description

    Returns:
        dict: A dictionary containing trade volume data for the asset.
    """
    asset_address = Web3.to_checksum_address(asset_address)

    # Fetch block details if not provided
    if not block_details_dict:
        try:
            block_details_dict = await get_block_details_in_block_range(
                from_block=from_block,
                to_block=to_block,
                rpc_helper=rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                'Error attempting to get block details of to_block {}: {}, retrying again',
                to_block,
                err,
            )
            raise err

    # Extract price data for the specific asset from all_assets_price_dict or fetch if not available
    if all_assets_price_dict:
        price_dict = {
            block_num: {asset_address: prices.get(asset_address, 0)}
            for block_num, prices in all_assets_price_dict.items()
        }
    else:
        core_logger.warning("all_assets_price_dict not provided, fetching price data...")
        try:
            price_dict = await get_all_asset_prices(
                from_block,
                to_block,
                rpc_helper,
            )
        except Exception as err:
            core_logger.opt(exception=True).error(
                'Error fetching asset prices: {}',
                err,
            )
            raise err

    # Fetch events for all assets in the pool if not provided
    if not all_assets_events_dict:
        all_assets_events_dict = await get_pool_supply_events(
            rpc_helper=rpc_helper,
            from_block=from_block,
            to_block=to_block,
        )

    # Filter events for the specific asset
    asset_supply_events = {
        key: filter(
            lambda x:
            x['args'].get('reserve', '') == asset_address or
            x['args'].get('collateralAsset', '') == asset_address,
            value,
        )
        for key, value in all_assets_events_dict.items()
    }

    # Initialize data models with empty/0 values
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

    asset_metadata = await get_asset_metadata(
        asset_address=asset_address,
        rpc_helper=rpc_helper,
    )

    # Process events for each block in the range
    for block_num in range(from_block, to_block + 1):
        # Get the asset price for the current block
        block_all_asset_prices = price_dict.get(block_num, {})
        asset_usd_price = block_all_asset_prices.get(asset_address, 0)
        asset_usd_price = asset_usd_price * (10 ** -ORACLE_DECIMALS) / (10 ** int(asset_metadata['decimals']))

        # Iterate over the current block's events and update the respective volume data
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

                # Get the price for the repaid debt asset
                debt_usd_price = block_all_asset_prices.get(Web3.to_checksum_address(debt_asset), 0)

                # Fetch decimal data for the debt asset
                debt_asset_metadata = await get_asset_metadata(
                    asset_address=debt_asset,
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
