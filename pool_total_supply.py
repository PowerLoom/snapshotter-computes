import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from computes.utils.core import get_asset_supply_and_debt_bulk
from computes.utils.models.data_models import AssetTotalData
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot
from computes.utils.models.message_models import EpochBaseSnapshot


class AssetTotalSupplyProcessor(GenericProcessorSnapshot):
    """
    Processor for computing total supply and related metrics for assets in the Aave protocol.
    """

    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AssetTotalSupplyProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ) -> Optional[Dict[str, Union[int, float]]]:
        """
        Compute the total supply and related metrics for an asset over a given epoch.

        Args:
            epoch (PowerloomSnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): RPC helper for blockchain interactions.

        Returns:
            Optional[Dict[str, Union[int, float]]]: A snapshot of the asset's total supply and related metrics.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        data_source_contract_address = epoch.data_source

        # Initialize dictionaries to store snapshot data for each block in the epoch
        epoch_asset_snapshot_map_total_supply = dict()
        epoch_asset_snapshot_map_liquidity_rate = dict()
        epoch_asset_snapshot_map_liquidity_index = dict()

        epoch_asset_snapshot_map_total_stable_debt = dict()
        epoch_asset_snapshot_map_total_variable_debt = dict()
        epoch_asset_snapshot_map_variable_borrow_rate = dict()
        epoch_asset_snapshot_map_stable_borrow_rate = dict()
        epoch_asset_snapshot_map_variable_borrow_index = dict()
        epoch_asset_snapshot_map_variable_borrow_rate = dict()

        epoch_asset_snapshot_map_last_update_timestamp = dict()
        epoch_asset_snapshot_map_isolated_debt = dict()
        epoch_asset_snapshot_map_asset_details = dict()
        epoch_asset_snapshot_map_available_liquidity = dict()
        epoch_asset_snapshot_map_rate_details = dict()

        max_block_timestamp = int(time.time())

        self._logger.debug(f'asset supply {data_source_contract_address} computation init time {time.time()}')
        
        # Fetch asset supply and debt data for the entire epoch
        asset_supply_debt_total: Dict[str, AssetTotalData] = await get_asset_supply_and_debt_bulk(
            asset_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            fetch_timestamp=True,
        )

        # Process data for each block in the epoch
        for block_num in range(min_chain_height, max_chain_height + 1):
            block_asset_supply_debt = asset_supply_debt_total.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False

            # Populate snapshot maps with data for each block
            epoch_asset_snapshot_map_total_supply[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalSupply
            epoch_asset_snapshot_map_liquidity_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.liquidityRate
            epoch_asset_snapshot_map_liquidity_index[
                f'block{block_num}'
            ] = block_asset_supply_debt.liquidityIndex
            epoch_asset_snapshot_map_total_stable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalStableDebt
            epoch_asset_snapshot_map_total_variable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalVariableDebt
            epoch_asset_snapshot_map_variable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.variableBorrowRate
            epoch_asset_snapshot_map_stable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.stableBorrowRate
            epoch_asset_snapshot_map_variable_borrow_index[
                f'block{block_num}'
            ] = block_asset_supply_debt.variableBorrowIndex
            epoch_asset_snapshot_map_last_update_timestamp[
                f'block{block_num}'
            ] = block_asset_supply_debt.lastUpdateTimestamp
            epoch_asset_snapshot_map_asset_details[
                f'block{block_num}'
            ] = block_asset_supply_debt.assetDetails
            epoch_asset_snapshot_map_available_liquidity[
                f'block{block_num}'
            ] = block_asset_supply_debt.availableLiquidity
            epoch_asset_snapshot_map_rate_details[
                f'block{block_num}'
            ] = block_asset_supply_debt.rateDetails
            epoch_asset_snapshot_map_isolated_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.isolationModeTotalDebt

            # Update max_block_timestamp if timestamp is available for the last block
            if fetch_ts:
                if not block_asset_supply_debt.timestamp:
                    self._logger.error(
                        (
                            'Could not fetch timestamp against max block'
                            ' height in epoch {} - {}to calculate pair'
                            ' reserves for contract {}. Using current time'
                            ' stamp for snapshot construction'
                        ),
                        data_source_contract_address,
                        min_chain_height,
                        max_chain_height,
                    )
                else:
                    max_block_timestamp = block_asset_supply_debt.timestamp

        # Create the final snapshot object
        asset_total_snapshot = AavePoolTotalAssetSnapshot(
            **{
                'totalAToken': epoch_asset_snapshot_map_total_supply,
                'liquidityRate': epoch_asset_snapshot_map_liquidity_rate,
                'liquidityIndex': epoch_asset_snapshot_map_liquidity_index,
                'totalVariableDebt': epoch_asset_snapshot_map_total_variable_debt,
                'totalStableDebt': epoch_asset_snapshot_map_total_stable_debt,
                'variableBorrowRate': epoch_asset_snapshot_map_variable_borrow_rate,
                'stableBorrowRate': epoch_asset_snapshot_map_stable_borrow_rate,
                'variableBorrowIndex': epoch_asset_snapshot_map_variable_borrow_index,
                'lastUpdateTimestamp': epoch_asset_snapshot_map_last_update_timestamp,
                'isolationModeTotalDebt': epoch_asset_snapshot_map_isolated_debt,
                'assetDetails': epoch_asset_snapshot_map_asset_details,
                'rateDetails': epoch_asset_snapshot_map_rate_details,
                'availableLiquidity': epoch_asset_snapshot_map_available_liquidity,
                'chainHeightRange': EpochBaseSnapshot(
                    begin=min_chain_height, end=max_chain_height,
                ),
                'timestamp': max_block_timestamp,
                'contract': data_source_contract_address,
            },
        )
        self._logger.debug(f'asset supply {data_source_contract_address}, computation end time {time.time()}')

        return asset_total_snapshot
