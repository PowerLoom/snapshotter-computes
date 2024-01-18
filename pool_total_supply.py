import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .utils.core import get_asset_supply_and_debt_bulk
from .utils.models.message_models import AavePoolTotalAssetSnapshot
from .utils.models.message_models import EpochBaseSnapshot


class AssetTotalSupplyProcessor(GenericProcessorSnapshot):
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

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        data_source_contract_address = epoch.data_source

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

        max_block_timestamp = int(time.time())

        self._logger.debug(f'asset supply {data_source_contract_address} computation init time {time.time()}')
        asset_supply_debt_total = await get_asset_supply_and_debt_bulk(
            asset_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            fetch_timestamp=True,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            block_asset_supply_debt = asset_supply_debt_total.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False

            epoch_asset_snapshot_map_total_supply[
                f'block{block_num}'
            ] = block_asset_supply_debt['total_supply']
            epoch_asset_snapshot_map_liquidity_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt['liquidity_rate']
            epoch_asset_snapshot_map_liquidity_index[
                f'block{block_num}'
            ] = block_asset_supply_debt['liquidity_index']
            epoch_asset_snapshot_map_total_stable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt['total_stable_debt']
            epoch_asset_snapshot_map_total_variable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt['total_variable_debt']
            epoch_asset_snapshot_map_variable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt['variable_borrow_rate']
            epoch_asset_snapshot_map_stable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt['stable_borrow_rate']
            epoch_asset_snapshot_map_variable_borrow_index[
                f'block{block_num}'
            ] = block_asset_supply_debt['variable_borrow_index']
            epoch_asset_snapshot_map_last_update_timestamp[
                f'block{block_num}'
            ] = block_asset_supply_debt['last_update_timestamp']

            if fetch_ts:
                if not block_asset_supply_debt.get('timestamp', None):
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
                    max_block_timestamp = block_asset_supply_debt.get(
                        'timestamp',
                    )
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

                'chainHeightRange': EpochBaseSnapshot(
                    begin=min_chain_height, end=max_chain_height,
                ),
                'timestamp': max_block_timestamp,
                'contract': data_source_contract_address,
            },
        )
        self._logger.debug(f'asset supply {data_source_contract_address}, computation end time {time.time()}')

        return asset_total_snapshot
