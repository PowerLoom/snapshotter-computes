import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.modules.computes.utils.constants import lido_contract_object
from snapshotter.modules.computes.utils.core import get_lido_staking_yield
from snapshotter.modules.computes.utils.models.message_models import LidoStakingYieldSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper


class LidoStakingYieldProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='LidoStakingYieldProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        epoch_apr_snapshot_map = dict()
        epoch_report_timestamp_map = dict()

        lido_contract_address = lido_contract_object.address

        self._logger.debug(f'Lido staking yield computation init time {time.time()}')

        apr_data_dict = await get_lido_staking_yield(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            block_apr_data = apr_data_dict.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False

            epoch_apr_snapshot_map[
                f'block{block_num}'
            ] = block_apr_data['staking_apr']
            epoch_report_timestamp_map[
                f'block{block_num}'
            ] = block_apr_data['report_timestamp']

            if fetch_ts:
                max_block_timestamp = block_apr_data.get(
                    'timestamp',
                )

        staking_yield_snapshot = LidoStakingYieldSnapshot(
            contract=lido_contract_address,
            chainHeightRange={'begin': min_chain_height, 'end': max_chain_height},
            timestamp=max_block_timestamp,
            stakingApr=epoch_apr_snapshot_map,
            reportTimestamp=epoch_report_timestamp_map,
        )

        self._logger.debug(f'Lido staking yield computation end time {time.time()}')

        return staking_yield_snapshot
