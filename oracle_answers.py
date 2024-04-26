import time
from typing import Dict
from typing import Optional
from typing import Union

from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .utils.core import get_oracle_answers
from .utils.models.data_models import LatestRoundData
from .utils.models.message_models import ChainlinkOracleAnswersSnapshot
from .utils.models.message_models import EpochBaseSnapshot


class OracleAnswerProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='OracleAnswerProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        data_source_contract_address = epoch.data_source

        epoch_round_id_map = dict()
        epoch_answer_map= dict()
        epoch_started_at_map = dict()
        epoch_updated_at_map = dict()
        epoch_round_answer_map = dict()
        epoch_block_timestamp_map = dict()
        max_block_timestamp = int(time.time())

        self._logger.debug(f'oracle answers for: {data_source_contract_address} - computation init time {time.time()}')
        oracle_epoch_answers: Dict[int, LatestRoundData] = await get_oracle_answers(
            oracle_contract_address=data_source_contract_address,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            from_block=min_chain_height,
            to_block=max_chain_height,
            fetch_timestamp=True,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            oracle_block_answer = oracle_epoch_answers.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False

            epoch_round_id_map[
                f'block{block_num}'
            ] = oracle_block_answer.roundId
            epoch_answer_map[
                f'block{block_num}'
            ] = oracle_block_answer.answer
            epoch_started_at_map[
                f'block{block_num}'
            ] = oracle_block_answer.startedAt
            epoch_updated_at_map[
                f'block{block_num}'
            ] = oracle_block_answer.updatedAt
            epoch_round_answer_map[
                f'block{block_num}'
            ] = oracle_block_answer.answeredInRound
            epoch_block_timestamp_map[
                f'block{block_num}'
            ] = oracle_block_answer.blockTimestamp

            if fetch_ts:
                if not oracle_block_answer.blockTimestamp:
                    self._logger.error(
                        (
                            'Could not fetch timestamp against max block'
                            ' height in epoch {} - {} to get oracle answers'
                            ' for contract {}. Using current time'
                            ' stamp for snapshot construction'
                        ),
                        data_source_contract_address,
                        min_chain_height,
                        max_chain_height,
                    )
                else:
                    max_block_timestamp = oracle_block_answer.blockTimestamp

        oracle_answers_snapshot = ChainlinkOracleAnswersSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(begin=min_chain_height, end=max_chain_height),
            timestamp=max_block_timestamp,
            roundIds=epoch_round_id_map,
            oracleAnswers=epoch_answer_map,
            roundStartTimes=epoch_started_at_map,
            roundUpdateTimes=epoch_updated_at_map,
            answeredInRounds=epoch_round_answer_map,
            blockTimestamps=epoch_block_timestamp_map,
        )

        self._logger.debug(f'oracle answers for: {data_source_contract_address} - computation end time {time.time()}')

        return oracle_answers_snapshot
