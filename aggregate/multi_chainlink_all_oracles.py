from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from ..utils.helpers import get_oracle_metadata
from ..utils.models.message_models import ChainlinkAllOraclesSnapshot
from ..utils.models.message_models import ChainlinkOracleAggregateSnapshot
from ..utils.models.message_models import ChainlinkOracleAnswersSnapshot
from ..utils.models.message_models import ChainlinkOracleData


class AggregateAllOraclesProcessor(GenericProcessorAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateAllOraclesProcessor')

    async def compute(
        self,
        msg_obj: PowerloomCalculateAggregateMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,

    ):

        self._logger.info(f'Calculating all oracle data for {msg_obj}')
        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        projects_metadata = {}

        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        complete_flags = []
        for msg, data in zip(msg_obj.messages, snapshot_data):
            if not data:
                continue
            if 'answers' in msg.projectId:
                snapshot = ChainlinkOracleAnswersSnapshot.parse_obj(data)
            elif 'average' in msg.projectId:
                snapshot = ChainlinkOracleAggregateSnapshot.parse_obj(data)
                complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot

            oracle_address = msg.projectId.split(':')[-2]
            oracle_metadata = await get_oracle_metadata(
                oracle_address=oracle_address,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            )

            projects_metadata[msg.projectId] = oracle_metadata

        oracle_data = {}

        # iterate over all snapshots and generate oracle data
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            oracle_metadata = projects_metadata[snapshot_project_id]
            oracle_address = snapshot_project_id.split(':')[-2]

            oracle_data[oracle_address] = {
                'address': oracle_address,
                'description': oracle_metadata['description'],
                'decimals': oracle_metadata['decimals'],
            }

            if 'answers' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end
                oracle_data[oracle_address]['answer'] = snapshot.oracleAnswers[f'block{max_epoch_block}']

            elif 'average' in snapshot_project_id:
                oracle_data[oracle_address]['answer24hAvg'] = snapshot.averageAnswer
                oracle_data[oracle_address]['avgSampleSize'] = snapshot.sampleSize

        all_oracles = []
        for oracle in oracle_data.values():
            all_oracles.append(ChainlinkOracleData.parse_obj(oracle))

        all_oracles = sorted(all_oracles, key=lambda x: x.description, reverse=True)

        all_oracle_snapshot = ChainlinkAllOraclesSnapshot(
            epochId=epoch_id,
            oracles=all_oracles,
        )

        if not all(complete_flags):
            all_oracle_snapshot.complete = False

        self._logger.info(f'Got all oracle data: {all_oracle_snapshot}')

        return all_oracle_snapshot
