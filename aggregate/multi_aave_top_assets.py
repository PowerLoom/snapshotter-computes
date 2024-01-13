from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from ..utils.models.message_models import AavePoolTotalAssetSnapshot
from ..utils.models.message_models import AaveTopAssetSnapshot
from ..utils.models.message_models import AaveTopAssetsSnapshot
from ..utils.constants import ray, seconds_in_year
from ..utils.helpers import get_asset_metadata
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper


class AggreagateTopAssetsProcessor(GenericProcessorAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateTopAssetsProcessor')

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

        self._logger.info(f'Calculating top asset data for {msg_obj}')
        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        projects_metadata = {}

        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        for msg, data in zip(msg_obj.messages, snapshot_data):
            snapshot = AavePoolTotalAssetSnapshot.parse_obj(data)
            snapshot_mapping[msg.projectId] = snapshot
            asset_address = msg.projectId.split(':')[-2]

            asset_metadata = await get_asset_metadata(
                asset_address=asset_address,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            )

            projects_metadata[msg.projectId] = asset_metadata

        asset_data = {}

        # iterate over all snapshots and generate asset data
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            asset_metadata = projects_metadata[snapshot_project_id]

            max_epoch_block = snapshot.chainHeightRange.end

            self._logger.info(f'Got meta data: {asset_metadata}')

            asset_data[asset_metadata['address']] = {
                'address': asset_metadata['address'],
                'name': asset_metadata['name'],
                'symbol': asset_metadata['symbol'],
                'decimals': asset_metadata['decimals'],
            }

            supply_apr = snapshot.liquidityRate[f'block{max_epoch_block}'] / ray
            variable_apr = snapshot.variableBorrowRate[f'block{max_epoch_block}'] / ray

            supply_apy = (((1 + (supply_apr / seconds_in_year)) ** seconds_in_year) - 1) * 100
            variable_apy = (((1 + (variable_apr / seconds_in_year)) ** seconds_in_year) - 1) * 100

            asset_data[asset_metadata['address']]['totalAToken'] = snapshot.totalAToken[f'block{max_epoch_block}']
            asset_data[asset_metadata['address']]['liquidityApy'] = supply_apy

            asset_data[asset_metadata['address']]['totalVariableDebt'] = snapshot.totalVariableDebt[f'block{max_epoch_block}']
            asset_data[asset_metadata['address']]['variableApy'] = variable_apy

        top_assets = []
        for asset in asset_data.values():
            self._logger.info(f'Got asset data: {asset}')
            top_assets.append(AaveTopAssetSnapshot.parse_obj(asset))

        top_assets = sorted(top_assets, key=lambda x: x.totalAToken.usd_supply, reverse=True)

        top_assets_snapshot = AaveTopAssetsSnapshot(
            epochId=epoch_id,
            assets=top_assets,
        )

        self._logger.info(f'Got top asset data: {top_assets_snapshot}')
        
        return top_assets_snapshot
