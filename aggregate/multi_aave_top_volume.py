from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from ..utils.helpers import get_asset_metadata
from ..utils.models.message_models import AaveTopAssetsVolumeSnapshot
from ..utils.models.message_models import AaveTopAssetVolumeSnapshot
from ..utils.models.message_models import AaveVolumeAggregateSnapshot


class AggreagateTopVolumeProcessor(GenericProcessorAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateTopVolumeProcessor')

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

        self._logger.info(f'Calculating top volume data for {msg_obj}')
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
            snapshot = AaveVolumeAggregateSnapshot.parse_obj(data)
            complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot
            asset_address = msg.projectId.split(':')[-2]

            asset_metadata = await get_asset_metadata(
                asset_address=asset_address,
                redis_conn=redis,
                rpc_helper=rpc_helper,
            )

            projects_metadata[msg.projectId] = asset_metadata

        volume_data = {}

        # iterate over all snapshots and generate asset data
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            asset_metadata = projects_metadata[snapshot_project_id]
            decimals = 10 ** int(asset_metadata['decimals'])

            # normalize token values
            snapshot.totalBorrow.totalToken /= decimals
            snapshot.totalRepay.totalToken /= decimals
            snapshot.totalSupply.totalToken /= decimals
            snapshot.totalWithdraw.totalToken /= decimals

            volume_data[asset_metadata['address']] = {
                'name': asset_metadata['name'],
                'symbol': asset_metadata['symbol'],
                'address': asset_metadata['address'],
                'totalBorrow': snapshot.totalBorrow,
                'totalRepay': snapshot.totalRepay,
                'totalSupply': snapshot.totalSupply,
                'totalWithdraw': snapshot.totalWithdraw,
                'totalLiquidatedCollateral': snapshot.totalLiquidatedCollateral,
                'borrowChange24h': 0,
                'repayChange24h': 0,
                'supplyChange24h': 0,
                'withdrawChange24h': 0,
                'liquidationChange24h': 0,
            }

        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )

        if not extrapolated_flag:
            previous_top_assets_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_top_assets_snapshot_data:
                previous_top_assets_snapshot = AaveTopAssetsVolumeSnapshot.parse_obj(previous_top_assets_snapshot_data)
                for asset in previous_top_assets_snapshot.assets:
                    if asset.address in volume_data:
                        borrow_before_24h = asset.totalBorrow.totalUSD
                        repay_before_24h = asset.totalRepay.totalUSD
                        supply_before_24h = asset.totalSupply.totalUSD
                        withdraw_before_24h = asset.totalWithdraw.totalUSD
                        liquidation_before_24h = asset.totalLiquidatedCollateral.usd_supply

                        if borrow_before_24h > 0:
                            volume_data[asset.address]['borrowChange24h'] = (
                                volume_data[asset.address]['totalBorrow'].totalUSD - borrow_before_24h
                            ) / borrow_before_24h * 100
                        if repay_before_24h > 0:
                            volume_data[asset.address]['repayChange24h'] = (
                                volume_data[asset.address]['totalRepay'].totalUSD - repay_before_24h
                            ) / repay_before_24h * 100
                        if supply_before_24h > 0:
                            volume_data[asset.address]['supplyChange24h'] = (
                                volume_data[asset.address]['totalSupply'].totalUSD - supply_before_24h
                            ) / supply_before_24h * 100
                        if withdraw_before_24h > 0:
                            volume_data[asset.address]['withdrawChange24h'] = (
                                volume_data[asset.address]['totalWithdraw'].totalUSD - withdraw_before_24h
                            ) / withdraw_before_24h * 100
                        if liquidation_before_24h > 0:
                            volume_data[asset.address]['liquidationChange24h'] = (
                                volume_data[asset.address]['totalLiquidatedCollateral'].usd_supply -
                                liquidation_before_24h
                            ) / liquidation_before_24h * 100

        top_assets_volume = []
        for asset in volume_data.values():
            top_assets_volume.append(AaveTopAssetVolumeSnapshot.parse_obj(asset))

        top_assets_volume = sorted(top_assets_volume, key=lambda x: x.totalBorrow.totalUSD, reverse=True)

        top_assets_volume_snapshot = AaveTopAssetsVolumeSnapshot(
            epochId=epoch_id,
            assets=top_assets_volume,
        )

        if not all(complete_flags):
            top_assets_volume_snapshot.complete = False

        self._logger.info(f'Got top volume data: {top_assets_volume_snapshot}')

        return top_assets_volume_snapshot
