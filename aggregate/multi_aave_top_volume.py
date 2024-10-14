from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from computes.utils.helpers import get_asset_metadata
from computes.utils.models.message_models import AaveTopAssetsVolumeSnapshot
from computes.utils.models.message_models import AaveTopAssetVolumeSnapshot
from computes.utils.models.message_models import AaveVolumeAggregateSnapshot


class AggreagateTopVolumeProcessor(GenericProcessorAggregate):
    """
    Processor for aggregating top volume data across multiple Aave pools.
    """

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
        """
        Compute aggregated top volume data for Aave pools.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): Message object containing calculation details.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper for the source chain.
            anchor_rpc_helper (RpcHelper): RPC helper for the anchor chain.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract for accessing protocol state.
            project_id (str): ID of the project.

        Returns:
            AaveTopAssetsVolumeSnapshot: Aggregated top volume snapshot.
        """

        self._logger.info(f'Calculating top volume data for {msg_obj}')
        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        projects_metadata = {}

        # Fetch snapshot data for all messages
        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        complete_flags = []
        # Process snapshot data and fetch asset metadata
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

        # Calculate volume data for each snapshot
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            asset_metadata = projects_metadata[snapshot_project_id]
            decimals = 10 ** int(asset_metadata['decimals'])

            # Normalize token values
            snapshot.totalBorrow.totalToken /= decimals
            snapshot.totalRepay.totalToken /= decimals
            snapshot.totalSupply.totalToken /= decimals
            snapshot.totalWithdraw.totalToken /= decimals
            snapshot.totalLiquidatedCollateral.totalToken /= decimals

            # Initialize volume data for the asset
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

        # Calculate 24-hour changes
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
                        # Calculate percentage changes for each metric
                        self._calculate_percentage_change(volume_data[asset.address], asset)

        # Create top assets volume list
        top_assets_volume = []
        for asset in volume_data.values():
            top_assets_volume.append(AaveTopAssetVolumeSnapshot.parse_obj(asset))

        # Sort top assets by borrow volume
        top_assets_volume = sorted(top_assets_volume, key=lambda x: x.totalBorrow.totalUSD, reverse=True)

        # Create and return the final snapshot
        top_assets_volume_snapshot = AaveTopAssetsVolumeSnapshot(
            epochId=epoch_id,
            assets=top_assets_volume,
        )

        if not all(complete_flags):
            top_assets_volume_snapshot.complete = False

        self._logger.info(f'Got top volume data: {top_assets_volume_snapshot}')

        return top_assets_volume_snapshot

    def _calculate_percentage_change(self, current_asset: dict, previous_asset: AaveTopAssetVolumeSnapshot):
        """
        Calculate percentage changes for various metrics of an asset.

        Args:
            current_asset (dict): Current asset data.
            previous_asset (AaveTopAssetVolumeSnapshot): Previous asset data.
        """
        metrics = [
            ('borrow', 'totalBorrow'),
            ('repay', 'totalRepay'),
            ('supply', 'totalSupply'),
            ('withdraw', 'totalWithdraw'),
            ('liquidation', 'totalLiquidatedCollateral'),
        ]

        for change_key, total_key in metrics:
            previous_value = getattr(previous_asset, total_key).totalUSD
            if previous_value > 0:
                current_value = current_asset[total_key].totalUSD
                change = (current_value - previous_value) / previous_value * 100
                current_asset[f'{change_key}Change24h'] = change
