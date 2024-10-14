from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from snapshotter.utils.callback_helpers import GenericProcessorAggregate
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.rpc import RpcHelper

from computes.utils.constants import RAY
from computes.utils.constants import SECONDS_IN_YEAR
from computes.utils.helpers import get_asset_metadata
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot
from computes.utils.models.message_models import AaveTopAssetSnapshot
from computes.utils.models.message_models import AaveTopAssetsSnapshot
from computes.utils.models.message_models import AaveTopDebtData
from computes.utils.models.message_models import AaveTopSupplyData


class AggreagateTopAssetsProcessor(GenericProcessorAggregate):
    """
    Processor for aggregating top assets data across multiple Aave pools.
    """

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
        """
        Compute aggregated top assets data for Aave pools.

        Args:
            msg_obj (PowerloomCalculateAggregateMessage): Message object containing calculation details.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper for the source chain.
            anchor_rpc_helper (RpcHelper): RPC helper for the anchor chain.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract for accessing protocol state.
            project_id (str): ID of the project.

        Returns:
            AaveTopAssetsSnapshot: Aggregated top assets snapshot.
        """

        self._logger.info(f'Calculating top asset data for {msg_obj}')
        epoch_id = msg_obj.epochId

        snapshot_mapping = {}
        projects_metadata = {}

        # Fetch snapshot data for all messages
        snapshot_data = await get_submission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        # Process snapshot data and fetch asset metadata
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

        # Calculate asset data for each snapshot
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]
            asset_metadata = projects_metadata[snapshot_project_id]

            max_epoch_block = snapshot.chainHeightRange.end

            self._logger.info(f'Got meta data: {asset_metadata}')

            # Initialize asset data
            asset_data[asset_metadata['address']] = {
                'address': asset_metadata['address'],
                'name': asset_metadata['name'],
                'symbol': asset_metadata['symbol'],
                'decimals': asset_metadata['decimals'],
            }

            # Calculate APY and other metrics
            supply_apr = snapshot.liquidityRate[f'block{max_epoch_block}'] / RAY
            variable_apr = snapshot.variableBorrowRate[f'block{max_epoch_block}'] / RAY

            supply_apy = (((1 + (supply_apr / SECONDS_IN_YEAR)) ** SECONDS_IN_YEAR) - 1) * 100
            variable_apy = (((1 + (variable_apr / SECONDS_IN_YEAR)) ** SECONDS_IN_YEAR) - 1) * 100

            token_supply_conv = snapshot.totalAToken[f'block{max_epoch_block}'].token_supply / (
                10 ** int(asset_metadata['decimals'])
            )
            token_debt_conv = snapshot.totalVariableDebt[f'block{max_epoch_block}'].token_debt / (
                10 ** int(asset_metadata['decimals'])
            )

            # Populate asset data
            asset_data[asset_metadata['address']]['liquidityApy'] = supply_apy
            asset_data[asset_metadata['address']]['totalAToken'] = AaveTopSupplyData(
                token_supply=token_supply_conv,
                usd_supply=snapshot.totalAToken[f'block{max_epoch_block}'].usd_supply,
            )

            asset_data[asset_metadata['address']]['variableApy'] = variable_apy
            asset_data[asset_metadata['address']]['totalVariableDebt'] = AaveTopDebtData(
                token_debt=token_debt_conv,
                usd_debt=snapshot.totalVariableDebt[f'block{max_epoch_block}'].usd_debt,
            )

            isIsolated = snapshot.isolationModeTotalDebt[f'block{max_epoch_block}'] > 0
            asset_data[asset_metadata['address']]['isIsolated'] = isIsolated

        # Create top assets list
        top_assets = []
        for asset in asset_data.values():
            self._logger.info(f'Got asset data: {asset}')
            top_assets.append(AaveTopAssetSnapshot.parse_obj(asset))

        # Sort top assets by supply amount in USD
        top_assets = sorted(top_assets, key=lambda x: x.totalAToken.usd_supply, reverse=True)

        # Create and return the final snapshot
        top_assets_snapshot = AaveTopAssetsSnapshot(
            epochId=epoch_id,
            assets=top_assets,
        )

        self._logger.info(f'Got top asset data: {top_assets_snapshot}')

        return top_assets_snapshot
