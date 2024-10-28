import time

from eth_utils import keccak
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper
from ipfs_client.main import AsyncIPFSClient

from computes.settings.config import settings as module_settings
from computes.utils.core import get_asset_trade_volume
from computes.utils.models.message_models import AaveSupplyVolumeSnapshot, EpochBaseSnapshot


class AssetSupplyVolumeProcessor(GenericProcessor):
    """
    Processor for computing supply volume and related metrics for assets in the Aave protocol.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module='AssetSupplyVolumeProcessor')

    async def _compute_single(
        self,
        data_source_contract_address: str,
        min_chain_height: int,
        max_chain_height: int,
        rpc_helper: RpcHelper,
        preloader_results: dict,
    ):
        bulk_asset = preloader_results.get('bulk_asset', {})
        bulk_event = preloader_results.get('bulk_event', {})
        block_details_dict = preloader_results.get('block_details', {})

        if not bulk_asset or not bulk_event:
            self._logger.error('Bulk asset or event data not found in preloader results')

        all_assets_price_dict = bulk_asset[1]
        all_assets_events_dict = bulk_event
        
        result = await get_asset_trade_volume(
            asset_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
            all_assets_price_dict=all_assets_price_dict,
            all_assets_events_dict=all_assets_events_dict,
            block_details_dict=block_details_dict,
        )

        max_block_timestamp = result.get('timestamp', int(time.time()))
        result.pop('timestamp', None)

        events = [log for key in result.keys() for log in result[key]['logs']]

        supply_volume_snapshot = AaveSupplyVolumeSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(begin=min_chain_height, end=max_chain_height),
            timestamp=max_block_timestamp,
            borrow=result['borrow']['totals'],
            repay=result['repay']['totals'],
            supply=result['supply']['totals'],
            withdraw=result['withdraw']['totals'],
            liquidation=result['liquidation']['totalLiquidatedCollateral'],
            events=events,
            liquidationList=result['liquidation']['liquidations'],
        )

        return supply_volume_snapshot

    def _gen_pair_idx_to_compute(self, msg_obj: SnapshotProcessMessage):
        monitored_pairs = module_settings.initial_pools
        current_epoch = msg_obj.epochId
        snapshotter_hash = keccak(int(settings.instance_id.lower(), 16))
        current_day = msg_obj.day
        return (current_epoch + int.from_bytes(snapshotter_hash, 'big') + settings.slot_id + current_day) % len(monitored_pairs)

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        preloader_results: dict,
    ):
        min_chain_height = msg_obj.begin
        max_chain_height = msg_obj.end

        monitored_pools = module_settings.initial_pools
        self._logger.debug(f'pool supply volume computation init time {time.time()}')

        pair_idx = self._gen_pair_idx_to_compute(msg_obj)
        data_source_contract_address = monitored_pools[pair_idx]

        snapshot = await self._compute_single(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            rpc_helper=rpc_helper,
            preloader_results=preloader_results,
        )

        self._logger.debug(f'pool supply volume, computation end time {time.time()}')

        return [(data_source_contract_address, snapshot)]
