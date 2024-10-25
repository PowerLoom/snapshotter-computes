import asyncio
from web3 import Web3
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from computes.pool_supply_volume import AssetSupplyVolumeProcessor
from computes.utils.models.data_models import volumeData
from computes.utils.models.message_models import AaveSupplyVolumeSnapshot
from computes.utils.helpers import get_bulk_asset_data, get_pool_supply_events


async def test_pool_supply_volume_compute():
    # Initialize RpcHelper and other necessary objects
    rpc_helper = RpcHelper()
    await rpc_helper.init()
    
    # Get the current block number
    current_block = await rpc_helper.get_current_block_number()
    end_block = current_block - 1
    start_block = end_block - 9
    
    msg_obj = SnapshotProcessMessage(
        begin=start_block,
        end=end_block,
        epochId=1,
        day=1,
    )

    processor = AssetSupplyVolumeProcessor()
    
    # Get bulk asset data
    bulk_asset_results = await get_bulk_asset_data(
        rpc_helper=rpc_helper,
        from_block=start_block,
        to_block=end_block,
    )
    
    # Get bulk event data
    bulk_event_results = await get_pool_supply_events(
        rpc_helper=rpc_helper,
        from_block=start_block,
        to_block=end_block,
    )
    
    # Prepare preloader results
    mock_preloader_results = {
        'bulk_asset': bulk_asset_results,
        'bulk_event': bulk_event_results
    }
    
    result = await processor.compute(
        msg_obj=msg_obj,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=rpc_helper,
        ipfs_reader=None,
        protocol_state_contract=None,
        preloader_results=mock_preloader_results,
    )
    
    # Assert that we got a result
    assert result is not None
    assert len(result) > 0
    
    
    data_source_contract_address, snapshot = result[0]
    
    assert Web3.is_address(data_source_contract_address)
    
    assert isinstance(snapshot, AaveSupplyVolumeSnapshot)
    assert hasattr(snapshot, 'borrow')
    assert hasattr(snapshot, 'repay')
    assert hasattr(snapshot, 'supply')
    assert hasattr(snapshot, 'withdraw')
    assert hasattr(snapshot, 'liquidation')
    assert hasattr(snapshot, 'events')
    assert hasattr(snapshot, 'liquidationList')
    assert hasattr(snapshot, 'chainHeightRange')
    assert hasattr(snapshot, 'timestamp')
    assert hasattr(snapshot, 'contract')
    
    assert snapshot.chainHeightRange.begin == start_block
    assert snapshot.chainHeightRange.end == end_block
    
    assert snapshot.contract == data_source_contract_address
    
    # Assert that the timestamp is present
    assert snapshot.timestamp is not None
    
    # Assert that the volume data structures are present and have the correct format
    for volume_type in ['borrow', 'repay', 'supply', 'withdraw']:
        volume_data = getattr(snapshot, volume_type)
        assert isinstance(volume_data, volumeData)
        assert hasattr(volume_data, 'totalUSD')
        assert hasattr(volume_data, 'totalToken')
    
    # Assert that the liquidation data structure is present and has the correct format
    assert isinstance(snapshot.liquidation, volumeData)
    assert hasattr(snapshot.liquidation, 'totalUSD')
    assert hasattr(snapshot.liquidation, 'totalToken')
    
    # Assert that events and liquidationList are lists
    assert isinstance(snapshot.events, list)
    assert isinstance(snapshot.liquidationList, list)

if __name__ == "__main__":
    asyncio.run(test_pool_supply_volume_compute())
