import asyncio
from web3 import Web3
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from computes.pool_total_supply import AssetTotalSupplyProcessor
from computes.utils.models.message_models import AavePoolTotalAssetSnapshot
from computes.utils.helpers import get_bulk_asset_data

async def test_pool_total_supply_compute():
    # Initialize RpcHelper and other necessary objects
    rpc_helper = RpcHelper()
    await rpc_helper.init()
    
    # Get the current block number
    current_block = await rpc_helper.get_current_block_number()
    
    # Calculate the range for the previous 10 blocks
    end_block = current_block - 1
    start_block = end_block - 9
    
    # Create a SnapshotProcessMessage
    msg_obj = SnapshotProcessMessage(
        begin=start_block,
        end=end_block,
        epochId=1,
        day=1,
    )
    
    # Initialize AssetTotalSupplyProcessor
    processor = AssetTotalSupplyProcessor()
    
    # Get bulk asset data
    results = await get_bulk_asset_data(
        rpc_helper=rpc_helper,
        from_block=start_block,
        to_block=end_block,
    )
    
    # Prepare preloader results
    mock_preloader_results = {
        'bulk_asset': results
    }
    
    # Call the compute function
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
    
    # Unpack the result
    data_source_contract_address, snapshot = result[0]
    
    # Assert that the data_source_contract_address is correct
    assert Web3.is_address(data_source_contract_address)
    
    # Assert that the snapshot has the correct structure
    assert isinstance(snapshot, AavePoolTotalAssetSnapshot)
    assert hasattr(snapshot, 'totalAToken')
    assert hasattr(snapshot, 'liquidityRate')
    assert hasattr(snapshot, 'liquidityIndex')
    assert hasattr(snapshot, 'totalVariableDebt')
    assert hasattr(snapshot, 'totalStableDebt')
    assert hasattr(snapshot, 'variableBorrowRate')
    assert hasattr(snapshot, 'stableBorrowRate')
    assert hasattr(snapshot, 'variableBorrowIndex')
    assert hasattr(snapshot, 'lastUpdateTimestamp')
    assert hasattr(snapshot, 'isolationModeTotalDebt')
    assert hasattr(snapshot, 'assetDetails')
    assert hasattr(snapshot, 'rateDetails')
    assert hasattr(snapshot, 'availableLiquidity')
    assert hasattr(snapshot, 'chainHeightRange')
    assert hasattr(snapshot, 'timestamp')
    assert hasattr(snapshot, 'contract')
    
    # Assert that the chainHeightRange is correct
    assert snapshot.chainHeightRange.begin == start_block
    assert snapshot.chainHeightRange.end == end_block
    
    # Assert that the contract address matches
    assert snapshot.contract == data_source_contract_address
    
    # Assert that the timestamp is present
    assert snapshot.timestamp is not None

if __name__ == "__main__":
    asyncio.run(test_pool_total_supply_compute())
