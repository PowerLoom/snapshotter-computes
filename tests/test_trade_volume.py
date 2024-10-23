import asyncio
from web3 import Web3
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.modules.computes.trade_volume import TradeVolumeProcessor
from snapshotter.utils.snapshot_utils import get_eth_price_usd
from snapshotter.modules.computes.utils.models.message_models import UniswapTradesSnapshot

async def test_trade_volume_compute():
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
    
    # Initialize TradeVolumeProcessor
    processor = TradeVolumeProcessor()
    
    # Get ETH price dictionary
    eth_price_dict = await get_eth_price_usd(start_block, end_block, rpc_helper)
    
    # Call the compute function
    result = await processor.compute(
        msg_obj=msg_obj,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=rpc_helper,  # Using the same RPC helper for simplicity
        ipfs_reader=None,  # You may need to mock this or provide a real IPFS reader
        protocol_state_contract=None,  # You may need to mock this or provide a real contract
        eth_price_dict=eth_price_dict,
    )
    
    # Assert that we got a result
    assert result is not None
    assert len(result) > 0
    
    # Unpack the result
    data_source_contract_address, snapshot = result[0]
    
    # Assert that the data_source_contract_address is correct
    assert Web3.is_address(data_source_contract_address)
    
    # Assert that the snapshot has the correct structure
    assert isinstance(snapshot, UniswapTradesSnapshot)
    assert hasattr(snapshot, 'contract')
    assert hasattr(snapshot, 'chainHeightRange')
    assert hasattr(snapshot, 'timestamp')
    assert hasattr(snapshot, 'totalTrade')
    assert hasattr(snapshot, 'totalFee')
    assert hasattr(snapshot, 'token0TradeVolume')
    assert hasattr(snapshot, 'token1TradeVolume')
    assert hasattr(snapshot, 'token0TradeVolumeUSD')
    assert hasattr(snapshot, 'token1TradeVolumeUSD')
    assert hasattr(snapshot, 'events')
    
    # Assert that the chainHeightRange is correct
    assert snapshot.chainHeightRange.begin == start_block
    assert snapshot.chainHeightRange.end == end_block
    
    # Assert that the trade volumes and fees are non-negative
    assert snapshot.totalTrade >= 0
    assert snapshot.totalFee >= 0
    assert snapshot.token0TradeVolume >= 0
    assert snapshot.token1TradeVolume >= 0
    assert snapshot.token0TradeVolumeUSD >= 0
    assert snapshot.token1TradeVolumeUSD >= 0
    
    # Assert that the events attribute has the expected structure
    assert hasattr(snapshot.events, 'Swap')
    assert hasattr(snapshot.events, 'Mint')
    assert hasattr(snapshot.events, 'Burn')
    assert hasattr(snapshot.events, 'Trades')
    
    # You may want to add more specific assertions based on your knowledge of the expected data

if __name__ == "__main__":
    asyncio.run(test_trade_volume_compute())

