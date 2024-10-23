import asyncio
from web3 import Web3
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.modules.computes.pair_total_reserves import PairTotalReservesProcessor
from snapshotter.utils.snapshot_utils import get_eth_price_usd


async def test_pair_total_reserves_compute():
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
        epochId=1,  # You may want to adjust this
        day=1,  # You may want to adjust this
    )
    
    # Initialize PairTotalReservesProcessor
    processor = PairTotalReservesProcessor()
    
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
    assert hasattr(snapshot, 'token0Reserves')
    assert hasattr(snapshot, 'token1Reserves')
    assert hasattr(snapshot, 'token0ReservesUSD')
    assert hasattr(snapshot, 'token1ReservesUSD')
    assert hasattr(snapshot, 'token0Prices')
    assert hasattr(snapshot, 'token1Prices')
    assert hasattr(snapshot, 'chainHeightRange')
    assert hasattr(snapshot, 'timestamp')
    assert hasattr(snapshot, 'contract')
    
    # Assert that the chainHeightRange is correct
    assert snapshot.chainHeightRange.begin == start_block
    assert snapshot.chainHeightRange.end == end_block
    
    # Assert that we have data for each block in the range
    for block in range(start_block, end_block + 1):
        block_key = f'block{block}'
        assert block_key in snapshot.token0Reserves
        assert block_key in snapshot.token1Reserves
        assert block_key in snapshot.token0ReservesUSD
        assert block_key in snapshot.token1ReservesUSD
        assert block_key in snapshot.token0Prices
        assert block_key in snapshot.token1Prices
    
    # You may want to add more specific assertions based on your knowledge of the expected data

if __name__ == "__main__":
    asyncio.run(test_pair_total_reserves_compute())

