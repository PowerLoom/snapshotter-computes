import time
from typing import List, Tuple
from ipfs_client.main import AsyncIPFSClient
from rpc_helper.rpc import RpcHelper

from computes.utils.core import get_pair_trade_volume
from computes.utils.models.message_models import (
    EpochBaseSnapshot,
    UniswapTradesSnapshot,
    TradeType,
    UniswapTrade
)
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger


class TradesProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and storing trade volume for Uniswap pairs.
    
    This class handles the computation and storage of trade volume data for Uniswap V3 pools
    within a given epoch. It processes trade events, calculates volumes, and creates snapshots
    of trading activity for each active pool.
    """

    def __init__(self) -> None:
        """
        Initialize the processor with a logger instance.
        """
        self._logger = logger.bind(module="TradeVolumeProcessor")

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        preloader_results: dict,
    ) -> List[Tuple[str, UniswapTradesSnapshot]]:
        """
        Compute the trade volume for Uniswap pairs within the given epoch.

        This method processes trade events for all active pools in the epoch, calculates
        trade volumes, and creates snapshots of trading activity. It handles:
        - Fetching block details and active pools
        - Processing trade events for each pool
        - Calculating trade volumes and USD values
        - Creating snapshots of trading activity

        Args:
            epoch (SnapshotProcessMessage): The epoch information containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching and data storage.
            rpc_helper (RpcHelper): RPC helper for main blockchain interactions.
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Protocol state contract instance.
            task_type (str): The task type string for formatting the snapshot key.

        Returns:
            List[Tuple[str, UniswapTradesSnapshot]]: List of (snapshot_key, snapshot_data) pairs.
        """
        
        min_chain_height = msg_obj.begin
        max_chain_height = msg_obj.end
        snapshots: List[Tuple[str, UniswapTradesSnapshot]] = list()
        epoch_snapshot_model = EpochBaseSnapshot(**msg_obj.model_dump())
        
        eth_price_dict = preloader_results.get('eth_price', None)
        block_details_dict = preloader_results.get('block_details', None)

        test_address = "0xc7bBeC68d12a0d1830360F8Ec58fA599bA1b0e9b"

        # Process each active pool
        for pool_address in [test_address]:
            self._logger.debug(
                "[Epoch {}-{}] Processing pool {} | Starting computation",
                min_chain_height,
                max_chain_height,
                pool_address
            )
            
            start_time = time.time()
            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Starting trades snapshot computation | Wall time: {}",
                min_chain_height,
                max_chain_height,
                pool_address,
                start_time
            )
            
            # Fetch trade data for the pool
            pair_trade_data = await get_pair_trade_volume(
                pair_address=pool_address,
                from_block=min_chain_height,
                to_block=max_chain_height,
                rpc_helper=rpc_helper,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                block_details_dict=block_details_dict,
            )

            # Skip if no trade data available
            if not pair_trade_data or not pair_trade_data.get('trades'):
                self._logger.error(
                    "[Epoch {}-{}] Pool {} | No pool trade data returned or trades list is empty from 'get_pair_trade_volume()'",
                    min_chain_height,
                    max_chain_height,
                    pool_address
                )
                continue

            # Process and transform trade logs
            transformed_trades: List[UniswapTrade] = []
            for processed_log in pair_trade_data['trades']: 
                # Map event name to trade type
                try:
                    trade_type_enum = TradeType(processed_log.eventName)
                except ValueError:
                    self._logger.warning(
                        "[Epoch {}-{}] Pool {} | Unknown eventName '{}' encountered for tradeType mapping. Skipping trade log: {}",
                        min_chain_height, max_chain_height, pool_address, processed_log.eventName, processed_log.txHash
                    )
                    continue
                
                # Get ETH price for the block
                eth_price = eth_price_dict.get(processed_log.blockNumber)
                if not eth_price:
                    self._logger.warning(
                        "[Epoch {}-{}] Pool {} | No ETH price found for block number {}. Skipping trade log: {}",
                        min_chain_height, max_chain_height, pool_address, processed_log.blockNumber, processed_log.txHash
                    )
                    eth_price = 0

                # Create raw log component
                raw_log_component = {
                    "address": processed_log.address,
                    "topics": processed_log.topics,
                    "data": processed_log.data,
                    "blockNumber": processed_log.blockNumber,
                    "transactionHash": processed_log.txHash,
                    "transactionIndex": processed_log.txIndex,
                    "logIndex": processed_log.logIndex,
                    "eventName": processed_log.eventName,
                    "filterName": processed_log.filterName,
                    "_score": processed_log.score,
                }

                # Create decoded data component with calculated values
                decoded_data_component = {
                    **processed_log.args, 
                    "calculated_token0_amount": processed_log.token0_amount,
                    "calculated_token1_amount": processed_log.token1_amount,
                    "block_timestamp": processed_log.timestamp,
                    "calculated_trade_amount_usd": processed_log.trade_amount_usd,
                    "calculated_eth_price": eth_price,
                }
                
                # Create and append trade entry
                uniswap_trade_entry = UniswapTrade(
                    tradeType=trade_type_enum,
                    log=raw_log_component,
                    data=decoded_data_component
                )
                transformed_trades.append(uniswap_trade_entry)
            
            # Create trades snapshot
            current_trades_snapshot = UniswapTradesSnapshot(
                address=pair_trade_data['address'], 
                epoch=epoch_snapshot_model,
                trades=transformed_trades
            )
            
            snapshots.append((pool_address, current_trades_snapshot))

            self._logger.info(
                "[Epoch {}-{}] Pool {} | UniswapTradesSnapshot created with {} trades. Wall time: {:.4f}s",
                min_chain_height,
                max_chain_height,
                pool_address,
                len(transformed_trades),
                time.time() - start_time
            )

        return snapshots

        