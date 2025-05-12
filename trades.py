import time
from typing import List, Tuple
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from computes.metadata import MetadataProcessor
from computes.utils.core import get_block_details_in_block_range
from computes.utils.core import get_pair_trade_volume
from computes.utils.models.message_models import (
    EpochBaseSnapshot,
    UniswapTradesSnapshot,
    TradeType,
    UniswapTrade
)
from snapshotter.settings.config import settings
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger


class TradesProcessor(GenericProcessorSnapshot):
    """
    Processor for calculating and storing trade volume for Uniswap pairs.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="TradeVolumeProcessor")

    async def compute(
        self,
        epoch: SnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        task_type: str,
    ) -> List[Tuple[str, UniswapTradesSnapshot]]:
        """
        Compute the trade volume for a Uniswap pair within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Protocol state contract instance.
            task_type (str): The task type string for formatting the snapshot key.

        Returns:
            List[Tuple[str, UniswapTradesSnapshot]]: List of (snapshot_key, snapshot_data).
        """
        
        min_chain_height = epoch.begin
        max_chain_height = epoch.end
        snapshots: List[Tuple[str, UniswapTradesSnapshot]] = list()

        try:
            block_details_dict = await get_block_details_in_block_range(
                min_chain_height,
                max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
            self._logger.debug(
                "[Epoch {}-{}] Block details fetched successfully for {} blocks",
                min_chain_height,
                max_chain_height,
                len(block_details_dict)
            )
        except Exception as err:
            self._logger.opt(exception=True).error(
                "[Epoch {}-{}] Failed to fetch block details: {}",
                min_chain_height,
                max_chain_height,
                err
            )
            block_details_dict = dict()

        # find list of active pools for the epoch
        active_pool_set_keys_to_fetch = []
        for block_number in range(min_chain_height, max_chain_height + 1):
            key = f"active_pools:{block_number}:{settings.namespace}"
            active_pool_set_keys_to_fetch.append(key)
        
        active_pool_addresses_bytes = set()
        if active_pool_set_keys_to_fetch:
            active_pool_addresses_bytes = await redis_conn.sunion(*active_pool_set_keys_to_fetch)
        
        active_pool_addresses = [addr.decode('utf-8') for addr in active_pool_addresses_bytes]
        active_pool_addresses = [Web3.to_checksum_address(addr) for addr in active_pool_addresses]
        
        self._logger.info(
            "[Epoch {}-{}] Starting token pair reserves computation for {} active pools",
            min_chain_height,
            max_chain_height,
            len(active_pool_addresses)
        )

        metadata_processor = MetadataProcessor()

        for pool_address in active_pool_addresses:
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
            
            pair_trade_data = await get_pair_trade_volume(
                pair_address=pool_address,
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
                anchor_rpc_helper=anchor_rpc_helper,
                ipfs_reader=ipfs_reader,
                protocol_state_contract=protocol_state_contract,
                metadata_processor=metadata_processor,
                block_details_dict=block_details_dict,
            )

            if not pair_trade_data or not pair_trade_data.get('trades'):
                self._logger.error(
                    "[Epoch {}-{}] Pool {} | No pool trade data returned or trades list is empty from 'get_pair_trade_volume()'",
                    min_chain_height,
                    max_chain_height,
                    pool_address
                )
                continue

            transformed_trades: List[UniswapTrade] = []
            for processed_log in pair_trade_data['trades']: 
                try:
                    trade_type_enum = TradeType(processed_log.eventName)
                except ValueError:
                    self._logger.warning(
                        "[Epoch {}-{}] Pool {} | Unknown eventName '{}' encountered for tradeType mapping. Skipping trade log: {}",
                        min_chain_height, max_chain_height, pool_address, processed_log.eventName, processed_log.txHash
                    )
                    continue

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

                decoded_data_component = {
                    **processed_log.args, 
                    "calculated_token0_amount": processed_log.token0_amount,
                    "calculated_token1_amount": processed_log.token1_amount,
                    "block_timestamp": processed_log.timestamp,
                    "calculated_trade_amount_usd": processed_log.trade_amount_usd,
                }
                
                uniswap_trade_entry = UniswapTrade(
                    tradeType=trade_type_enum,
                    log=raw_log_component,
                    data=decoded_data_component
                )
                transformed_trades.append(uniswap_trade_entry)
            
            epoch_snapshot_model = EpochBaseSnapshot(**pair_trade_data['epoch'])
            
            current_trades_snapshot = UniswapTradesSnapshot(
                address=pair_trade_data['address'], 
                epoch=epoch_snapshot_model,
                trades=transformed_trades
            )
            
            snapshot_key = task_type.format(poolAddress=pool_address, Namespace=settings.namespace)
            snapshots.append((snapshot_key, current_trades_snapshot))

            self._logger.info(
                "[Epoch {}-{}] Pool {} | UniswapTradesSnapshot created with {} trades. Wall time: {:.4f}s",
                min_chain_height,
                max_chain_height,
                pool_address,
                len(transformed_trades),
                time.time() - start_time
            )
            
        return snapshots

        