import time
from typing import List, Tuple
from typing import Optional

from rpc_helper.rpc import RpcHelper

from computes.utils.core import base_snapshot_from_block_range
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from computes.utils.models.message_models import UniswapBaseSnapshot
from ipfs_client.main import AsyncIPFSClient


class PairTotalReservesProcessor(GenericProcessor):
    """
    Processor for calculating and snapshotting total reserves for Uniswap pairs.
    
    This class handles the computation of total reserves for Uniswap V3 pools within a given epoch.
    It fetches block details, identifies active pools, and calculates reserves for each pool.
    """

    def __init__(self) -> None:
        """
        Initialize the processor with a logger instance.
        """
        self._logger = logger.bind(module="PairTotalReservesProcessor")

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        preloader_results: dict,
    ) -> List[Tuple[str, UniswapBaseSnapshot]]:
        """
        Compute the total reserves for Uniswap pairs within the given epoch.

        Args:
            epoch (SnapshotProcessMessage): The epoch information containing begin and end block heights.
            rpc_helper (RpcHelper): RPC helper for main blockchain interactions.
            anchor_rpc_helper (RpcHelper): RPC helper for anchor chain interactions.
            ipfs_reader (AsyncIPFSClient): IPFS client for reading data.
            protocol_state_contract: Contract instance for protocol state queries.
            preloader_results (dict): Preloader results for the epoch.

        Returns:
            List[Tuple[str, UniswapBaseSnapshot]]: List of tuples containing task identifiers and their corresponding snapshot data.
        """
        
        min_chain_height = msg_obj.end
        max_chain_height = msg_obj.end
        snapshots = list()

        block_details_dict = preloader_results.get('block_details', None)

        test_address = "0xc7bBeC68d12a0d1830360F8Ec58fA599bA1b0e9b"
        # Process each active pool to compute reserves
        for pool_address in [test_address]:
            self._logger.debug(
                "[Epoch {}-{}] Processing pool {} | Starting computation",
                min_chain_height,
                max_chain_height,
                pool_address
            )

            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Starting token pair reserves computation (will return UniswapBaseSnapshot) | Wall time: {}",
                min_chain_height,
                max_chain_height,
                pool_address,
                time.time()
            )
            
            # Fetch and compute reserves for the current pool
            base_snapshot_data: Optional[UniswapBaseSnapshot] = await base_snapshot_from_block_range(
                pair_address=pool_address,
                from_block=min_chain_height,
                to_block=max_chain_height,
                rpc_helper=rpc_helper,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                block_details_dict=block_details_dict,
            )

            if not base_snapshot_data:
                self._logger.error(
                    "[Epoch {}-{}] Pool {} | No UniswapBaseSnapshot data returned by 'get_pair_reserves()'",
                    min_chain_height,
                    max_chain_height,
                    pool_address
                )
                continue

            self._logger.debug(
                "[Epoch {}-{}] Pool {} | Computation completed (UniswapBaseSnapshot received) | Wall time: {}",
                min_chain_height,
                max_chain_height,
                pool_address,
                time.time()
            )
            snapshots.append((pool_address, base_snapshot_data))

        return snapshots