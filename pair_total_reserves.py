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
from computes.settings.config import settings as computes_settings
from snapshotter.settings.config import settings
import requests
from computes.utils.slot_selection import SlotSelectionManager
from tenacity import retry, stop_after_attempt, wait_random_exponential

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

    @retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=5, max=60))
    async def _get_epoch_active_pools(self, min_chain_height: int) -> List[str]:
        """
        Fetch the list of active pools for a given epoch (determined by min_chain_height)
        from the Block Data Service (BDS) API.

        This method attempts to GET the active pool addresses by querying the BDS API endpoint
        `/get_previous_epoch_info/{min_chain_height}`. Retries are handled automatically with
        exponential backoff (via the tenacity.retry decorator).

        Args:
            min_chain_height (int): The block height (start of epoch) to use for querying the BDS.

        Returns:
            List[str]: List of active pool addresses as strings for the specified epoch.

        Raises:
            Exception: If the BDS API response status is not 200 or a networking/decoding error occurs.
        """
        bds_api_url = computes_settings.bds_api_url  # Retrieve the BDS API base URL from settings
        try:
            # Send a GET request to the BDS API to fetch previous epoch info for the specified block height
            response = requests.get(f"{bds_api_url}/get_previous_epoch_info/{min_chain_height}")

            # Check if the API response status is not OK; raise an error with contextual logging
            if response.status_code != 200:
                self._logger.error(f"Failed to fetch active pools from BDS. Status code: {response.status_code}")
                raise Exception(f"Failed to fetch active pools from BDS. Status code: {response.status_code}")

            # Log the full JSON response for traceability
            self._logger.info(f"BDS response for active pools: {response.json()}")

            # Extract the list of pool addresses (keys from the 'pools' dictionary)
            active_pools = list(response.json()['pools'].keys())

        except Exception as e:
            # Log any failure that occurs during the fetch or JSON processing step, and re-raise the error
            self._logger.error(f"Exception occurred while fetching active pools from BDS: {e}")
            raise Exception(f"Failed to fetch active pools from BDS: {e}")

        return active_pools

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

        min_chain_height = msg_obj.begin
        max_chain_height = msg_obj.begin  # Single block epoch
        bds_api_url = computes_settings.bds_api_url
        block_details_dict = preloader_results.get('block_details', None)

        # Get current slot ID from settings
        slot_id = settings.slot_id
        
        # Get total node count from contract (cached 30s)
        # TODO: This uses nodeCount which includes burned/disabled nodes.
        # Some selected slot IDs may have no active node - those epochs
        # simply won't receive submissions from those slots.
        total_slots = await SlotSelectionManager.get_total_slots(rpc_helper, protocol_state_contract)
        
        # Get epoch end block hash from preloader results
        block_hash = None
        if block_details_dict and max_chain_height in block_details_dict:
            epoch_end_block = block_details_dict.get(max_chain_height, {})
            block_hash = epoch_end_block.get('hash', None)
        
        if not block_hash:
            # Fallback: fetch block hash directly via RPC
            # NOTE: This should rarely happen - preloader should provide block hash
            self._logger.warning(
                f"Block hash not found in preloader results for block {max_chain_height}, "
                f"falling back to RPC (this indicates preloader issue)"
            )
            try:
                block = await rpc_helper.get_current_node()['web3_client'].eth.get_block(max_chain_height)
                block_hash = block.get('hash', b'').hex() if block else None
            except Exception as e:
                self._logger.error(f"Failed to fetch block hash for block {max_chain_height}: {e}")
                return []
        
        if not block_hash:
            self._logger.error(f"Could not obtain block hash for epoch {msg_obj.epochId}")
            return []

        # Fetch active pools from BDS API
        active_pools = await self._get_epoch_active_pools(min_chain_height)

        if len(active_pools) == 0:
            self._logger.error(f"No active pools found for epoch {msg_obj.epochId} at block {min_chain_height}")
            return []

        # CRITICAL: Sort pool addresses for determinism across all nodes
        active_pools_sorted = sorted(active_pools)
        
        # Log determinism-critical parameters for debugging
        self._logger.debug(
            f"Slot assignment inputs - epoch: {msg_obj.epochId}, block: {max_chain_height}, "
            f"block_hash: {block_hash[:10]}..., slot_id: {slot_id}, total_slots: {total_slots}, "
            f"active_pools: {len(active_pools_sorted)}, first_3_pools: {active_pools_sorted[:3]}"
        )
        
        # Check if this slot is selected and get assigned pool
        assigned_pool = SlotSelectionManager.get_pool_for_slot(
            slot_id=slot_id,
            epoch_id=msg_obj.epochId,
            block_hash=block_hash,
            active_pool_addresses=active_pools_sorted,
            total_slots=total_slots
        )
        
        if assigned_pool is None:
            self._logger.info(
                f"Slot {slot_id} NOT selected for epoch {msg_obj.epochId} "
                f"(total_slots={total_slots}, block_hash: {block_hash[:10]}...), skipping computation"
            )
            return []
        
        pool_address = assigned_pool
        self._logger.info(
            f"Slot {slot_id} SELECTED for epoch {msg_obj.epochId}, "
            f"assigned pool: {pool_address} (total_slots={total_slots}, active_pools={len(active_pools_sorted)}, "
            f"block_hash: {block_hash[:10]}...)"
        )

        # fetch previous snapshots data
        previous_snapshot_response = requests.get(f"{bds_api_url}/previous_snapshots_data/{pool_address}/{min_chain_height}")

        self._logger.info(f"Fetching previous snapshots data for pool {pool_address} at block {min_chain_height}")

        if previous_snapshot_response.status_code != 200:
            self._logger.error(f"Failed to fetch previous snapshots data from bds: {previous_snapshot_response.status_code}")
            raise Exception(f"Failed to fetch previous snapshots data from bds: {previous_snapshot_response.status_code}")
        previous_snapshot_data = previous_snapshot_response.json()
        # parse into proper format
        previous_snapshot_data = [tuple(data) for data in previous_snapshot_data]

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
            return

        self._logger.debug(
            "[Epoch {}-{}] Pool {} | Computation completed (UniswapBaseSnapshot received) | Wall time: {}",
            min_chain_height,
            max_chain_height,
            pool_address,
            time.time()
        )

        base_snapshot_data.previousSnapshots = previous_snapshot_data

        self._logger.debug(f"Base snapshot data: {base_snapshot_data.model_dump_json()}")

        return [(pool_address, base_snapshot_data)]