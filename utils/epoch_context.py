"""
Slot-agnostic epoch context and per-pool computation for BDS data markets.

This module extracts the shared computation primitives that both the lite node
(via pair_total_reserves.py) and the future bulk snapshotter service can use
without any slot ID coupling.

Functions:
    get_epoch_active_pools: Fetch active pool addresses from BDS API
    prepare_epoch: Gather all epoch-level data needed for computation
    compute_pool_snapshot: Compute base snapshot for a single pool
"""

import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests
from computes.utils.reserves_cache import ReservesCache
from tenacity import retry, stop_after_attempt, wait_random_exponential

from rpc_helper.rpc import RpcHelper
from computes.utils.core import base_snapshot_from_block_range
from computes.utils.models.message_models import UniswapBaseSnapshot
from computes.utils.slot_selection import SlotSelectionManager
from computes.settings.config import settings as computes_settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage

epoch_context_logger = logger.bind(module='EpochContext')


@dataclass
class EpochContext:
    """Holds all epoch-level data needed for pool computation. No slot awareness."""
    block_hash: str
    total_slots: int
    active_pools_sorted: List[str]
    block_details_dict: dict
    min_chain_height: int
    max_chain_height: int
    bds_api_url: str


@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=5, max=60))
async def get_epoch_active_pools(
    epoch_block_height: int,
    bds_api_url: str,
) -> List[str]:
    """
    Fetch the list of active pools for a given epoch from the Block Data Service (BDS) API.

    Args:
        epoch_block_height: The block height to query BDS with.
        bds_api_url: Base URL for the BDS API.

    Returns:
        List of active pool addresses for the specified epoch.

    Raises:
        Exception: If the BDS API response status is not 200 or a network error occurs.
    """
    try:
        response = requests.get(f"{bds_api_url}/get_previous_epoch_info/{epoch_block_height}")

        if response.status_code != 200:
            epoch_context_logger.error(
                f"❌ Failed to fetch active pools from BDS. Status code: {response.status_code}"
            )
            raise Exception(f"Failed to fetch active pools from BDS. Status code: {response.status_code}")

        epoch_context_logger.info(f"📋 BDS response for active pools: {response.json()}")
        active_pools = list(response.json()['pools'].keys())

    except Exception as e:
        epoch_context_logger.error(f"❌ Exception occurred while fetching active pools from BDS: {e}")
        raise Exception(f"Failed to fetch active pools from BDS: {e}")

    return active_pools


async def prepare_epoch(
    msg_obj: SnapshotProcessMessage,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    protocol_state_contract,
    preloader_results: dict,
) -> EpochContext:
    """
    Gather all epoch-level data needed for computation. Called once per epoch
    regardless of how many slots or pools will be processed.

    This function is slot-agnostic: it fetches block hash, total node count,
    active pools from BDS, and block details -- all shared across any consumer.

    Args:
        msg_obj: Epoch message with begin/end block heights and epoch ID.
        rpc_helper: RPC helper for data source chain.
        anchor_rpc_helper: RPC helper for anchor/protocol chain.
        protocol_state_contract: Web3 contract instance for ProtocolState.
        preloader_results: Pre-computed data (block details, etc.).

    Returns:
        EpochContext with all epoch-level data populated.

    Raises:
        Exception: If block hash cannot be obtained or active pools fetch fails.
    """
    min_chain_height = msg_obj.begin
    max_chain_height = msg_obj.begin  # Single block epoch
    bds_api_url = computes_settings.bds_api_url
    block_details_dict = preloader_results.get('block_details', None)

    # Get total node count from contract (cached 30s)
    total_slots = await SlotSelectionManager.get_total_slots(anchor_rpc_helper, protocol_state_contract)

    # Get epoch end block hash from preloader results
    block_hash = None
    if block_details_dict and max_chain_height in block_details_dict:
        epoch_end_block = block_details_dict.get(max_chain_height, {})
        block_hash = epoch_end_block.get('hash', None)

    if not block_hash:
        epoch_context_logger.warning(
            f"⚠️  Block hash not found in preloader results for block {max_chain_height}, "
            f"falling back to RPC (this indicates preloader issue)"
        )
        try:
            block = await rpc_helper.get_current_node()['web3_client'].eth.get_block(max_chain_height)
            block_hash = block.get('hash', b'').hex() if block else None
        except Exception as e:
            epoch_context_logger.error(f"❌ Failed to fetch block hash for block {max_chain_height}: {e}")
            raise Exception(f"Failed to fetch block hash for block {max_chain_height}: {e}")

    if not block_hash:
        raise Exception(f"Could not obtain block hash for epoch {msg_obj.epochId}")

    # Fetch active pools from BDS API
    # For genesis epoch (epoch 0), query with latest block - 1 since epoch 0 has no historical data
    if msg_obj.epochId == 0:
        try:
            latest_block = await rpc_helper.get_current_node()['web3_client'].eth.get_block('latest')
            query_epoch = latest_block['number'] - 1
            epoch_context_logger.info(f"🎲 Genesis epoch: querying BDS with epoch {query_epoch} (latest - 1)")
            active_pools = await get_epoch_active_pools(query_epoch, bds_api_url)
        except Exception as e:
            epoch_context_logger.error(f"❌ Failed to get latest block for genesis epoch: {e}")
            raise
    else:
        active_pools = await get_epoch_active_pools(min_chain_height, bds_api_url)

    if len(active_pools) == 0:
        raise Exception(f"❌ No active pools found for epoch {msg_obj.epochId} at block {min_chain_height}")

    # CRITICAL: Sort pool addresses for determinism across all nodes
    active_pools_sorted = sorted(active_pools)

    return EpochContext(
        block_hash=block_hash,
        total_slots=total_slots,
        active_pools_sorted=active_pools_sorted,
        block_details_dict=block_details_dict or {},
        min_chain_height=min_chain_height,
        max_chain_height=max_chain_height,
        bds_api_url=bds_api_url,
    )


async def compute_pool_snapshot(
    pool_address: str,
    min_chain_height: int,
    max_chain_height: int,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    protocol_state_contract,
    block_details_dict: dict,
    bds_api_url: str,
    reserves_cache: Optional[ReservesCache] = None,
) -> Optional[Tuple[str, UniswapBaseSnapshot]]:
    """
    Compute base snapshot for a single pool. No slot awareness.
    Called once per unique pool needed in an epoch.

    Fetches previous snapshots from BDS, calls base_snapshot_from_block_range(),
    and attaches previous snapshot data to the result.

    Args:
        pool_address: The Uniswap V3 pool contract address.
        min_chain_height: Start block of the epoch.
        max_chain_height: End block of the epoch.
        rpc_helper: RPC helper for data source chain.
        anchor_rpc_helper: RPC helper for anchor/protocol chain.
        protocol_state_contract: Web3 contract instance for ProtocolState.
        block_details_dict: Pre-fetched block details.
        bds_api_url: Base URL for the BDS API.

    Returns:
        Tuple of (pool_address, UniswapBaseSnapshot) on success, None on failure.
    """
    # Fetch previous snapshots data from BDS
    epoch_context_logger.info(
        f"📡 Fetching previous snapshots data for pool {pool_address} at block {min_chain_height}"
    )
    previous_snapshot_response = requests.get(
        f"{bds_api_url}/previous_snapshots_data/{pool_address}/{min_chain_height}"
    )

    if previous_snapshot_response.status_code != 200:
        epoch_context_logger.error(
            f"❌ Failed to fetch previous snapshots data from BDS: {previous_snapshot_response.status_code}"
        )
        raise Exception(
            f"Failed to fetch previous snapshots data from BDS: {previous_snapshot_response.status_code}"
        )

    previous_snapshot_data = previous_snapshot_response.json()
    previous_snapshot_data = [tuple(data) for data in previous_snapshot_data]

    epoch_context_logger.debug(
        "🔧 [Epoch {}-{}] Processing pool {} | Starting computation",
        min_chain_height, max_chain_height, pool_address,
    )

    # Compute base snapshot
    base_snapshot_data: Optional[UniswapBaseSnapshot] = await base_snapshot_from_block_range(
        pair_address=pool_address,
        from_block=min_chain_height,
        to_block=max_chain_height,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=anchor_rpc_helper,
        protocol_state_contract=protocol_state_contract,
        block_details_dict=block_details_dict,
        reserves_cache=reserves_cache,
    )

    if not base_snapshot_data:
        epoch_context_logger.error(
            "❌ [Epoch {}-{}] Pool {} | No snapshot data returned",
            min_chain_height, max_chain_height, pool_address,
        )
        return None

    epoch_context_logger.debug(
        "✅ [Epoch {}-{}] Pool {} | Computation completed | Wall time: {}",
        min_chain_height, max_chain_height, pool_address, time.time(),
    )

    base_snapshot_data.previousSnapshots = previous_snapshot_data

    return (pool_address, base_snapshot_data)
