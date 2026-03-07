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

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential

from rpc_helper.rpc import RpcHelper
from computes.utils.contracts_factory import ComputesContext
from computes.utils.core import base_snapshot_from_block_range
from computes.utils.models.message_models import UniswapBaseSnapshot
from computes.utils.reserves_cache import ReservesCache
from computes.utils.slot_selection import SlotSelectionManager
from computes.settings.config import settings as computes_settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage

epoch_context_logger = logger.bind(module='EpochContext')

# In-memory cache for pools that revert on RPC (spam/fake). Avoids retrying every epoch.
_revert_pool_memory: set = set()
POOL_REVERT_REDIS_TTL = 86400  # 1 day


def _pool_revert_redis_key(pool_address: str) -> str:
    return f"computes:pool_revert:{pool_address.lower()}"


async def _is_pool_cached_as_reverting(pool_address: str, redis_conn) -> bool:
    """Check in-memory first, then Redis. Returns True if pool should be skipped."""
    pool_lower = pool_address.lower()
    if pool_lower in _revert_pool_memory:
        return True
    if redis_conn:
        try:
            val = await redis_conn.get(_pool_revert_redis_key(pool_address))
            if val is not None:
                _revert_pool_memory.add(pool_lower)
                return True
        except Exception:
            pass
    return False


async def _cache_pool_as_reverting(pool_address: str, redis_conn) -> None:
    """Store in memory and Redis (1-day TTL)."""
    pool_lower = pool_address.lower()
    _revert_pool_memory.add(pool_lower)
    if redis_conn:
        try:
            await redis_conn.set(
                _pool_revert_redis_key(pool_address),
                "1",
                ex=POOL_REVERT_REDIS_TTL,
            )
        except Exception:
            pass


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


@retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=15))
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
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{bds_api_url}/get_previous_epoch_info/{epoch_block_height}"
            )

        if response.status_code != 200:
            epoch_context_logger.error(
                f"❌ Failed to fetch active pools from BDS. Status code: {response.status_code}"
            )
            raise Exception(f"Failed to fetch active pools from BDS. Status code: {response.status_code}")

        data = response.json()
        epoch_context_logger.info(f"📋 BDS response for active pools: {data}")
        active_pools = list(data['pools'].keys())

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
    redis_conn=None,
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

    # Run total_slots and active_pools in parallel (both independent)
    if msg_obj.epochId == 0:
        latest_block = await rpc_helper.get_current_node()['web3_client'].eth.get_block('latest')
        query_epoch = latest_block['number'] - 1
        epoch_context_logger.info(f"🎲 Genesis epoch: querying BDS with epoch {query_epoch} (latest - 1)")
        total_slots, active_pools = await asyncio.gather(
            SlotSelectionManager.get_total_slots(anchor_rpc_helper, protocol_state_contract),
            get_epoch_active_pools(query_epoch, bds_api_url),
        )
    else:
        total_slots, active_pools = await asyncio.gather(
            SlotSelectionManager.get_total_slots(anchor_rpc_helper, protocol_state_contract),
            get_epoch_active_pools(min_chain_height, bds_api_url),
        )

    # Get block hash from preloader results or RPC fallback
    block_hash = None
    if block_details_dict and max_chain_height in block_details_dict:
        epoch_end_block = block_details_dict.get(max_chain_height, {})
        block_hash = epoch_end_block.get('hash', None)
        if block_hash is not None and hasattr(block_hash, 'hex'):
            block_hash = block_hash.hex()

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
    compute_ctx: ComputesContext,
    redis_conn=None,
    reserves_cache: Optional[ReservesCache] = None,
    epoch_id: Optional[int] = None,
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
    blocklist = getattr(computes_settings, 'pool_blocklist', None) or []
    if blocklist and pool_address.lower() in {a.lower() for a in blocklist}:
        epoch_context_logger.warning(
            "⚠️ [Epoch {}-{}] Pool {} | In blocklist, skipping",
            min_chain_height, max_chain_height, pool_address,
        )
        return None

    if await _is_pool_cached_as_reverting(pool_address, redis_conn):
        epoch_context_logger.debug(
            "⏭️ [Epoch {}-{}] Pool {} | Cached as reverting, skipping",
            min_chain_height, max_chain_height, pool_address,
        )
        return None

    # Fetch previous snapshots data from BDS
    epoch_context_logger.info(
        f"📡 Fetching previous snapshots data for pool {pool_address} at block {min_chain_height}"
    )
    async with httpx.AsyncClient() as client:
        previous_snapshot_response = await client.get(
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

    try:
        base_snapshot_data: Optional[UniswapBaseSnapshot] = await base_snapshot_from_block_range(
            pair_address=pool_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            protocol_state_contract=protocol_state_contract,
            block_details_dict=block_details_dict,
            compute_ctx=compute_ctx,
            redis_conn=redis_conn,
            reserves_cache=reserves_cache,
            epoch_id=epoch_id,
        )
    except Exception as e:
        err_str = str(e)
        if 'execution reverted' in err_str or 'RPC_JSONRPC_CALL_ERROR' in err_str:
            await _cache_pool_as_reverting(pool_address, redis_conn)
            epoch_context_logger.warning(
                "⚠️ [Epoch {}-{}] Pool {} | Contract reverts on RPC call (likely spam/fake pool), cached, skipping",
                min_chain_height, max_chain_height, pool_address,
            )
            return None
        raise

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
