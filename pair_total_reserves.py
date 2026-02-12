"""
Lite node processor for computing Uniswap V3 base snapshots.

This is a thin wrapper around the shared epoch_context primitives that adds
slot-specific behavior: reading settings.slot_id, checking slot selection via
SlotSelectionManager, and reporting selection status via slot_tracker.

The heavy lifting (epoch context preparation, BDS API calls, per-pool snapshot
computation) is delegated to computes.utils.epoch_context so that other consumers
(e.g. the bulk snapshotter service) can reuse the same primitives without
slot coupling.
"""

import random
from typing import List, Optional, Tuple

from rpc_helper.rpc import RpcHelper
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from computes.utils.models.message_models import UniswapBaseSnapshot
from ipfs_client.main import AsyncIPFSClient
from snapshotter.settings.config import settings
from computes.settings.config import settings as computes_settings
from computes.utils.slot_selection import SlotSelectionManager
from computes.utils.epoch_context import prepare_epoch, compute_pool_snapshot
from computes.utils.reserves_cache import ReservesCache
from computes.utils.rpc_usage import RpcUsageTracker


def _get_reserves_cache() -> Optional[ReservesCache]:
    """Build ReservesCache from computes settings if lite_reserves_cache is configured."""
    cfg = getattr(computes_settings, 'lite_reserves_cache', None)
    if cfg is None:
        return None
    return ReservesCache(
        enabled=cfg.enabled,
        memory_max_entries_per_pool=cfg.memory_max_entries_per_pool,
        file_enabled=cfg.file_enabled,
        file_path=cfg.file_path,
        pool_eviction_epoch_threshold=getattr(cfg, 'pool_eviction_epoch_threshold', 1000),
    )


def _get_rpc_usage_tracker() -> Optional[RpcUsageTracker]:
    """Build RpcUsageTracker if rpc_usage_tracking is enabled."""
    cfg = getattr(computes_settings, 'lite_reserves_cache', None)
    if cfg is None or not getattr(cfg, 'rpc_usage_tracking', False):
        return None
    return RpcUsageTracker(emit_structured_log=True)


class PairTotalReservesProcessor(GenericProcessor):
    """
    Processor for calculating and snapshotting total reserves for Uniswap V3 pairs.

    This processor is designed for single-slot operation in the lite node context.
    It uses the shared epoch_context module for slot-agnostic computation and adds
    slot selection logic on top.
    """

    def __init__(self) -> None:
        self._logger = logger.bind(module="PairTotalReservesProcessor")
        self._reserves_cache: Optional[ReservesCache] = _get_reserves_cache()
        self._rpc_usage_tracker: Optional[RpcUsageTracker] = _get_rpc_usage_tracker()

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        preloader_results: dict,
        slot_tracker=None,
    ) -> List[Tuple[str, UniswapBaseSnapshot]]:
        """
        Compute the total reserves for the Uniswap V3 pool assigned to this slot.

        Delegates epoch preparation and per-pool computation to epoch_context,
        handling only slot selection and health reporting locally.

        Args:
            msg_obj: Epoch message with block range and epoch ID.
            rpc_helper: RPC helper for data source chain (Ethereum mainnet).
            anchor_rpc_helper: RPC helper for anchor/protocol chain.
            ipfs_reader: IPFS client for reading data.
            protocol_state_contract: Contract instance for protocol state queries.
            preloader_results: Pre-computed data (block details, etc.).
            slot_tracker: Optional tracker for reporting slot selection to the lite node.

        Returns:
            List of (pool_address, UniswapBaseSnapshot) tuples. Empty if slot not selected.
        """
        slot_id = settings.slot_id

        # Prepare epoch context (block hash, total slots, active pools, etc.)
        try:
            ctx = await prepare_epoch(
                msg_obj=msg_obj,
                rpc_helper=rpc_helper,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                preloader_results=preloader_results,
            )
        except Exception as e:
            self._logger.error(f"❌ Failed to prepare epoch context: {e}")
            return []

        # Prune pools not hit in pool_eviction_epoch_threshold epochs
        if self._reserves_cache:
            evicted = self._reserves_cache.prune_stale_pools(msg_obj.epochId)
            if evicted > 0:
                self._logger.debug(
                    "[INCREMENTAL] Pruned {} stale pools (epoch {})",
                    evicted, msg_obj.epochId,
                )

        # Genesis epoch (epoch 0): all nodes process one deterministic random pool
        if msg_obj.epochId == 0:
            pool_address = random.Random(slot_id).choice(ctx.active_pools_sorted)

            if slot_tracker:
                slot_tracker.report_selection(
                    epoch_id=msg_obj.epochId,
                    was_selected=True,
                    slot_id=slot_id,
                )

            self._logger.info(
                f"🎲 Genesis epoch (epoch 0) - slot {slot_id} processing pool: {pool_address}"
            )

            if self._rpc_usage_tracker:
                self._rpc_usage_tracker.set_epoch(msg_obj.epochId)
            result = await compute_pool_snapshot(
                pool_address=pool_address,
                min_chain_height=ctx.min_chain_height,
                max_chain_height=ctx.max_chain_height,
                rpc_helper=rpc_helper,
                anchor_rpc_helper=anchor_rpc_helper,
                protocol_state_contract=protocol_state_contract,
                block_details_dict=ctx.block_details_dict,
                bds_api_url=ctx.bds_api_url,
                reserves_cache=self._reserves_cache,
                rpc_usage_tracker=self._rpc_usage_tracker,
                epoch_id=msg_obj.epochId,
            )
            if self._rpc_usage_tracker:
                self._rpc_usage_tracker.emit_summary()
            return [result] if result else []

        # Regular epoch: check slot selection
        self._logger.debug(
            f"🔍 Slot assignment inputs - epoch: {msg_obj.epochId}, block: {ctx.max_chain_height}, "
            f"block_hash: {ctx.block_hash[:10]}..., slot_id: {slot_id}, total_slots: {ctx.total_slots}, "
            f"active_pools: {len(ctx.active_pools_sorted)}, first_3_pools: {ctx.active_pools_sorted[:3]}"
        )

        assigned_pool = SlotSelectionManager.get_pool_for_slot(
            slot_id=slot_id,
            epoch_id=msg_obj.epochId,
            block_hash=ctx.block_hash,
            active_pool_addresses=ctx.active_pools_sorted,
            total_slots=ctx.total_slots,
        )

        # Report selection status to lite node health monitoring
        if slot_tracker:
            slot_tracker.report_selection(
                epoch_id=msg_obj.epochId,
                was_selected=(assigned_pool is not None),
                slot_id=slot_id,
            )

        if assigned_pool is None:
            self._logger.info(
                f"⏭️  Slot {slot_id} NOT selected for epoch {msg_obj.epochId} "
                f"(total_slots={ctx.total_slots}, block_hash: {ctx.block_hash[:10]}...), skipping"
            )
            return []

        self._logger.info(
            f"🎯 Slot {slot_id} SELECTED for epoch {msg_obj.epochId}, "
            f"assigned pool: {assigned_pool} (total_slots={ctx.total_slots}, "
            f"active_pools={len(ctx.active_pools_sorted)}, block_hash: {ctx.block_hash[:10]}...)"
        )

        if self._rpc_usage_tracker:
            self._rpc_usage_tracker.set_epoch(msg_obj.epochId)
        result = await compute_pool_snapshot(
            pool_address=assigned_pool,
            min_chain_height=ctx.min_chain_height,
            max_chain_height=ctx.max_chain_height,
            rpc_helper=rpc_helper,
            anchor_rpc_helper=anchor_rpc_helper,
            protocol_state_contract=protocol_state_contract,
            block_details_dict=ctx.block_details_dict,
            bds_api_url=ctx.bds_api_url,
            reserves_cache=self._reserves_cache,
            rpc_usage_tracker=self._rpc_usage_tracker,
            epoch_id=msg_obj.epochId,
        )
        if self._rpc_usage_tracker:
            self._rpc_usage_tracker.emit_summary()
        return [result] if result else []
