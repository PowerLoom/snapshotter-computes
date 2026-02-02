"""
Deterministic slot selection and pool assignment for BDS epochs.

Network Parameters:
- total_slots: Fetched from ProtocolState contract (getTotalNodeCount)
- SLOTS_PER_EPOCH: 1000 (slots selected per epoch)

Algorithm:
1. Fetch total node count from contract (cached 30s)
2. Create seed from epoch ID + epoch end block hash
3. Use Fisher-Yates shuffle with deterministic randomness to select 1000 slots
4. Assign each selected slot to exactly one pool via hash-based modulo

TODO: Currently uses getTotalNodeCount which includes ALL minted nodes (1 to nodeCount).
      Burned/disabled nodes will still be in the slot ID pool, meaning some
      selected slot IDs may have no active node behind them. This is acceptable
      for now - those slots simply won't submit. Future enhancement could use
      the actual list of enabled node IDs for more efficient slot utilization.
"""

import hashlib
import time
from typing import Dict, List, Optional, Set, Tuple

from snapshotter.utils.default_logger import logger

slot_selection_logger = logger.bind(module='SlotSelectionManager')


class SlotSelectionManager:
    """
    Manages deterministic slot selection and pool assignment for BDS epochs.
    
    This class provides methods to:
    - Fetch and cache total node count from the SnapshotterState contract
    - Deterministically select a subset of slots for each epoch
    - Assign each selected slot to a specific pool
    
    All methods are deterministic given the same inputs, ensuring consistent
    behavior across all nodes in the network.
    """
    
    SLOTS_PER_EPOCH = 1000
    NODE_COUNT_CACHE_TTL = 30  # seconds
    
    # Cache: (nodeCount, fetch_timestamp)
    _node_count_cache: Tuple[int, float] = (0, 0.0)
    
    @classmethod
    def get_total_slots(cls, protocol_state_contract) -> int:
        """
        Get total node count from ProtocolState contract with 30s caching.
        
        Args:
            protocol_state_contract: Web3 contract instance for ProtocolState
            
        Returns:
            Total node count (getTotalNodeCount from contract)
        """
        current_time = time.time()
        cached_count, cached_at = cls._node_count_cache
        
        if cached_count > 0 and (current_time - cached_at) < cls.NODE_COUNT_CACHE_TTL:
            return cached_count
        
        # Fetch from contract
        try:
            node_count = protocol_state_contract.functions.getTotalNodeCount().call()
            cls._node_count_cache = (node_count, current_time)
            slot_selection_logger.debug(
                f"Fetched getTotalNodeCount from contract: {node_count}"
            )
            return node_count
        except Exception as e:
            slot_selection_logger.error(f"Failed to fetch getTotalNodeCount from contract: {e}")
            # If we have a cached value, use it even if expired
            if cached_count > 0:
                slot_selection_logger.warning(
                    f"Using expired cached nodeCount: {cached_count}"
                )
                return cached_count
            raise
    
    @staticmethod
    def get_deterministic_seed(epoch_id: int, block_hash: str) -> bytes:
        """
        Generate deterministic seed from epoch and block hash.
        
        Args:
            epoch_id: The epoch ID
            block_hash: Hex string of the epoch's end block hash (with or without 0x prefix)
            
        Returns:
            32-byte seed for deterministic operations
        """
        # Normalize block hash
        if block_hash.startswith('0x'):
            block_hash = block_hash[2:]
        block_hash_bytes = bytes.fromhex(block_hash)
        
        return hashlib.sha256(block_hash_bytes + epoch_id.to_bytes(8, 'big')).digest()
    
    @classmethod
    def get_selected_slots(cls, seed: bytes, total_slots: int) -> Set[int]:
        """
        Get the set of slots selected for this epoch.
        
        Uses Fisher-Yates shuffle with deterministic randomness.
        
        Args:
            seed: Deterministic seed bytes
            total_slots: Total node count from contract
            
        Returns:
            Set of selected slot IDs (up to SLOTS_PER_EPOCH)
        """
        if total_slots <= 0:
            return set()
        
        # Handle case where total_slots < SLOTS_PER_EPOCH
        slots_to_select = min(cls.SLOTS_PER_EPOCH, total_slots)
        
        items = list(range(1, total_slots + 1))
        n = len(items)
        
        for i in range(slots_to_select):
            rand_bytes = hashlib.sha256(seed + i.to_bytes(4, 'big')).digest()
            j = i + (int.from_bytes(rand_bytes[:8], 'big') % (n - i))
            items[i], items[j] = items[j], items[i]
        
        return set(items[:slots_to_select])
    
    @classmethod
    def is_slot_selected(
        cls, 
        slot_id: int, 
        epoch_id: int, 
        block_hash: str, 
        total_slots: int
    ) -> bool:
        """
        Quick check if this slot is among the selected slots for this epoch.
        
        Args:
            slot_id: The slot ID to check (1 to total_slots)
            epoch_id: The epoch ID
            block_hash: Hex string of the epoch's end block hash
            total_slots: Total node count from contract
            
        Returns:
            True if slot is selected for this epoch
        """
        seed = cls.get_deterministic_seed(epoch_id, block_hash)
        selected = cls.get_selected_slots(seed, total_slots)
        return slot_id in selected
    
    @classmethod
    def get_pool_for_slot(
        cls,
        slot_id: int,
        epoch_id: int,
        block_hash: str,
        active_pool_addresses: List[str],
        total_slots: int
    ) -> Optional[str]:
        """
        Get the pool address assigned to a specific slot for this epoch.
        
        Args:
            slot_id: The slot ID (1 to total_slots)
            epoch_id: The epoch ID
            block_hash: Hex string of the epoch's end block hash
            active_pool_addresses: List of active pool addresses (MUST be sorted for determinism)
            total_slots: Total node count from contract
            
        Returns:
            Pool address if slot is selected for this epoch, None otherwise
        """
        if not active_pool_addresses:
            slot_selection_logger.warning(
                f"No active pools provided for epoch {epoch_id}, slot {slot_id}"
            )
            return None
            
        seed = cls.get_deterministic_seed(epoch_id, block_hash)
        selected_slots = cls.get_selected_slots(seed, total_slots)
        
        slot_selection_logger.debug(
            f"Epoch {epoch_id}: Generated seed from block_hash {block_hash[:10]}..., "
            f"selected {len(selected_slots)}/{total_slots} slots for epoch"
        )
        
        if slot_id not in selected_slots:
            return None
        
        # Hash slot_id with seed for uniform distribution across pools
        slot_hash = hashlib.sha256(seed + slot_id.to_bytes(4, 'big')).digest()
        pool_index = int.from_bytes(slot_hash[:8], 'big') % len(active_pool_addresses)
        
        slot_selection_logger.debug(
            f"Slot {slot_id} selected for epoch {epoch_id}, assigned to pool index {pool_index} "
            f"({active_pool_addresses[pool_index]})"
        )
        
        return active_pool_addresses[pool_index]
    
    @classmethod
    def get_epoch_assignments(
        cls,
        epoch_id: int,
        block_hash: str,
        active_pool_addresses: List[str],
        total_slots: int
    ) -> Dict[int, str]:
        """
        Get full mapping of slot_id -> pool_address for this epoch.
        Only returns entries for selected slots.
        
        Args:
            epoch_id: The epoch ID
            block_hash: Hex string of the epoch's end block hash
            active_pool_addresses: List of active pool addresses (MUST be sorted for determinism)
            total_slots: Total node count from contract
            
        Returns:
            Dict mapping slot_id -> pool_address for selected slots
        """
        if not active_pool_addresses:
            return {}
            
        seed = cls.get_deterministic_seed(epoch_id, block_hash)
        selected_slots = cls.get_selected_slots(seed, total_slots)
        
        assignments = {}
        num_pools = len(active_pool_addresses)
        
        for slot_id in selected_slots:
            slot_hash = hashlib.sha256(seed + slot_id.to_bytes(4, 'big')).digest()
            pool_index = int.from_bytes(slot_hash[:8], 'big') % num_pools
            assignments[slot_id] = active_pool_addresses[pool_index]
        
        return assignments
    
    @classmethod
    def clear_cache(cls) -> None:
        """Clear the node count cache. Useful for testing."""
        cls._node_count_cache = (0, 0.0)
