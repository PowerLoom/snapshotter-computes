"""
Unit tests for SlotSelectionManager with dynamic total_slots.
"""

import pytest
from unittest.mock import MagicMock, patch
from computes.utils.slot_selection import SlotSelectionManager


class TestSlotSelectionManager:
    """
    Unit tests for SlotSelectionManager with dynamic total_slots.
    """
    
    # Standard test values
    TOTAL_SLOTS = 10000  # Simulated nodeCount from contract
    
    def setup_method(self):
        """Clear cache before each test"""
        SlotSelectionManager.clear_cache()
    
    def test_deterministic_seed_consistency(self):
        """Same inputs produce same seed"""
        seed1 = SlotSelectionManager.get_deterministic_seed(100, "0xabc123def456")
        seed2 = SlotSelectionManager.get_deterministic_seed(100, "0xabc123def456")
        assert seed1 == seed2
    
    def test_deterministic_seed_with_and_without_0x_prefix(self):
        """Block hash with and without 0x prefix produces same seed"""
        seed1 = SlotSelectionManager.get_deterministic_seed(100, "0xabc123def456")
        seed2 = SlotSelectionManager.get_deterministic_seed(100, "abc123def456")
        assert seed1 == seed2
    
    def test_deterministic_seed_different_epochs(self):
        """Different epochs produce different seeds"""
        seed1 = SlotSelectionManager.get_deterministic_seed(100, "0xabc123def456")
        seed2 = SlotSelectionManager.get_deterministic_seed(101, "0xabc123def456")
        assert seed1 != seed2
    
    def test_deterministic_seed_different_block_hashes(self):
        """Different block hashes produce different seeds"""
        seed1 = SlotSelectionManager.get_deterministic_seed(100, "0xabc123def456")
        seed2 = SlotSelectionManager.get_deterministic_seed(100, "0xdef456abc123")
        assert seed1 != seed2
    
    def test_slot_selection_count(self):
        """Exactly 1000 slots selected from 10000"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "ab" * 32)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        assert len(selected) == 1000
    
    def test_slot_selection_count_small_pool(self):
        """When total_slots < SLOTS_PER_EPOCH, select all available"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "ab" * 32)
        total_slots = 500  # Less than 1000
        selected = SlotSelectionManager.get_selected_slots(seed, total_slots)
        assert len(selected) == 500
        # All slots should be selected
        assert selected == set(range(1, 501))
    
    def test_slot_selection_count_exact_slots_per_epoch(self):
        """When total_slots == SLOTS_PER_EPOCH, select all"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "ab" * 32)
        total_slots = 1000
        selected = SlotSelectionManager.get_selected_slots(seed, total_slots)
        assert len(selected) == 1000
        assert selected == set(range(1, 1001))
    
    def test_slot_selection_empty_for_zero_slots(self):
        """Returns empty set when total_slots is 0"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "ab" * 32)
        selected = SlotSelectionManager.get_selected_slots(seed, 0)
        assert len(selected) == 0
    
    def test_slot_selection_determinism(self):
        """Same epoch+block_hash+total_slots always selects same slots"""
        epoch_id = 12345
        block_hash = "0x" + "cd" * 32
        
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        selected1 = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        selected2 = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        assert selected1 == selected2
    
    def test_different_total_slots_different_selection(self):
        """Different total_slots values produce different selections"""
        epoch_id = 100
        block_hash = "0x" + "ab" * 32
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        
        selected_10000 = SlotSelectionManager.get_selected_slots(seed, 10000)
        selected_8000 = SlotSelectionManager.get_selected_slots(seed, 8000)
        
        # Different pool sizes = different selection
        assert selected_10000 != selected_8000
    
    def test_pool_assignment_coverage(self):
        """All selected slots get a pool assignment"""
        epoch_id = 100
        block_hash = "0x" + "ef" * 32
        pools = ["0xPool1", "0xPool2", "0xPool3"]
        
        assignments = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        assert len(assignments) == 1000
    
    def test_pool_distribution(self):
        """Pools are assigned roughly evenly"""
        epoch_id = 100
        block_hash = "0x" + "11" * 32
        pools = ["0xPoolA", "0xPoolB", "0xPoolC", "0xPoolD"]
        
        assignments = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        
        pool_counts = {}
        for pool in assignments.values():
            pool_counts[pool] = pool_counts.get(pool, 0) + 1
        
        # Each pool should get roughly 250 assignments (1000/4)
        # Allow 20% variance
        for count in pool_counts.values():
            assert 200 <= count <= 300, f"Pool got {count} assignments, expected ~250"
    
    def test_edge_case_single_pool(self):
        """All slots get same pool when only 1 pool active"""
        epoch_id = 100
        block_hash = "0x" + "22" * 32
        pools = ["0xOnlyPool"]
        
        assignments = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        
        assert all(pool == "0xOnlyPool" for pool in assignments.values())
    
    def test_edge_case_no_pools(self):
        """Returns empty dict when no pools"""
        epoch_id = 100
        block_hash = "0x" + "33" * 32
        pools = []
        
        assignments = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        assert assignments == {}
    
    def test_get_pool_for_unselected_slot(self):
        """Returns None for unselected slot"""
        epoch_id = 100
        block_hash = "0x" + "44" * 32
        pools = ["0xPool1"]
        
        # Get selected slots
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        
        # Find an unselected slot
        unselected_slot = None
        for slot in range(1, self.TOTAL_SLOTS + 1):
            if slot not in selected:
                unselected_slot = slot
                break
        
        result = SlotSelectionManager.get_pool_for_slot(
            unselected_slot, epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        assert result is None
    
    def test_get_pool_for_selected_slot(self):
        """Returns pool address for selected slot"""
        epoch_id = 100
        block_hash = "0x" + "44" * 32
        pools = ["0xPool1", "0xPool2"]
        
        # Get selected slots
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        
        # Pick a selected slot
        selected_slot = next(iter(selected))
        
        result = SlotSelectionManager.get_pool_for_slot(
            selected_slot, epoch_id, block_hash, pools, self.TOTAL_SLOTS
        )
        assert result in pools
    
    def test_is_slot_selected_returns_true_for_selected(self):
        """is_slot_selected returns True for selected slot"""
        epoch_id = 100
        block_hash = "0x" + "55" * 32
        
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        selected_slot = next(iter(selected))
        
        result = SlotSelectionManager.is_slot_selected(
            selected_slot, epoch_id, block_hash, self.TOTAL_SLOTS
        )
        assert result is True
    
    def test_is_slot_selected_returns_false_for_unselected(self):
        """is_slot_selected returns False for unselected slot"""
        epoch_id = 100
        block_hash = "0x" + "55" * 32
        
        seed = SlotSelectionManager.get_deterministic_seed(epoch_id, block_hash)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        
        # Find an unselected slot
        unselected_slot = None
        for slot in range(1, self.TOTAL_SLOTS + 1):
            if slot not in selected:
                unselected_slot = slot
                break
        
        result = SlotSelectionManager.is_slot_selected(
            unselected_slot, epoch_id, block_hash, self.TOTAL_SLOTS
        )
        assert result is False
    
    def test_pool_list_must_be_sorted(self):
        """Different sort orders produce different assignments (verification test)"""
        epoch_id = 100
        block_hash = "0x" + "55" * 32
        
        # Same pools, different order
        pools_sorted = ["0xPoolA", "0xPoolB", "0xPoolC"]
        pools_unsorted = ["0xPoolC", "0xPoolA", "0xPoolB"]
        
        assignments_sorted = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools_sorted, self.TOTAL_SLOTS
        )
        assignments_unsorted = SlotSelectionManager.get_epoch_assignments(
            epoch_id, block_hash, pools_unsorted, self.TOTAL_SLOTS
        )
        
        # Assignments will differ because pool order affects modulo result
        # This test confirms the importance of sorting
        assert assignments_sorted != assignments_unsorted
    
    def test_slot_within_bounds(self):
        """All selected slots are within 1 to total_slots range"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "66" * 32)
        
        for total in [1000, 5000, 10000]:
            selected = SlotSelectionManager.get_selected_slots(seed, total)
            assert all(1 <= slot <= total for slot in selected)
    
    def test_get_total_slots_caching(self):
        """get_total_slots caches the result"""
        mock_contract = MagicMock()
        mock_contract.functions.nodeCount.return_value.call.return_value = 5000
        
        # First call should fetch from contract
        result1 = SlotSelectionManager.get_total_slots(mock_contract)
        assert result1 == 5000
        assert mock_contract.functions.nodeCount.return_value.call.call_count == 1
        
        # Second call should use cache
        result2 = SlotSelectionManager.get_total_slots(mock_contract)
        assert result2 == 5000
        assert mock_contract.functions.nodeCount.return_value.call.call_count == 1  # Still 1
    
    def test_get_total_slots_cache_expires(self):
        """get_total_slots cache expires after TTL"""
        mock_contract = MagicMock()
        mock_contract.functions.nodeCount.return_value.call.return_value = 5000
        
        # First call
        result1 = SlotSelectionManager.get_total_slots(mock_contract)
        assert result1 == 5000
        
        # Manually expire the cache
        SlotSelectionManager._node_count_cache = (5000, 0)  # Set timestamp to 0
        
        # Contract now returns different value
        mock_contract.functions.nodeCount.return_value.call.return_value = 6000
        
        # Second call should fetch fresh value
        result2 = SlotSelectionManager.get_total_slots(mock_contract)
        assert result2 == 6000
    
    def test_consistent_pool_assignment_across_epochs(self):
        """Same slot gets consistent pool assignment given same parameters"""
        slot_id = 42
        block_hash = "0x" + "77" * 32
        pools = sorted(["0xPoolA", "0xPoolB", "0xPoolC"])
        
        # Check that the same slot always gets the same pool for the same epoch
        pool1 = SlotSelectionManager.get_pool_for_slot(
            slot_id, 100, block_hash, pools, self.TOTAL_SLOTS
        )
        pool2 = SlotSelectionManager.get_pool_for_slot(
            slot_id, 100, block_hash, pools, self.TOTAL_SLOTS
        )
        
        # Same epoch = same assignment (if selected)
        assert pool1 == pool2
    
    def test_slot_selection_uniqueness(self):
        """No duplicate slots in selection"""
        seed = SlotSelectionManager.get_deterministic_seed(100, "0x" + "88" * 32)
        selected = SlotSelectionManager.get_selected_slots(seed, self.TOTAL_SLOTS)
        
        # Convert to list to check for duplicates
        selected_list = list(selected)
        assert len(selected_list) == len(set(selected_list))
