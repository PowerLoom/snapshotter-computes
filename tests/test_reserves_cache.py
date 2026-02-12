"""
Tests for reserves cache: eviction, RPC usage, and cache hit/miss equivalence.

Verifies that CACHE_HIT (replay from cached block) produces identical snapshot
to CACHE_MISS (ticks+slot0 fetch) for the same pool and block range.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from computes.utils.reserves_cache import ReservesCache
from computes.utils.rpc_usage import RpcUsageTracker, num_tick_segments_for_fee


# --- ReservesCache unit tests ---


def test_reserves_cache_get_set():
    """Basic get/set and eviction within pool."""
    cache = ReservesCache(enabled=True, memory_max_entries_per_pool=3)
    cache.set("0xabc", 100, 1000, 2000)
    cache.set("0xabc", 101, 1100, 2100)
    assert cache.get_latest_before("0xabc", 102) == (101, 1100, 2100)
    assert cache.get_latest_before("0xabc", 101) == (100, 1000, 2000)
    assert cache.get_latest_before("0xabc", 100) is None


def test_reserves_cache_prune_stale_pools():
    """Pools not hit in threshold epochs are evicted."""
    cache = ReservesCache(
        enabled=True,
        memory_max_entries_per_pool=5,
        pool_eviction_epoch_threshold=10,
    )
    cache.set("0xaaa", 100, 1, 2, current_epoch=5)
    cache.set("0xbbb", 200, 3, 4, current_epoch=6)
    assert cache.get_latest_before("0xaaa", 101, current_epoch=7) == (100, 1, 2)

    # At epoch 20: 0xaaa last touched at 5 -> 15 ago, 0xbbb at 6 -> 14 ago
    # Both over threshold 10
    evicted = cache.prune_stale_pools(20)
    assert evicted == 2
    assert cache.get_latest_before("0xaaa", 101) is None
    assert cache.get_latest_before("0xbbb", 201) is None


def test_reserves_cache_prune_preserves_recent():
    """Pools hit within threshold are kept."""
    cache = ReservesCache(
        enabled=True,
        pool_eviction_epoch_threshold=100,
    )
    cache.set("0xccc", 50, 10, 20, current_epoch=100)
    evicted = cache.prune_stale_pools(150)  # 50 epochs ago
    assert evicted == 0
    assert cache.get_latest_before("0xccc", 51) == (50, 10, 20)


# --- RPC usage ---


def test_num_tick_segments_for_fee():
    assert num_tick_segments_for_fee(100) == 16
    assert num_tick_segments_for_fee(500) == 4
    assert num_tick_segments_for_fee(3000) == 2
    assert num_tick_segments_for_fee(10000) == 1


def test_rpc_usage_tracker():
    t = RpcUsageTracker(emit_structured_log=False)
    t.record_reserves_miss("0xabc", 100, tick_calls=4, slot0_calls=1)
    t.record_reserves_hit("0xdef", 99, 100)
    t.record_reserves_store("0xabc", 100)
    s = t.get_summary()
    assert s["reserves_cache_misses"] == 1
    assert s["reserves_cache_hits"] == 1
    assert s["reserves_ticks_eth_calls"] == 4
    assert s["reserves_slot0_eth_calls"] == 1


# --- Cache hit/miss equivalence (integration-style with mocks) ---


@pytest.mark.asyncio
async def test_cache_hit_miss_same_reserves():
    """
    CACHE_MISS path and CACHE_HIT path produce identical reserves at to_block.
    Uses mocked calculate_reserves and get_events_by_block.
    """
    from computes.utils.core import generate_pair_reserves_dict_and_trade_data
    from computes.utils.models.message_models import UniswapPoolMetadata, UniswapTokenMetadata

    pool = "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"  # USDC-WETH 0.3%
    from_block = 100
    to_block = 100
    # Initial reserves at block 99 (before epoch block 100)
    init_t0, init_t1 = 1_000_000_000_000, 2_000_000_000_000

    metadata = UniswapPoolMetadata(
        address=pool,
        token0=UniswapTokenMetadata(address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", name="USDC", symbol="USDC", decimals=6),
        token1=UniswapTokenMetadata(address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", name="WETH", symbol="WETH", decimals=18),
        fee=3000,
        factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
    )
    mock_rpc = MagicMock()
    token0_price = {from_block: 1.0}
    token1_price = {from_block: 2000.0}
    token0_raw = {from_block: 0.0005}
    token1_raw = {from_block: 2000.0}
    block_details = {from_block: {"timestamp": 1234567890}}

    # No events in block 100 -> reserves stay at init
    empty_events = {from_block: []}

    async def mock_calculate_reserves(pa, ab, meta, rpc):
        return (init_t0, init_t1)

    async def mock_get_events(pair_address, rpc, from_block, to_block):
        return empty_events

    with (
        patch("computes.utils.core.calculate_reserves", side_effect=mock_calculate_reserves),
        patch("computes.utils.core.get_events_by_block", side_effect=mock_get_events),
    ):
        # Run 1: no cache (CACHE_MISS)
        cache_empty = ReservesCache(enabled=True)
        dict_miss, trade_miss = await generate_pair_reserves_dict_and_trade_data(
            pair_address=pool,
            from_block=from_block,
            to_block=to_block,
            rpc_helper=mock_rpc,
            pair_per_token_metadata=metadata,
            token0_price_map=token0_price,
            token1_price_map=token1_price,
            token0_price_raw=token0_raw,
            token1_price_raw=token1_raw,
            block_details_dict=block_details,
            reserves_cache=cache_empty,
        )

        # Run 2: cache pre-populated at block 99 (CACHE_HIT for epoch at 100)
        cache_warm = ReservesCache(enabled=True)
        cache_warm.set(pool, from_block - 1, init_t0, init_t1)
        dict_hit, trade_hit = await generate_pair_reserves_dict_and_trade_data(
            pair_address=pool,
            from_block=from_block,
            to_block=to_block,
            rpc_helper=mock_rpc,
            pair_per_token_metadata=metadata,
            token0_price_map=token0_price,
            token1_price_map=token1_price,
            token0_price_raw=token0_raw,
            token1_price_raw=token1_raw,
            block_details_dict=block_details,
            reserves_cache=cache_warm,
        )

    # Same reserves at epoch block
    d_miss = dict_miss.get(to_block)
    d_hit = dict_hit.get(to_block)
    assert d_miss is not None and d_hit is not None
    assert d_miss.token0Reserves == d_hit.token0Reserves
    assert d_miss.token1Reserves == d_hit.token1Reserves
    assert d_miss.token0ReservesNormalized == d_hit.token0ReservesNormalized
    assert d_miss.token1ReservesNormalized == d_hit.token1ReservesNormalized
    # Trade data match
    assert trade_miss.totalTradesUSD == trade_hit.totalTradesUSD


@pytest.mark.asyncio
async def test_cache_replay_multiblock():
    """
    CACHE_HIT with gap: cached at block N, epoch at N+5. Replay events N+1..N+5.
    """
    from computes.utils.core import generate_pair_reserves_dict_and_trade_data
    from computes.utils.models.message_models import UniswapPoolMetadata, UniswapTokenMetadata

    pool = "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"
    from_block = 105
    to_block = 105
    cached_block = 99
    init_t0, init_t1 = 100, 200

    metadata = UniswapPoolMetadata(
        address=pool,
        token0=UniswapTokenMetadata(address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", name="USDC", symbol="USDC", decimals=6),
        token1=UniswapTokenMetadata(address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", name="WETH", symbol="WETH", decimals=18),
        fee=3000,
        factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
    )
    mock_rpc = MagicMock()
    blocks_in_range = list(range(from_block, to_block + 1))
    token0_price = {b: 1.0 for b in blocks_in_range}
    token1_price = {b: 2000.0 for b in blocks_in_range}
    token0_raw = {b: 0.0005 for b in blocks_in_range}
    token1_raw = {b: 2000.0 for b in blocks_in_range}
    block_details = {b: {"timestamp": 1234567890 + b} for b in blocks_in_range}

    # Empty events 100..105 - reserves stay at init
    empty_events = {b: [] for b in range(100, 106)}

    async def mock_calculate_reserves(pa, ab, meta, rpc):
        return (init_t0, init_t1)

    async def mock_get_events(pair_address, rpc, from_block, to_block):
        return {b: [] for b in range(from_block, to_block + 1)}

    with (
        patch("computes.utils.core.calculate_reserves", side_effect=mock_calculate_reserves),
        patch("computes.utils.core.get_events_by_block", side_effect=mock_get_events),
    ):
        # CACHE_HIT: we have reserves at 99, replay 100..105
        cache = ReservesCache(enabled=True)
        cache.set(pool, cached_block, init_t0, init_t1)
        dict_hit, _ = await generate_pair_reserves_dict_and_trade_data(
            pair_address=pool,
            from_block=from_block,
            to_block=to_block,
            rpc_helper=mock_rpc,
            pair_per_token_metadata=metadata,
            token0_price_map=token0_price,
            token1_price_map=token1_price,
            token0_price_raw=token0_raw,
            token1_price_raw=token1_raw,
            block_details_dict=block_details,
            reserves_cache=cache,
        )
        # CACHE_MISS: fetch at 104
        cache_miss = ReservesCache(enabled=True)
        dict_miss, _ = await generate_pair_reserves_dict_and_trade_data(
            pair_address=pool,
            from_block=from_block,
            to_block=to_block,
            rpc_helper=mock_rpc,
            pair_per_token_metadata=metadata,
            token0_price_map=token0_price,
            token1_price_map=token1_price,
            token0_price_raw=token0_raw,
            token1_price_raw=token1_raw,
            block_details_dict=block_details,
            reserves_cache=cache_miss,
        )

    assert dict_hit[to_block].token0Reserves == dict_miss[to_block].token0Reserves
    assert dict_hit[to_block].token1Reserves == dict_miss[to_block].token1Reserves
