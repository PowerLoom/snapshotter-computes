# Snapshotter Compute Packages

Compute packages contain the data-market-specific processing logic for the Powerloom snapshotter ecosystem. This repository is loaded dynamically at runtime by both the **lite node** (`snapshotter-lite-v2`) and the **full node** (`snapshotter-core-edge`).

## Branch Strategy

Different branches serve different node types and data markets:

| Branch | Node Type | Data Market | Description |
|--------|-----------|-------------|-------------|
| `feat/bds_lite` | Lite node | BDS Uniswap V3 (ETH) | Base snapshots with deterministic slot selection |
| `bds_eth_uniswapv3_core_unified_cache-experimental` | Full node | BDS Uniswap V3 (ETH) | Full computation with caching and aggregation |
| `eth_uniswapv2_core` | Full node | Uniswap V2 (ETH) | Legacy V2 data market |
| `main` | - | - | Default branch |

The node's environment configuration (`SNAPSHOTTER_COMPUTE_REPO_BRANCH`) determines which branch is loaded.

## Integration with Node Runtimes

### Lite Node (`snapshotter-lite-v2`)

Cloned into `/app/computes` at container startup via `init_docker.sh`:
```bash
git clone --depth 1 --branch $SNAPSHOTTER_COMPUTE_REPO_BRANCH $SNAPSHOTTER_COMPUTE_REPO "/app/computes"
```

### Full Node (`snapshotter-core-edge`)

Cloned into `./computes/` via `bootstrap.sh` and mounted as a Docker volume:
```yaml
volumes:
  - ./computes:/computes
```

### Module Loading

Both node types load compute processors dynamically via `importlib`:
```python
module = importlib.import_module(project_config.processor.module)
class_ = getattr(module, project_config.processor.class_name)
```

The `projects.json` config file specifies which module and class to load.

## Package Structure

```
computes/
├── pair_total_reserves.py       # Lite node processor (thin wrapper, slot-specific)
├── preloaders/
│   └── eth_price/
│       └── preloader.py         # ETH price preloader
├── settings/
│   ├── config.py                # Settings loader
│   ├── settings_model.py        # Pydantic models
│   └── settings.json            # Default settings
├── static/
│   ├── abis/                    # Contract ABIs
│   └── bytecode/                # Helper contract bytecode
├── utils/
│   ├── constants.py             # Shared constants
│   ├── core.py                  # Core computation functions (per-pool snapshot math)
│   ├── epoch_context.py         # Shared epoch context and pool computation (slot-agnostic)
│   ├── helpers.py               # Utility helpers
│   ├── models/                  # Data and message models
│   ├── reserves_cache.py        # Lite reserves cache (incremental replay, see below)
│   └── slot_selection.py        # Deterministic slot selection algorithm
└── tests/                       # Test suite
    └── test_reserves_cache.py   # Cache eviction, RPC usage, hit/miss equivalence
```

## Architecture: Separation of Concerns

The compute package separates slot-agnostic computation from slot-specific orchestration:

**Shared layer (`utils/epoch_context.py`)** -- no slot awareness:
- `EpochContext` dataclass: holds block hash, total slots, active pools, block details
- `get_epoch_active_pools()`: fetch active pool addresses from BDS API
- `prepare_epoch()`: gather all epoch-level data (called once per epoch)
- `compute_pool_snapshot()`: compute base snapshot for a single pool (called once per unique pool)

**Lite node wrapper (`pair_total_reserves.py`)** -- adds slot-specific behavior:
- Reads `settings.slot_id`
- Checks `SlotSelectionManager.get_pool_for_slot()` for this slot
- Reports selection status via `slot_tracker`
- Delegates all computation to `epoch_context` functions

**Future bulk snapshotter service** can import the shared layer directly:
```python
from computes.utils.epoch_context import prepare_epoch, compute_pool_snapshot
from computes.utils.slot_selection import SlotSelectionManager
```
And orchestrate multi-slot computation without touching `pair_total_reserves.py`.

## Processor Interface

The lite node loads `PairTotalReservesProcessor` via `projects.json` config:

```python
async def compute(
    self,
    msg_obj: SnapshotProcessMessage,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    preloader_results: dict,
    slot_tracker=None,
) -> List[Tuple[str, BaseSnapshot]]:
```

**Parameters:**
- `msg_obj`: Epoch metadata (epoch ID, block range, day counter)
- `rpc_helper`: RPC helper for data source chain (e.g., Ethereum mainnet)
- `anchor_rpc_helper`: RPC helper for anchor/protocol chain (where protocol contracts live)
- `ipfs_reader`: IPFS client for reading previous snapshots
- `protocol_state_contract`: Web3 contract instance for ProtocolState
- `preloader_results`: Pre-computed data (e.g., ETH price)
- `slot_tracker`: Optional tracker for reporting slot selection decisions to the node

**Returns:**
- List of `(pool_address, snapshot_data)` tuples
- Empty list if slot was not selected for this epoch

## Deterministic Slot Selection (`utils/slot_selection.py`)

The `SlotSelectionManager` implements epoch-based work distribution for BDS data markets:

1. **Total Slots**: Fetched from `getTotalNodeCount()` on the ProtocolState contract (cached 30s)
2. **Seed**: `SHA256(block_hash + epoch_id)` - deterministic per epoch
3. **Selection**: Fisher-Yates partial shuffle selects 1000 slots from the total pool
4. **Pool Assignment**: `SHA256(seed + slot_id) % num_pools` assigns each selected slot to one pool

All operations are deterministic given the same inputs, enabling any observer to verify assignments.

## Lite Reserves Cache (`utils/reserves_cache.py`)

**Note**: Cache is **disabled by default** (`enabled: false`) for BDS CID determinism. Event replays can introduce divergence between lite and bulk nodes; disabling ensures both always do fresh chain fetch.

When enabled, and a slot is selected every few epochs (block gap 2–9 between consecutive assignments), the **incremental reserves cache** avoids expensive ticks+slot0 RPC calls by reusing cached reserves and replaying event deltas.

### Flow

1. **First work at block N**: fetch initial reserves at block N−1 (ticks+slot0); process events in block N; cache reserves at block N.
2. **Next work at block N+M**: lookup cache for reserves at block N; fetch events N+1..N+M; apply Mint/Burn/Swap deltas in one loop; result = reserves at N+M; cache for next time.

### Log Semantics (N−1 vs N)

For an epoch at block N (single-block epoch):

- **CACHE_MISS at block N−1**: We need initial reserves (state at end of block N−1) before processing events in block N. We fetch via ticks+slot0 at block N−1.
- **CACHE_STORE at block N**: After applying Mint/Burn/Swap deltas from block N to the initial reserves, we store the result — reserves at end of block N (i.e. initial at N−1 + deltas from N).

So CACHE_MISS always references the block we *fetch* (N−1); CACHE_STORE always references the block we *store* (N). The stored value is the final reserves after applying event deltas.

### Config (`settings.json`)

```json
"lite_reserves_cache": {
  "enabled": false,
  "memory_max_entries_per_pool": 20,
  "file_enabled": false,
  "file_path": "./.reserves_cache",
  "pool_eviction_epoch_threshold": 1000,
  "rpc_usage_tracking": false
}
```

- `enabled`: turn cache on/off
- `memory_max_entries_per_pool`: per-pool LRU limit (evict oldest block when exceeded)
- `file_enabled` / `file_path`: optional file persistence for restarts
- `pool_eviction_epoch_threshold`: evict pools not accessed in this many epochs (default 1000)
- `rpc_usage_tracking`: emit `[RPC_USAGE]` JSON logs for eth_call quantification

If `lite_reserves_cache` is absent or disabled, behavior matches pre-cache (no change).

### Decimal Normalization (`utils/normalization.py`)

Reserves and prices use `Decimal` with `ROUND_CEILING` (round up) for deterministic, conservative output. Ensures identical raw reserves produce identical JSON and CIDs across lite and bulk nodes. See `tests/test_normalization.py`.

### Wiring

- `PairTotalReservesProcessor` builds `ReservesCache` from settings and passes it to `compute_pool_snapshot`.
- `core.fetch_initial_reserves` returns `(reserves, cached_block)`; `cached_block` tells the caller to use event range `(cached_block+1)` to `to_block`.
- Trade data and `PairBlockDetail` are only accumulated for epoch blocks `[from_block, to_block]`; gap blocks update running reserves only.

### When CACHE_HIT Occurs

CACHE_HIT only happens when the **same pool** is assigned to the same slot again. Slot selection assigns different pools each epoch; if your slot gets pool A, then pool B, then pool C, you will see CACHE_MISS every time. CACHE_HIT appears when the same pool (e.g. a busy USDC–WETH pool) is assigned to your slot in consecutive or near-consecutive epochs.

### Memory Bounds and Growth

- **Per-pool**: Bounded. Each pool keeps at most `memory_max_entries_per_pool` (default 20) block entries. When exceeded, the smallest block is evicted.
- **Across pools**: Unbounded. Pools are never removed from the cache. With many unique pools over time (e.g. new pools joining the active set), memory can grow. Busy pools that repeat often benefit from the cache; one-off or rare pools add entries without eviction at the pool level.
- **Mitigation**: `pool_eviction_epoch_threshold` evicts idle pools; tune `memory_max_entries_per_pool` down if needed; the cache is optional (`enabled: false` disables it).

### Quantifying RPC Savings

A CACHE_MISS triggers `calculate_reserves`, which does:

- **getTicks**: 1–16 `eth_call`s to the helper contract (depends on fee tier: 0.05%→16, 0.3%→2, 1%→4, etc.)
- **slot0**: 1 `eth_call` per block

Per CACHE_MISS: roughly **2–17 RPC calls**. CACHE_HIT avoids all of these for that pool.

To measure: `grep "\[INCREMENTAL\] CACHE_MISS"` vs `grep "\[INCREMENTAL\] CACHE_HIT"` in logs. Hit rate = CACHE_HIT / (CACHE_HIT + CACHE_MISS).

### RPC Usage Tracking

When `rpc_usage_tracking: true` in `lite_reserves_cache` config, the node emits structured `[RPC_USAGE]` JSON logs for exact eth_call quantification:

| Event | When | Fields |
|-------|------|--------|
| `reserves_cache_miss` | CACHE_MISS path | `pool`, `at_block`, `ticks_eth_calls`, `slot0_eth_calls`, `total_reserves_eth_calls` |
| `reserves_cache_hit` | CACHE_HIT path | `pool`, `cached_block`, `to_block` (0 eth_calls for reserves) |
| `reserves_cache_store` | After storing reserves at block | `pool`, `block` |
| `rpc_usage_summary` | End of epoch | `reserves_cache_misses`, `reserves_cache_hits`, `reserves_ticks_eth_calls`, `reserves_slot0_eth_calls`, `reserves_total_eth_calls` |

- `pool_eviction_epoch_threshold`: Evict pools not hit in this many epochs (default 1000).
- `rpc_usage_tracking`: Enable `[RPC_USAGE]` JSON logs (default false).

**Parse logs**: `grep "\[RPC_USAGE\]" <logfile>` — each line is a JSON object.

### Logging

All cache operations log with `[INCREMENTAL]` prefix (DEBUG/INFO) for production debugging.

### Key Constants
- `SLOTS_PER_EPOCH = 1000`: Number of slots selected per epoch
- `NODE_COUNT_CACHE_TTL = 30`: Seconds to cache the node count

### Selection Probability
With ~8000 total slots: each slot has ~12.2% chance of being selected per epoch (~12s).

## Health Monitoring

When `slot_tracker` is provided (lite node), the compute package reports:
```python
slot_tracker.report_selection(
    epoch_id=msg_obj.epochId,
    was_selected=True/False,
    slot_id=slot_id
)
```

This enables the node to:
- Distinguish "not selected" (normal) from "selected but failed" (problem)
- Track consecutive failures only for epochs where the slot was selected
- Alert operators after 3 consecutive selected-but-failed epochs

## Running Reserves Cache Tests

Tests live in `computes/tests/test_reserves_cache.py` and cover cache eviction, RPC usage tracking, and cache hit/miss equivalence.

**Prerequisites**

1. Create `.env.test` from the project root (snapshotter-core-edge):
   ```bash
   cp env.test.example .env.test
   ```
   Edit `.env.test` if you need specific RPC URLs or Redis; the reserves cache tests use mocks and work with placeholder values.

2. Run from the **snapshotter-core-edge** root (not from `computes/`):
   ```bash
   cd /path/to/snapshotter-core-edge
   python -m pytest computes/tests/test_reserves_cache.py -v
   ```

**Tests included**

| Test | Purpose |
|------|---------|
| `test_reserves_cache_get_set` | Basic get/set and per-pool LRU eviction |
| `test_reserves_cache_prune_stale_pools` | Pools beyond `pool_eviction_epoch_threshold` are evicted |
| `test_reserves_cache_prune_preserves_recent` | Pools within threshold are kept |
| `test_num_tick_segments_for_fee` | Fee tier → tick eth_calls mapping |
| `test_rpc_usage_tracker` | RpcUsageTracker totals |
| `test_cache_hit_miss_same_reserves` | CACHE_MISS vs CACHE_HIT yield identical reserves |
| `test_cache_replay_multiblock` | CACHE_HIT with block gap yields same reserves as CACHE_MISS |
