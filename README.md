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

When a slot is selected every few epochs (block gap 2–9 between consecutive assignments), the **incremental reserves cache** avoids expensive ticks+slot0 RPC calls by reusing cached reserves and replaying event deltas.

### Flow

1. **First work at block N**: compute base snapshot via ticks+slot0; process events in block N; cache `(token0, token1)` at block N.
2. **Next work at block N+M**: lookup cache for reserves at block N; fetch events N+1..N+M; apply Mint/Burn/Swap deltas in one loop; result = reserves at N+M; cache for next time.

### Config (`settings.json`)

```json
"lite_reserves_cache": {
  "enabled": true,
  "memory_max_entries_per_pool": 20,
  "file_enabled": false,
  "file_path": "./.reserves_cache"
}
```

- `enabled`: turn cache on/off
- `memory_max_entries_per_pool`: per-pool LRU limit (evict oldest block when exceeded)
- `file_enabled` / `file_path`: optional file persistence for restarts

If `lite_reserves_cache` is absent or disabled, behavior matches pre-cache (no change).

### Wiring

- `PairTotalReservesProcessor` builds `ReservesCache` from settings and passes it to `compute_pool_snapshot`.
- `core.fetch_initial_reserves` returns `(reserves, cached_block)`; `cached_block` tells the caller to use event range `(cached_block+1)` to `to_block`.
- Trade data and `PairBlockDetail` are only accumulated for epoch blocks `[from_block, to_block]`; gap blocks update running reserves only.

### Logging

All cache operations log with `[INCREMENTAL]` prefix (DEBUG/INFO) for production debugging.

For full implementation details, event-delta rules, and validation checklist, see `ai-coord-docs/dsv_mainnet_launch/deterministic_slot_selection/LITE_NODE_INCREMENTAL_RESERVES.md` (if available in the workspace).

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
