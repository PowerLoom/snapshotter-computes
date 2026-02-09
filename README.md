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
├── pair_total_reserves.py       # Main processor: PairTotalReservesProcessor
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
│   ├── core.py                  # Core computation functions
│   ├── helpers.py               # Utility helpers
│   ├── models/                  # Data and message models
│   └── slot_selection.py        # Deterministic slot selection algorithm
└── tests/                       # Test suite
```

## Processor Interface

Compute processors implement a `compute()` method with the following signature:

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
