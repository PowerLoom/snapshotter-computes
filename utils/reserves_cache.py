"""
Lite-node reserves cache: (pool, block) -> (token0, token1).
In-memory per-pool LRU eviction + optional file persistence.
Used to skip ticks+slot0 RPC when we have cached reserves at an earlier block
and can apply event deltas N+1 to to_block in one loop.
"""

import json
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from snapshotter.utils.default_logger import logger

reserves_cache_logger = logger.bind(module='ReservesCache')


class ReservesCache:
    """
    Cache of (pool, block) -> (token0_reserves, token1_reserves).
    Per-pool in-memory with eviction; optional file persistence.
    """

    def __init__(
        self,
        enabled: bool = True,
        memory_max_entries_per_pool: int = 20,
        file_enabled: bool = False,
        file_path: str = './.reserves_cache',
    ):
        self.enabled = enabled
        self.memory_max_entries_per_pool = memory_max_entries_per_pool
        self.file_enabled = file_enabled
        self.file_path = Path(file_path)
        # pool -> {block: (token0, token1)}, evict smallest block when over limit
        self._memory: Dict[str, Dict[int, Tuple[int, int]]] = {}

    def get_latest_before(
        self,
        pool_address: str,
        max_block: int,
    ) -> Optional[Tuple[int, int, int]]:
        """
        Return (cached_block, token0, token1) for the largest cached block < max_block
        for this pool, or None if no such entry.
        """
        if not self.enabled:
            return None
        pool = self._normalize_pool(pool_address)
        blocks = self._memory.get(pool)
        if not blocks:
            if self.file_enabled:
                return self._get_latest_from_file(pool, max_block)
            return None
        candidates = [(b, v) for b, v in blocks.items() if b < max_block]
        if not candidates:
            if self.file_enabled:
                return self._get_latest_from_file(pool, max_block)
            return None
        best_block = max(candidates, key=lambda x: x[0])
        cached_block, (t0, t1) = best_block[0], best_block[1]
        return (cached_block, t0, t1)

    def set(
        self,
        pool_address: str,
        block: int,
        token0: int,
        token1: int,
    ) -> None:
        """Store reserves for (pool, block). Evict oldest per-pool if over limit."""
        if not self.enabled:
            return
        pool = self._normalize_pool(pool_address)
        if pool not in self._memory:
            self._memory[pool] = {}
        blocks = self._memory[pool]
        blocks[block] = (token0, token1)
        # Evict smallest block when over limit
        while len(blocks) > self.memory_max_entries_per_pool:
            min_block = min(blocks.keys())
            del blocks[min_block]
        if self.file_enabled:
            self._set_file(pool, block, token0, token1)

    def _normalize_pool(self, pool_address: str) -> str:
        return pool_address.lower().strip()

    def _pool_file_path(self, pool: str) -> Path:
        # Use last 10 chars to avoid filesystem length issues; pool is checksum
        safe = pool[-10:] if len(pool) >= 10 else pool
        return self.file_path / safe

    def _get_latest_from_file(
        self,
        pool: str,
        max_block: int,
    ) -> Optional[Tuple[int, int, int]]:
        path = self._pool_file_path(pool)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            reserves_cache_logger.warning(
                "[INCREMENTAL] ReservesCache file read failed for pool {}: {}",
                pool[:18], e,
            )
            return None
        if not isinstance(data, dict):
            return None
        candidates = [(int(b), (v[0], v[1])) for b, v in data.items() if int(b) < max_block]
        if not candidates:
            return None
        best = max(candidates, key=lambda x: x[0])
        return (best[0], best[1][0], best[1][1])

    def _set_file(
        self,
        pool: str,
        block: int,
        token0: int,
        token1: int,
    ) -> None:
        path = self._pool_file_path(pool)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = {}
            if path.exists():
                try:
                    data = json.loads(path.read_text())
                except Exception:
                    pass
            data[str(block)] = [token0, token1]
            # Keep only recent blocks in file (same limit as memory per pool)
            keys = sorted([int(k) for k in data.keys()])
            while len(keys) > self.memory_max_entries_per_pool:
                del data[str(keys[0])]
                keys = keys[1:]
            path.write_text(json.dumps(data))
        except Exception as e:
            reserves_cache_logger.warning(
                "[INCREMENTAL] ReservesCache file write failed for pool {} block {}: {}",
                pool[:18], block, e,
            )

    def clear(self) -> None:
        """Clear in-memory cache (e.g. for tests)."""
        self._memory.clear()
