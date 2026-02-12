"""
Structured RPC usage tracking for lite node reserve computation.

Emits machine-parseable records so you can derive exact eth_call counts.
Reserves-related: CACHE_MISS = ticks (1-16 by fee) + slot0 (1) eth_calls; CACHE_HIT = 0.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from snapshotter.utils.default_logger import logger

rpc_usage_logger = logger.bind(module='RpcUsage')


def num_tick_segments_for_fee(fee: int) -> int:
    """Return getTicks eth_call count for a Uniswap V3 fee tier."""
    if fee < 500:
        return 16
    if fee < 3000:
        return 4
    if fee < 10000:
        return 2
    return 1


@dataclass
class RpcUsageTracker:
    """
    Tracks reserves-related RPC usage for quantification.

    Per CACHE_MISS: ticks_eth_calls + slot0_eth_calls (1).
    Per CACHE_HIT: 0 reserves RPC.
    """

    reserves_cache_misses: int = 0
    reserves_cache_hits: int = 0
    reserves_cache_stores: int = 0
    reserves_ticks_eth_calls: int = 0
    reserves_slot0_eth_calls: int = 0
    emit_structured_log: bool = True
    current_epoch_id: Optional[int] = None
    _events: list = field(default_factory=list)

    def set_epoch(self, epoch_id: int) -> None:
        self.current_epoch_id = epoch_id

    def record_reserves_miss(
        self,
        pool: str,
        at_block: int,
        tick_calls: int,
        slot0_calls: int = 1,
    ) -> None:
        self.reserves_cache_misses += 1
        self.reserves_ticks_eth_calls += tick_calls
        self.reserves_slot0_eth_calls += slot0_calls
        ev = {
            "event": "rpc_usage",
            "op": "reserves_cache_miss",
            "epoch_id": self.current_epoch_id,
            "pool": pool[:18],
            "at_block": at_block,
            "ticks_eth_calls": tick_calls,
            "slot0_eth_calls": slot0_calls,
            "total_reserves_eth_calls": tick_calls + slot0_calls,
        }
        self._events.append(ev)
        if self.emit_structured_log:
            rpc_usage_logger.info(
                "[RPC_USAGE] {}",
                json.dumps(ev),
            )

    def record_reserves_hit(
        self,
        pool: str,
        cached_block: int,
        to_block: int,
    ) -> None:
        self.reserves_cache_hits += 1
        ev = {
            "event": "rpc_usage",
            "op": "reserves_cache_hit",
            "epoch_id": self.current_epoch_id,
            "pool": pool[:18],
            "cached_block": cached_block,
            "to_block": to_block,
            "reserves_eth_calls_saved": "variable",
        }
        self._events.append(ev)
        if self.emit_structured_log:
            rpc_usage_logger.info(
                "[RPC_USAGE] {}",
                json.dumps(ev),
            )

    def record_reserves_store(
        self,
        pool: str,
        block: int,
    ) -> None:
        self.reserves_cache_stores += 1
        ev = {
            "event": "rpc_usage",
            "op": "reserves_cache_store",
            "epoch_id": self.current_epoch_id,
            "pool": pool[:18],
            "block": block,
        }
        self._events.append(ev)

    def get_summary(self) -> Dict:
        return {
            "reserves_cache_misses": self.reserves_cache_misses,
            "reserves_cache_hits": self.reserves_cache_hits,
            "reserves_cache_stores": self.reserves_cache_stores,
            "reserves_ticks_eth_calls": self.reserves_ticks_eth_calls,
            "reserves_slot0_eth_calls": self.reserves_slot0_eth_calls,
            "reserves_total_eth_calls": self.reserves_ticks_eth_calls + self.reserves_slot0_eth_calls,
        }

    def emit_summary(self) -> None:
        s = self.get_summary()
        s["event"] = "rpc_usage_summary"
        s["ts"] = time.time()
        s["epoch_id"] = self.current_epoch_id
        if self.emit_structured_log:
            rpc_usage_logger.info(
                "[RPC_USAGE] {}",
                json.dumps(s),
            )

    def reset(self) -> None:
        self.reserves_cache_misses = 0
        self.reserves_cache_hits = 0
        self.reserves_cache_stores = 0
        self.reserves_ticks_eth_calls = 0
        self.reserves_slot0_eth_calls = 0
        self._events.clear()
