"""
USD price sanity clamps for BDS Uniswap V3 snapshots.

Prevents poisoned USD maps (reference-pool blow-ups, non-finite values) from
propagating into base snapshots, trade volumes, and aggregates.
"""
from __future__ import annotations

import math
from typing import Dict, Optional

from snapshotter.utils.default_logger import logger
from web3 import Web3

from computes.settings.config import settings as worker_settings

_usd_logger = logger.bind(module="PowerLoom|UsdSanity")

# Token USD — above this is treated as corrupt for BDS spot pricing.
MAX_TOKEN_USD = 1_000_000.0
MIN_TOKEN_USD = 1e-18

# Pegged stables: pin to $1 (USDC already handled in helpers; USDT/DAI included here).
STABLECOIN_USD = 1.0

WETH_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.WETH)
USDC_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.USDC)
USDT_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.USDT)
DAI_ADDRESS = Web3.to_checksum_address(worker_settings.contract_addresses.DAI)

STABLECOIN_ADDRESSES = frozenset({USDC_ADDRESS, USDT_ADDRESS, DAI_ADDRESS})

# Max USD(raw_ratio) — catches reference-pool multiplier explosions while keeping
# legitimate high-priced tokens (e.g. WBTC) if raw ratio is sane.
MAX_USD_PER_RAW_UNIT = 1_000_000.0

MAX_USD_RECURSION_DEPTH = 6


def is_stablecoin(token_address: str) -> bool:
    return Web3.to_checksum_address(token_address) in STABLECOIN_ADDRESSES


def clamp_token_usd(
    price: float,
    token_address: str,
    *,
    block_num: Optional[int] = None,
    pool_address: Optional[str] = None,
    raw_ratio: Optional[float] = None,
) -> float:
    """
    Clamp a single token USD price.

    Stablecoins are pinned to $1. Other tokens are bounded and checked against
    implausible USD/raw_ratio when raw_ratio is supplied.
    """
    addr = Web3.to_checksum_address(token_address)
    if is_stablecoin(addr):
        if price is not None and math.isfinite(price) and abs(price - STABLECOIN_USD) > 0.05:
            _usd_logger.debug(
                "Stablecoin {} USD {} at block {} pinned to 1.0 (pool {})",
                addr[:10],
                price,
                block_num,
                pool_address,
            )
        return STABLECOIN_USD

    if price is None or not math.isfinite(price):
        _usd_logger.warning(
            "Non-finite USD for token {} at block {} (pool {}); clamping to 0",
            addr[:10],
            block_num,
            pool_address,
        )
        return 0.0

    p = float(price)
    if raw_ratio is not None and raw_ratio > 0 and math.isfinite(raw_ratio):
        if p / raw_ratio > MAX_USD_PER_RAW_UNIT:
            _usd_logger.warning(
                "Implausible USD/raw for token {} at block {}: usd={} raw={} "
                "(pool {}); clamping USD to {}",
                addr[:10],
                block_num,
                p,
                raw_ratio,
                pool_address,
                MAX_TOKEN_USD,
            )
            p = MAX_TOKEN_USD

    if p > MAX_TOKEN_USD:
        _usd_logger.warning(
            "Token {} USD {} at block {} exceeds max; clamping (pool {})",
            addr[:10],
            p,
            block_num,
            pool_address,
        )
        p = MAX_TOKEN_USD
    elif p < 0:
        p = 0.0
    elif 0 < p < MIN_TOKEN_USD:
        p = MIN_TOKEN_USD
    return p


def clamp_usd_price_maps(
    pair_metadata,
    token0_price_raw: Dict[int, float],
    token1_price_raw: Dict[int, float],
    token0_price_usd: Dict[int, float],
    token1_price_usd: Dict[int, float],
) -> tuple:
    """Apply per-block clamps to both USD maps for a pool."""
    pool = getattr(pair_metadata, "address", None)
    t0 = pair_metadata.token0.address
    t1 = pair_metadata.token1.address
    out0: Dict[int, float] = {}
    out1: Dict[int, float] = {}
    blocks = set(token0_price_usd) | set(token1_price_usd)
    for block_num in blocks:
        r0 = token0_price_raw.get(block_num)
        r1 = token1_price_raw.get(block_num)
        out0[block_num] = clamp_token_usd(
            token0_price_usd.get(block_num, 0.0),
            t0,
            block_num=block_num,
            pool_address=pool,
            raw_ratio=r0,
        )
        out1[block_num] = clamp_token_usd(
            token1_price_usd.get(block_num, 0.0),
            t1,
            block_num=block_num,
            pool_address=pool,
            raw_ratio=r1,
        )
    return out0, out1
