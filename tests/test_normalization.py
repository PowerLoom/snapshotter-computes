"""
Unit tests for deterministic decimal normalization.

Verifies that identical raw reserves produce identical JSON output,
enabling CID consistency across lite and bulk nodes.
Uses ROUND_CEILING (round up) for conservative reserve reporting.
"""
import json

import pytest

from computes.utils.normalization import (
    PRICE_DECIMALS,
    USD_DECIMALS,
    normalize_reserve,
    quantize_decimal,
    quantize_float,
)
from computes.utils.models.message_models import EpochBaseSnapshot, UniswapBaseSnapshot


def test_normalize_reserve_8_decimals():
    """Token0 (8 decimals): deterministic normalization."""
    amount = 125061643
    result = normalize_reserve(amount, 8)
    assert str(result) == "1.25061643"


def test_normalize_reserve_18_decimals():
    """Token1 (18 decimals): full precision preserved."""
    amount = 62532078512042075697
    result = normalize_reserve(amount, 18)
    assert str(result) == "62.532078512042075697"


def test_normalize_reserve_rounding():
    """ROUND_CEILING (round up) produces deterministic results."""
    # Exact values stay exact
    result = normalize_reserve(25, 1)
    assert str(result) == "2.5"
    result = normalize_reserve(35, 1)
    assert str(result) == "3.5"


def test_round_ceiling_rounds_up():
    """ROUND_CEILING rounds toward +infinity."""
    from decimal import Decimal

    v = Decimal("2.21")
    q = quantize_decimal(v, 1)
    assert str(q) == "2.3"  # 2.21 rounds up to 2.3 with ROUND_CEILING


def test_quantize_decimal():
    """Quantize to fixed decimals."""
    from decimal import Decimal

    v = Decimal("1.23456789")
    q = quantize_decimal(v, 4)
    assert str(q) == "1.2346"


def test_quantize_float():
    """Float to quantized Decimal."""
    v = quantize_float(123.456789, 4)
    assert str(v) == "123.4568"


def test_constants():
    """Precision constants."""
    assert PRICE_DECIMALS == 8
    assert USD_DECIMALS == 8


def test_deterministic_json_same_input_same_output():
    """
    Identical raw reserves produce identical JSON string.
    Critical for CID consistency across lite and bulk nodes.
    """
    # Same reserves, same block
    block = 21000000
    token0_raw = 125061643  # 8 decimals
    token1_raw = 62532078512042075697  # 18 decimals

    from decimal import Decimal

    token0_norm = str(normalize_reserve(token0_raw, 8))
    token1_norm = str(normalize_reserve(token1_raw, 18))

    snapshot = UniswapBaseSnapshot(
        address="0x1234",
        epoch=EpochBaseSnapshot(begin=20999990, end=21000000),
        timestamps={block: 1700000000},
        token0="0xa",
        token1="0xb",
        token0Reserves={block: Decimal(token0_norm)},
        token1Reserves={block: Decimal(token1_norm)},
        token0ReservesUSD={block: Decimal("125000.12345678")},
        token1ReservesUSD={block: Decimal("125000.12345678")},
        token0Prices={block: Decimal("1.00000000")},
        token1Prices={block: Decimal("1.00000000")},
        token0PricesUSD={block: Decimal("50000.00000000")},
        token1PricesUSD={block: Decimal("50000.00000000")},
    )

    json1 = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    json2 = json.dumps(snapshot.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

    assert json1 == json2
    # Parse and verify structure
    parsed = json.loads(json1)
    assert parsed["token0Reserves"][str(block)] == "1.25061643"
    assert parsed["token1Reserves"][str(block)] == "62.532078512042075697"
