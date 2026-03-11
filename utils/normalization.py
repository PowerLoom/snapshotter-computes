"""
Deterministic decimal normalization for snapshot serialization.

Ensures identical raw reserves produce identical JSON output and CIDs
across lite and bulk nodes by using Decimal with fixed-precision quantize.
"""
from decimal import Decimal, ROUND_CEILING, localcontext

PRICE_DECIMALS = 8
USD_DECIMALS = 8

# Max significant digits for 18-decimal tokens with large amounts (uint256/1e18).
# Default Decimal prec=28 is insufficient; quantize can need ~29+ digits.
_QUANTIZE_PREC = 78


def normalize_reserve(amount: int, decimals: int) -> Decimal:
    """
    Deterministic normalization: amount / 10^decimals, rounded to token decimals.

    Args:
        amount: Raw token amount (integer).
        decimals: Token decimals (e.g. 8 for WBTC, 18 for WETH).

    Returns:
        Decimal quantized to the token's decimal precision.
    """
    d = Decimal(amount) / Decimal(10 ** decimals)
    quantize_exp = Decimal('0.1') ** decimals
    with localcontext() as ctx:
        ctx.prec = _QUANTIZE_PREC
        return d.quantize(quantize_exp, rounding=ROUND_CEILING)


def quantize_decimal(value: Decimal, decimals: int) -> Decimal:
    """
    Quantize a Decimal to a fixed number of decimal places.

    Args:
        value: Decimal value to quantize.
        decimals: Number of decimal places.

    Returns:
        Quantized Decimal.
    """
    quantize_exp = Decimal('0.1') ** decimals
    with localcontext() as ctx:
        ctx.prec = _QUANTIZE_PREC
        return value.quantize(quantize_exp, rounding=ROUND_CEILING)


def quantize_float(value: float, decimals: int) -> Decimal:
    """
    Convert float to Decimal and quantize for deterministic output.

    Args:
        value: Float value (e.g. from price fetcher).
        decimals: Number of decimal places.

    Returns:
        Quantized Decimal.
    """
    return quantize_decimal(Decimal(str(value)), decimals)
