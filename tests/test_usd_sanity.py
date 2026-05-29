import math

from web3 import Web3

from computes.utils.usd_sanity import (
    STABLECOIN_USD,
    USDT_ADDRESS,
    clamp_token_usd,
    clamp_usd_price_maps,
    is_stablecoin,
)
from computes.utils.models.message_models import UniswapPoolMetadata


def _meta(t0: str, t1: str) -> UniswapPoolMetadata:
    return UniswapPoolMetadata(
        address="0x4d68B530920D26c3b01C99fecC19e21011B72bBD",
        token0={"address": t0, "decimals": 18, "symbol": "ZAMA", "name": "ZAMA"},
        token1={"address": t1, "decimals": 6, "symbol": "USDT", "name": "Tether"},
        fee=3000,
        factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
    )


def test_stablecoin_pinned():
    assert is_stablecoin(USDT_ADDRESS)
    assert clamp_token_usd(7e41, USDT_ADDRESS, block_num=1) == STABLECOIN_USD


def test_implausible_usd_raw_clamped():
    zama = Web3.to_checksum_address("0xA12CC123ba206d4031D1c7f6223D1C2Ec249f4f3")
    out = clamp_token_usd(7e41, zama, block_num=1, raw_ratio=0.032)
    assert out == 1_000_000.0


def test_clamp_usd_price_maps():
    meta = _meta(
        "0xA12CC123ba206d4031D1c7f6223D1C2Ec249f4f3",
        USDT_ADDRESS,
    )
    b = 25164298
    t0r, t1r = {b: 0.032}, {b: 31.0}
    t0u, t1u = {b: 7e41}, {b: 2e43}
    c0, c1 = clamp_usd_price_maps(meta, t0r, t1r, t0u, t1u)
    assert c1[b] == STABLECOIN_USD
    assert c0[b] == 1_000_000.0
    assert math.isfinite(c0[b])
