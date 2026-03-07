from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field, field_serializer


class EpochBaseSnapshot(BaseModel):
    """Represents a block range for an epoch."""
    begin: int  # Start of the epoch
    end: int    # End of the epoch


class SnapshotBase(BaseModel):
    """Base class for snapshot models."""
    contract: str                    # Contract address
    chainHeightRange: EpochBaseSnapshot  # Range of blocks for this snapshot
    timestamp: int                   # Timestamp of the snapshot


class UniswapBaseSnapshot(BaseModel):
    """
    Base Snapshot Model for Uniswap Pools/Pairs.
    Uses Decimal for deterministic serialization and CID consistency.
    """
    address: str
    epoch: EpochBaseSnapshot
    timestamps: Dict[int, int]
    token0: str
    token1: str
    token0Reserves: Dict[int, Decimal]
    token1Reserves: Dict[int, Decimal]
    token0ReservesUSD: Dict[int, Decimal]
    token1ReservesUSD: Dict[int, Decimal]
    token0Prices: Dict[int, Decimal]
    token1Prices: Dict[int, Decimal]
    token0PricesUSD: Dict[int, Decimal]
    token1PricesUSD: Dict[int, Decimal]
    totalTrade: Decimal = Decimal('0')
    totalTradeMintBurn: Decimal = Decimal('0')
    totalFee: Decimal = Decimal('0')
    token0MintBurnVolume: Decimal = Decimal('0')
    token1MintBurnVolume: Decimal = Decimal('0')
    token0MintBurnVolumeUSD: Decimal = Decimal('0')
    token1MintBurnVolumeUSD: Decimal = Decimal('0')
    token0TradeVolume: Decimal = Decimal('0')
    token1TradeVolume: Decimal = Decimal('0')
    token0TradeVolumeUSD: Decimal = Decimal('0')
    token1TradeVolumeUSD: Decimal = Decimal('0')
    previousSnapshots: List[Tuple[int, str]] = []

    @field_serializer(
        'token0Reserves', 'token1Reserves',
        'token0ReservesUSD', 'token1ReservesUSD',
        'token0Prices', 'token1Prices',
        'token0PricesUSD', 'token1PricesUSD',
    )
    def serialize_decimal_dict(self, v: Dict[int, Decimal]) -> Dict[int, str]:
        return {k: str(val) for k, val in v.items()}

    @field_serializer(
        'totalTrade', 'totalTradeMintBurn', 'totalFee',
        'token0MintBurnVolume', 'token1MintBurnVolume',
        'token0MintBurnVolumeUSD', 'token1MintBurnVolumeUSD',
        'token0TradeVolume', 'token1TradeVolume',
        'token0TradeVolumeUSD', 'token1TradeVolumeUSD',
    )
    def serialize_decimal(self, v: Decimal) -> str:
        return str(v)


class ActivePoolsSnapshot(BaseModel):
    """
    Snapshot of active pools for a Uniswap pair.
    """
    pools: Dict[str, int]  # Dictionary mapping pool addresses to frequency of occurrence
    epoch: EpochBaseSnapshot  # Range of blocks for this snapshot
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker


class ActiveTokensSnapshot(BaseModel):
    """
    Snapshot of active tokens for a Uniswap pair.
    """
    tokens: Dict[str, int]  # Dictionary mapping token addresses to frequency of occurrence
    epoch: EpochBaseSnapshot  # Range of blocks for this snapshot
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker


class UniswapPairTotalReservesSnapshot(SnapshotBase):
    """
    Snapshot of total reserves for a Uniswap pair.
    """
    token0Reserves: Dict[str, float]     # Block number to corresponding total reserves for token0
    token1Reserves: Dict[str, float]     # Block number to corresponding total reserves for token1
    token0ReservesUSD: Dict[str, float]  # USD value of token0 reserves
    token1ReservesUSD: Dict[str, float]  # USD value of token1 reserves
    token0Prices: Dict[str, float]       # Prices of token0
    token1Prices: Dict[str, float]       # Prices of token1


class UniswapEthPriceSnapshot(BaseModel):
    """
    Snapshot of ETH price for a Uniswap pair.
    """
    epoch: EpochBaseSnapshot  # Range of blocks for this snapshot
    ethPrice: Dict[int, float]  # Block number to corresponding ETH price
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker


class UniswapTokenMetadata(BaseModel):
    """
    Metadata for a Uniswap token.
    """
    address: str  # Contract address of the token
    name: str  # Name of the token
    symbol: str  # Symbol of the token
    decimals: int  # Number of decimals for the token


class UniswapPoolMetadata(BaseModel):
    """
    Metadata for a Uniswap pair.
    """
    address: str  # Contract address of the pair
    token0: UniswapTokenMetadata  # Metadata for token0
    token1: UniswapTokenMetadata  # Metadata for token1
    fee: int  # Fee for the pair
    factory: str  # Factory address for the pair


class UniswapTokenPoolsSnapshot(BaseModel):
    """
    Snapshot of token pools for a Uniswap pair.
    """
    pools: Dict[str, UniswapPoolMetadata]  # Dictionary mapping token addresses to pool metadata


class LiquidityDepthSnapshot(SnapshotBase):
    """Snapshot of liquidity depth for a Uniswap pair."""
    ticks_by_block: Dict[str, dict]


class logsTradeModel(BaseModel):
    """
    Logs and trades in a Uniswap event.
    """
    logs: List  # List of log entries for the event
    trades: Dict[str, float]  # Dictionary mapping trade identifiers to trade amounts


class UniswapTradeEvents(BaseModel):
    """
    A collection of Uniswap trade events.
    """
    Swap: logsTradeModel  # Swap event details
    Mint: logsTradeModel  # Mint (liquidity addition) event details
    Burn: logsTradeModel  # Burn (liquidity removal) event details
    Trades: Dict[str, float]  # Aggregated trades data


class UniswapTopTokenSnapshot(BaseModel):
    """
    Represents a snapshot of a top token on Uniswap.
    """
    name: str  # Token name
    symbol: str  # Token symbol
    decimals: int  # Number of decimal places for the token
    address: str  # Token contract address
    price: float  # Current price of the token
    priceChange24h: float  # 24-hour price change percentage
    volume24h: float  # 24-hour trading volume
    liquidity: float  # Total liquidity for the token


class UniswapTopPair24hSnapshot(BaseModel):
    """
    Represents a snapshot of a top Uniswap pair's performance over the last 24 hours.
    """
    name: str       # Name of the trading pair 
    address: str    # Contract address of the trading pair
    liquidity: float  # Total liquidity in the pair
    volume24h: float  # Trading volume in the last 24 hours
    fee24h: float   # Fees generated in the last 24 hours


class UniswapTopPair7dSnapshot(BaseModel):
    """
    Represents a snapshot of a top Uniswap pair's performance over the last 7 days.
    """
    name: str       # Name of the trading pair
    address: str    # Contract address of the trading pair
    volume7d: float # Trading volume in the last 7 days
    fee7d: float    # Fees generated in the last 7 days


class MonitoredPairsSnapshot(BaseModel):
    """Snapshot of monitored Uniswap pairs."""
    pairs: List[str] = []  # List of monitored pair addresses


class TradeType(str, Enum):
    """
    Defines the different types of Uniswap trade events.

    Enum values:
        SWAP: Regular token exchange events.
        MINT: Liquidity provision events.
        BURN: Liquidity withdrawal events.
    """
    SWAP = "Swap"
    MINT = "Mint"
    BURN = "Burn"


class UniswapTrade(BaseModel):
    """
    Represents a single Uniswap trade event with associated data.

    Captures both the raw log data and the decoded trade information.

    Attributes:
        tradeType (TradeType): The type of trade event (Swap, Mint, or Burn).
        log (Dict[str, Any]): The raw blockchain log data for this trade.
        data (Dict[str, Any]): The decoded trade data with human-readable values.
    """
    tradeType: TradeType = Field(..., description="The type of trade event")
    log: Dict[str, Any]  # Raw log data
    data: Dict[str, Any]  # Decoded trade data


class UniswapTradesSnapshot(BaseModel):
    """
    Uniswap Trades Snapshot Model

    Collects all trade events that occurred within a specific block range for a pool.

    Attributes:
        address (str): The contract address of the Uniswap pair.
        epoch (EpochBaseSnapshot): The block range this snapshot covers.
        trades (List[UniswapTrade]): List of trade events sorted by transaction index.
        previousSnapshots (List[Tuple[int, str]]): References to previous snapshots
            as tuples of (epoch_number, snapshot_cid).
    """
    address: str                 # The contract address
    epoch: EpochBaseSnapshot     # Range of blocks for this snapshot
    trades: List[UniswapTrade]   # Sorted by transaction index
    # Previous Snapshot Links
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker


class AllUniswapTradesSnapshot(BaseModel):
    """
    All Uniswap Trades Snapshot Model
    """
    epoch: EpochBaseSnapshot     # Range of blocks for this snapshot
    tradeData: Dict[str, UniswapTradesSnapshot]  # Dictionary mapping pool addresses to trades
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker