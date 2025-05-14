from enum import Enum
from typing import Dict
from typing import List
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from typing import Tuple

from snapshotter.utils.models.message_models import AggregateBase


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
    Base Snapshot Model for Uniswap Pools/Pairs
    
    This model captures comprehensive data about a Uniswap liquidity pool including
    reserves, prices, and trading activity across a specific block range (epoch).
    
    Attributes:
        address (str): The contract address of the Uniswap pair.
        epoch (EpochBaseSnapshot): The block range this snapshot covers.
        token0Reserves (Dict[int, float]): Mapping of block numbers to token0 reserves.
        token1Reserves (Dict[int, float]): Mapping of block numbers to token1 reserves.
        token0ReservesUSD (Dict[int, float]): USD value of token0 reserves by block.
        token1ReservesUSD (Dict[int, float]): USD value of token1 reserves by block.
        token0Prices (Dict[int, float]): Prices of token0 in terms of token1 by block.
        token1Prices (Dict[int, float]): Prices of token1 in terms of token0 by block.
        token0PricesUSD (Dict[int, float]): USD prices of token0 by block.
        token1PricesUSD (Dict[int, float]): USD prices of token1 by block.
        totalTrade (float): Total trading volume in USD for this epoch.
        totalFee (float): Total fees collected in USD for this epoch.
        token0TradeVolume (float): Trading volume for token0 in its native units.
        token1TradeVolume (float): Trading volume for token1 in its native units.
        token0TradeVolumeUSD (float): USD value of token0 trading volume.
        token1TradeVolumeUSD (float): USD value of token1 trading volume.
        previousSnapshots (List[Tuple[int, str]]): References to previous snapshots
            as tuples of (epoch_number, snapshot_cid).
    """
    # Generic data
    address: str                    # Contract address
    epoch: EpochBaseSnapshot        # Range of blocks for this snapshot
    timestamps: Dict[int, int]      # Timestamp of the snapshot
    token0: str
    token1: str
    # Reserve Data
    token0Reserves: Dict[int, float]     # Block number to corresponding total reserves for token0
    token1Reserves: Dict[int, float]     # Block number to corresponding total reserves for token1
    token0ReservesUSD: Dict[int, float]  # USD value of token0 reserves
    token1ReservesUSD: Dict[int, float]  # USD value of token1 reserves
    token0Prices: Dict[int, float]       # Prices of token0 (in terms of token1)
    token1Prices: Dict[int, float]       # Prices of token1 (in terms of token0)
    token0PricesUSD: Dict[int, float]    # Prices of token0 (in USD)
    token1PricesUSD: Dict[int, float]    # Prices of token1 (in USD)
    # Trade Volume Data
    totalTrade: float  # Total trade volume in USD
    totalFee: float    # Total fees collected in USD
    token0TradeVolume: float      # Trade volume for token0 in its native decimals
    token1TradeVolume: float      # Trade volume for token1 in its native decimals
    token0TradeVolumeUSD: float   # Trade volume for token0 in USD
    token1TradeVolumeUSD: float   # Trade volume for token1 in USD
    # Previous Snapshot Links
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


class UniswapTradesAggregateSnapshot(AggregateBase):
    """
    Represents an aggregate snapshot of Uniswap trades across multiple epochs or contracts.
    """
    totalTrade: float = 0  # Total trade volume in USD
    totalFee: float = 0  # Total fees collected in USD
    token0TradeVolume: float = 0  # Cumulative trade volume for token0 in its native decimals
    token1TradeVolume: float = 0  # Cumulative trade volume for token1 in its native decimals
    token0TradeVolumeUSD: float = 0  # Cumulative trade volume for token0 in USD
    token1TradeVolumeUSD: float = 0  # Cumulative trade volume for token1 in USD
    complete: bool = True  # Indicates whether the aggregate snapshot is complete


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


class UniswapTopTokensSnapshot(AggregateBase):
    """Aggregate snapshot of top tokens on Uniswap."""
    tokens: List[UniswapTopTokenSnapshot] = []  # List of top token snapshots
    complete: bool = True  # Indicates if the snapshot is complete


class UniswapTopPair24hSnapshot(BaseModel):
    """
    Represents a snapshot of a top Uniswap pair's performance over the last 24 hours.
    """
    name: str       # Name of the trading pair 
    address: str    # Contract address of the trading pair
    liquidity: float  # Total liquidity in the pair
    volume24h: float  # Trading volume in the last 24 hours
    fee24h: float   # Fees generated in the last 24 hours


class UniswapTopPairs24hSnapshot(AggregateBase):
    """Aggregate snapshot of top Uniswap pairs in the last 24 hours."""
    pairs: List[UniswapTopPair24hSnapshot] = []  # List of top pair snapshots
    complete: bool = True  # Indicates if the snapshot is complete


class UniswapTopPair7dSnapshot(BaseModel):
    """
    Represents a snapshot of a top Uniswap pair's performance over the last 7 days.
    """
    name: str       # Name of the trading pair
    address: str    # Contract address of the trading pair
    volume7d: float # Trading volume in the last 7 days
    fee7d: float    # Fees generated in the last 7 days


class UniswapTopPairs7dSnapshot(AggregateBase):
    """Aggregate snapshot of top Uniswap pairs in the last 7 days."""
    pairs: List[UniswapTopPair7dSnapshot] = []  # List of top pair snapshots
    complete: bool = True  # Indicates if the snapshot is complete


class UniswapStatsSnapshot(AggregateBase):
    """Aggregate snapshot of overall Uniswap statistics."""
    volume24h: float = 0         # 24-hour trading volume
    tvl: float = 0               # Total Value Locked
    fee24h: float = 0            # 24-hour fee collection
    volumeChange24h: float = 0   # 24-hour volume change
    tvlChange24h: float = 0      # 24-hour TVL change
    feeChange24h: float = 0      # 24-hour fee change
    complete: bool = True        # Indicates if the snapshot is complete


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