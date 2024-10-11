from typing import Dict
from typing import List

from pydantic import BaseModel

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


class UniswapTradesSnapshot(BaseModel):
    """
    Represents a snapshot of Uniswap trades for a specific epoch and contract.
    """
    epoch: EpochBaseSnapshot  # The epoch (block range) for this snapshot
    contract: str  # The contract address
    totalTrade: float  # Total trade volume in USD
    totalFee: float  # Total fees collected in USD
    token0TradeVolume: float  # Trade volume for token0 in its native decimals
    token1TradeVolume: float  # Trade volume for token1 in its native decimals
    token0TradeVolumeUSD: float  # Trade volume for token0 in USD
    token1TradeVolumeUSD: float  # Trade volume for token1 in USD
    events: UniswapTradeEvents  # Detailed breakdown of trade events


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