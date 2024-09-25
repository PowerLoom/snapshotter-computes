from typing import Dict, List
from pydantic import BaseModel
from snapshotter.utils.models.message_models import AggregateBase

class EpochBaseSnapshot(BaseModel):
    """
    Represents a base snapshot for an epoch.
    """
    begin: int  # Start of the epoch
    end: int    # End of the epoch


class SnapshotBase(BaseModel):
    """
    Base class for all snapshots.
    """
    contract: str                    # Contract address
    chainHeightRange: EpochBaseSnapshot  # Range of block heights for this snapshot
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


class logsTradeModel(BaseModel):
    """
    Model for logs and trades.
    """
    logs: List                   # List of logs
    trades: Dict[str, float]     # Dictionary of trades


class UniswapTradeEvents(BaseModel):
    """
    Model for Uniswap trade events.
    """
    Swap: logsTradeModel         # Swap events
    Mint: logsTradeModel         # Mint events
    Burn: logsTradeModel         # Burn events
    Trades: Dict[str, float]     # All trades


class UniswapTradesSnapshot(SnapshotBase):
    """
    Snapshot of Uniswap trades.
    """
    totalTrade: float            # Total trade volume in USD
    totalFee: float              # Total fees in USD
    token0TradeVolume: float     # Trade volume of token0 in native decimals
    token1TradeVolume: float     # Trade volume of token1 in native decimals
    token0TradeVolumeUSD: float  # USD value of token0 trade volume
    token1TradeVolumeUSD: float  # USD value of token1 trade volume
    events: UniswapTradeEvents   # Trade events


class UniswapTradesAggregateSnapshot(AggregateBase):
    """
    Aggregate snapshot of Uniswap trades.
    """
    totalTrade: float = 0            # Total trade volume in USD
    totalFee: float = 0              # Total fees in USD
    token0TradeVolume: float = 0     # Trade volume of token0 in native decimals
    token1TradeVolume: float = 0     # Trade volume of token1 in native decimals
    token0TradeVolumeUSD: float = 0  # USD value of token0 trade volume
    token1TradeVolumeUSD: float = 0  # USD value of token1 trade volume
    complete: bool = True            # Indicates if the snapshot is complete


class UniswapTopTokenSnapshot(BaseModel):
    """
    Snapshot of a top Uniswap token.
    """
    name: str
    symbol: str
    decimals: int
    address: str
    price: float
    priceChange24h: float
    volume24h: float
    liquidity: float


class UniswapTopTokensSnapshot(AggregateBase):
    """
    Aggregate snapshot of top Uniswap tokens.
    """
    tokens: List[UniswapTopTokenSnapshot] = []
    complete: bool = True


class UniswapTopPair24hSnapshot(BaseModel):
    """
    24-hour snapshot of a top Uniswap pair.
    """
    name: str
    address: str
    liquidity: float
    volume24h: float
    fee24h: float


class UniswapTopPairs24hSnapshot(AggregateBase):
    """
    Aggregate 24-hour snapshot of top Uniswap pairs.
    """
    pairs: List[UniswapTopPair24hSnapshot] = []
    complete: bool = True


class UniswapTopPair7dSnapshot(BaseModel):
    """
    7-day snapshot of a top Uniswap pair.
    """
    name: str
    address: str
    volume7d: float
    fee7d: float


class UniswapTopPairs7dSnapshot(AggregateBase):
    """
    Aggregate 7-day snapshot of top Uniswap pairs.
    """
    pairs: List[UniswapTopPair7dSnapshot] = []
    complete: bool = True


class UniswapStatsSnapshot(AggregateBase):
    """
    Snapshot of overall Uniswap statistics.
    """
    volume24h: float = 0         # 24-hour trading volume
    tvl: float = 0               # Total Value Locked
    fee24h: float = 0            # 24-hour fees
    volumeChange24h: float = 0   # 24-hour volume change
    tvlChange24h: float = 0      # 24-hour TVL change
    feeChange24h: float = 0      # 24-hour fee change
    complete: bool = True        # Indicates if the snapshot is complete
