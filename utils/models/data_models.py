from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_serializer


class UniswapEvent(BaseModel):
    eventName: str
    filterName: str
    txHash: str
    blockNumber: int
    txIndex: int
    logIndex: int
    address: str
    topics: List[str]
    data: str
    args: Dict[str, Any]
    score: Optional[int] = Field(None, alias='_score')


class UniswapProcessedLog(UniswapEvent):
    """
    Represents a processed log from a Uniswap event, along with calculated trade data.
    Inherits all fields from UniswapEvent.
    """
    token0_amount: float
    token1_amount: float
    trade_amount_usd: float
    timestamp: Optional[int] = None


class TradeData(BaseModel):
    """
    Represents trading data for a pair of tokens.
    Uses Decimal for deterministic serialization.
    """
    totalTradesUSD: Decimal = Decimal('0')
    totalTradesMintBurnUSD: Decimal = Decimal('0')
    totalFeeUSD: Decimal = Decimal('0')
    token0MintBurnVolume: Decimal = Decimal('0')
    token1MintBurnVolume: Decimal = Decimal('0')
    token0MintBurnVolumeUSD: Decimal = Decimal('0')
    token1MintBurnVolumeUSD: Decimal = Decimal('0')
    token0TradeVolume: Decimal = Decimal('0')
    token1TradeVolume: Decimal = Decimal('0')
    token0TradeVolumeUSD: Decimal = Decimal('0')
    token1TradeVolumeUSD: Decimal = Decimal('0')

    @field_serializer(
        'totalTradesUSD', 'totalTradesMintBurnUSD', 'totalFeeUSD',
        'token0MintBurnVolume', 'token1MintBurnVolume',
        'token0MintBurnVolumeUSD', 'token1MintBurnVolumeUSD',
        'token0TradeVolume', 'token1TradeVolume',
        'token0TradeVolumeUSD', 'token1TradeVolumeUSD',
    )
    def serialize_decimal(self, v: Decimal) -> str:
        return str(v)

    def __add__(self, other: "TradeData") -> "TradeData":
        """
        Add trading data from another TradeData object.
        Args:
            other (TradeData): Another TradeData object to add.
        Returns:
            TradeData: The updated TradeData object.
        """
        self.totalTradesUSD += other.totalTradesUSD
        self.totalFeeUSD += other.totalFeeUSD
        self.totalTradesMintBurnUSD += other.totalTradesMintBurnUSD
        self.token0TradeVolume += other.token0TradeVolume
        self.token1TradeVolume += other.token1TradeVolume
        self.token0TradeVolumeUSD += other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD += other.token1TradeVolumeUSD
        self.token0MintBurnVolume += other.token0MintBurnVolume
        self.token1MintBurnVolume += other.token1MintBurnVolume
        self.token0MintBurnVolumeUSD += other.token0MintBurnVolumeUSD
        self.token1MintBurnVolumeUSD += other.token1MintBurnVolumeUSD
        return self

    def __sub__(self, other: "TradeData") -> "TradeData":
        """
        Subtract trading data from another TradeData object.
        Args:
            other (TradeData): Another TradeData object to subtract.
        Returns:
            TradeData: The updated TradeData object.
        """
        self.totalTradesUSD -= other.totalTradesUSD
        self.totalFeeUSD -= other.totalFeeUSD
        self.totalTradesMintBurnUSD -= other.totalTradesMintBurnUSD
        self.token0TradeVolume -= other.token0TradeVolume
        self.token1TradeVolume -= other.token1TradeVolume
        self.token0TradeVolumeUSD -= other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD -= other.token1TradeVolumeUSD
        self.token0MintBurnVolume -= other.token0MintBurnVolume
        self.token1MintBurnVolume -= other.token1MintBurnVolume
        self.token0MintBurnVolumeUSD -= other.token0MintBurnVolumeUSD
        self.token1MintBurnVolumeUSD -= other.token1MintBurnVolumeUSD
        return self

    def __abs__(self) -> "TradeData":
        """
        Calculate the absolute values of all trading data.
        Returns:
            TradeData: A new TradeData object with absolute values.
        """
        self.totalTradesUSD = abs(self.totalTradesUSD)
        self.totalFeeUSD = abs(self.totalFeeUSD)
        self.totalTradesMintBurnUSD = abs(self.totalTradesMintBurnUSD)
        self.token0TradeVolume = abs(self.token0TradeVolume)
        self.token1TradeVolume = abs(self.token1TradeVolume)
        self.token0TradeVolumeUSD = abs(self.token0TradeVolumeUSD)
        self.token1TradeVolumeUSD = abs(self.token1TradeVolumeUSD)
        self.token0MintBurnVolume = abs(self.token0MintBurnVolume)
        self.token1MintBurnVolume = abs(self.token1MintBurnVolume)
        self.token0MintBurnVolumeUSD = abs(self.token0MintBurnVolumeUSD)
        self.token1MintBurnVolumeUSD = abs(self.token1MintBurnVolumeUSD)
        return self


class EventTradeData(BaseModel):
    """
    Represents trade data for a specific event.
    Attributes:
        logs (List[Dict]): List of log dictionaries associated with the event.
        trades (TradeData): Trading data for the event.
    """
    logs: List[Dict]
    trades: TradeData


class EpochEventTradeData(BaseModel):
    """
    Represents trade data for different types of events within an epoch.
    Attributes:
        Swap (EventTradeData): Trade data for Swap events.
        Mint (EventTradeData): Trade data for Mint events.
        Burn (EventTradeData): Trade data for Burn events.
        Trades (TradeData): Aggregated trade data for all events.
    """
    Swap: EventTradeData
    Mint: EventTradeData
    Burn: EventTradeData
    Trades: TradeData


# --- New Models for Tick and Slot0 Data ---

class TickData(BaseModel):
    """
    Represents the processed data for a single tick from the getTicks helper call.
    Corresponds to the output of transform_tick_bytes_to_list.
    """
    liquidity_net: int  # Decoded from int128
    idx: int            # Decoded from int24


class Slot0Data(BaseModel):
    """
    Represents the data returned by the slot0 function of a UniswapV3Pool contract.
    """
    sqrtPriceX96: int          # uint160
    tick: int                  # int24
    observationIndex: int      # uint16
    observationCardinality: int # uint16
    observationCardinalityNext: int # uint16
    feeProtocol: int           # uint8
    unlocked: bool             # bool


class PairBlockDetail(BaseModel):
    """
    Represents the reserve and price details for a token pair at a specific block.
    Uses Decimal for deterministic serialization.
    """
    token0ReservesNormalized: Decimal
    token1ReservesNormalized: Decimal
    token0Reserves: int
    token1Reserves: int
    token0ReservesUSD: Decimal
    token1ReservesUSD: Decimal
    token0Price: Decimal
    token1Price: Decimal
    token0PriceInToken1: Decimal
    token1PriceInToken0: Decimal
    timestamp: Optional[int] = None

    @field_serializer(
        'token0ReservesNormalized', 'token1ReservesNormalized',
        'token0ReservesUSD', 'token1ReservesUSD',
        'token0Price', 'token1Price',
        'token0PriceInToken1', 'token1PriceInToken0',
    )
    def serialize_decimal(self, v: Decimal) -> str:
        return str(v)

