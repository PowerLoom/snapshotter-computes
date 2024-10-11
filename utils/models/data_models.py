from typing import Dict, TypedDict
from typing import List

from pydantic import BaseModel


class trade_data(BaseModel):
    """
    Represents trading data for a pair of tokens.

    Attributes:
        totalTradesUSD (float): Total value of trades in USD.
        totalFeeUSD (float): Total fees collected in USD.
        token0TradeVolume (float): Trading volume for token0.
        token1TradeVolume (float): Trading volume for token1.
        token0TradeVolumeUSD (float): Trading volume for token0 in USD.
        token1TradeVolumeUSD (float): Trading volume for token1 in USD.
    """
    totalTradesUSD: float
    totalFeeUSD: float
    token0TradeVolume: float
    token1TradeVolume: float
    token0TradeVolumeUSD: float
    token1TradeVolumeUSD: float

    def __add__(self, other: "trade_data") -> "trade_data":
        """
        Add trading data from another trade_data object.
        Args:
            other (trade_data): Another trade_data object to add.
        Returns:
            trade_data: The updated trade_data object.
        """
        self.totalTradesUSD += other.totalTradesUSD
        self.totalFeeUSD += other.totalFeeUSD
        self.token0TradeVolume += other.token0TradeVolume
        self.token1TradeVolume += other.token1TradeVolume
        self.token0TradeVolumeUSD += other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD += other.token1TradeVolumeUSD
        return self

    def __sub__(self, other: "trade_data") -> "trade_data":
        """
        Subtract trading data from another trade_data object.
        Args:
            other (trade_data): Another trade_data object to subtract.
        Returns:
            trade_data: The updated trade_data object.
        """
        self.totalTradesUSD -= other.totalTradesUSD
        self.totalFeeUSD -= other.totalFeeUSD
        self.token0TradeVolume -= other.token0TradeVolume
        self.token1TradeVolume -= other.token1TradeVolume
        self.token0TradeVolumeUSD -= other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD -= other.token1TradeVolumeUSD
        return self

    def __abs__(self) -> "trade_data":
        """
        Calculate the absolute values of all trading data.
        Returns:
            trade_data: A new trade_data object with absolute values.
        """
        self.totalTradesUSD = abs(self.totalTradesUSD)
        self.totalFeeUSD = abs(self.totalFeeUSD)
        self.token0TradeVolume = abs(self.token0TradeVolume)
        self.token1TradeVolume = abs(self.token1TradeVolume)
        self.token0TradeVolumeUSD = abs(self.token0TradeVolumeUSD)
        self.token1TradeVolumeUSD = abs(self.token1TradeVolumeUSD)
        return self


class event_trade_data(BaseModel):
    """
    Represents trade data for a specific event.
    Attributes:
        logs (List[Dict]): List of log dictionaries associated with the event.
        trades (trade_data): Trading data for the event.
    """
    logs: List[Dict]
    trades: trade_data


class epoch_event_trade_data(BaseModel):
    """
    Represents trade data for different types of events within an epoch.
    Attributes:
        Swap (event_trade_data): Trade data for Swap events.
        Mint (event_trade_data): Trade data for Mint events.
        Burn (event_trade_data): Trade data for Burn events.
        Trades (trade_data): Aggregated trade data for all events.
    """
    Swap: event_trade_data
    Mint: event_trade_data
    Burn: event_trade_data
    Trades: trade_data




