from typing import Dict
from typing import List

from pydantic import BaseModel
from pydantic.dataclasses import dataclass


class ReserveConfiguration(BaseModel):
    data: int


@dataclass
class data_provider_reserve_data():
    unbacked: int
    accruedToTreasuryScaled: int
    totalAToken: int
    totalStableDebt: int
    totalVariableDebt: int
    liquidityRate: int
    variableBorrowRate: int
    stableBorrowRate: int
    averageStableBorrowRate: int
    liquidityIndex: int
    variableBorrowIndex: int
    lastUpdateTimestamp: int
    timestamp: int = None


@dataclass
class pool_reserve_data():
    configuration: int
    liquidityIndex: int
    currentLiquidityRate: int
    variableBorrowIndex: int
    currentVariableBorrowRate: int
    currentStableBorrowRate: int
    lastUpdateTimestamp: int
    reserve_id: int
    aTokenAddress: str
    stableDebtTokenAddress: str
    variableDebtTokenAddress: str
    interestRateStrategyAddress: str
    accruedToTreasury: int
    unbacked: int
    isolationModeTotalDebt: int


class trade_data(BaseModel):
    totalTradesUSD: float
    totalFeeUSD: float
    token0TradeVolume: float
    token1TradeVolume: float
    token0TradeVolumeUSD: float
    token1TradeVolumeUSD: float

    def __add__(self, other: 'trade_data') -> 'trade_data':
        self.totalTradesUSD += other.totalTradesUSD
        self.totalFeeUSD += other.totalFeeUSD
        self.token0TradeVolume += other.token0TradeVolume
        self.token1TradeVolume += other.token1TradeVolume
        self.token0TradeVolumeUSD += other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD += other.token1TradeVolumeUSD
        return self

    def __sub__(self, other: 'trade_data') -> 'trade_data':
        self.totalTradesUSD -= other.totalTradesUSD
        self.totalFeeUSD -= other.totalFeeUSD
        self.token0TradeVolume -= other.token0TradeVolume
        self.token1TradeVolume -= other.token1TradeVolume
        self.token0TradeVolumeUSD -= other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD -= other.token1TradeVolumeUSD
        return self

    def __abs__(self) -> 'trade_data':
        self.totalTradesUSD = abs(self.totalTradesUSD)
        self.totalFeeUSD = abs(self.totalFeeUSD)
        self.token0TradeVolume = abs(self.token0TradeVolume)
        self.token1TradeVolume = abs(self.token1TradeVolume)
        self.token0TradeVolumeUSD = abs(self.token0TradeVolumeUSD)
        self.token1TradeVolumeUSD = abs(self.token1TradeVolumeUSD)
        return self


class event_trade_data(BaseModel):
    logs: List[Dict]
    trades: trade_data


class epoch_event_trade_data(BaseModel):
    Swap: event_trade_data
    Mint: event_trade_data
    Burn: event_trade_data
    Trades: trade_data
