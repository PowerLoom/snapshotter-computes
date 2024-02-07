from typing import Dict
from typing import List

from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from .message_models import AaveDebtData
from .message_models import AaveSupplyData
from .message_models import AssetDetailsData


@dataclass
class DataProviderReserveData():
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
class UiDataProviderReserveData():
    liquidityIndex: int
    variableBorrowIndex: int
    liquidityRate: int
    variableBorrowRate: int
    stableBorrowRate: int
    lastUpdateTimestamp: int
    availableLiquidity: int
    totalPrincipalStableDebt: int
    averageStableRate: int
    stableDebtLastUpdateTimestamp: int
    totalScaledVariableDebt: int
    priceInMarketReferenceCurrency: int
    accruedToTreasury: int
    assetDetails: AssetDetailsData


class AssetTotalData(BaseModel):
    totalSupply: AaveSupplyData
    availableLiquidity: AaveSupplyData
    totalStableDebt: AaveDebtData
    totalVariableDebt: AaveDebtData
    liquidityRate: int
    liquidityIndex: int
    variableBorrowRate: int
    stableBorrowRate: int
    variableBorrowIndex: int
    lastUpdateTimestamp: int
    assetDetails: AssetDetailsData
    timestamp: int = None


class volume_data(BaseModel):
    totalUSD: float
    totalToken: int

    def __add__(self, other: 'volume_data') -> 'volume_data':
        self.totalUSD += other.totalUSD
        self.totalToken += other.totalToken
        return self

    def __sub__(self, other: 'volume_data') -> 'volume_data':
        self.totalUSD -= other.totalUSD
        self.totalToken -= other.totalToken
        return self

    def __abs__(self) -> 'volume_data':
        self.totalUSD = abs(self.totalUSD)
        self.totalToken = abs(self.totalToken)
        return self


class event_volume_data(BaseModel):
    logs: List[Dict]
    totals: volume_data


class epoch_event_volume_data(BaseModel):
    borrow: event_volume_data
    repay: event_volume_data
    supply: event_volume_data
    withdraw: event_volume_data
