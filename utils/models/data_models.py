from typing import Dict
from typing import List

from pydantic import BaseModel
from pydantic.dataclasses import dataclass


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


class AssetDetailsData(BaseModel):
    ltv: float
    liqThreshold: float
    liqBonus: float
    resFactor: float
    borrowCap: int
    supplyCap: int
    eLtv: float
    eliqThreshold: float
    eliqBonus: float
    optimalRate: float


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


class AaveSupplyData(BaseModel):
    token_supply: int
    usd_supply: float


class AaveDebtData(BaseModel):
    token_debt: int
    usd_debt: float


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


class volumeData(BaseModel):
    totalUSD: float = 0.0
    totalToken: int = 0

    def __add__(self, other: 'volumeData') -> 'volumeData':
        self.totalUSD += other.totalUSD
        self.totalToken += other.totalToken
        return self

    def __sub__(self, other: 'volumeData') -> 'volumeData':
        self.totalUSD -= other.totalUSD
        self.totalToken -= other.totalToken
        return self

    def __abs__(self) -> 'volumeData':
        self.totalUSD = abs(self.totalUSD)
        self.totalToken = abs(self.totalToken)
        return self


class eventVolumeData(BaseModel):
    logs: List[Dict]
    totals: volumeData


class epochEventVolumeData(BaseModel):
    borrow: eventVolumeData
    repay: eventVolumeData
    supply: eventVolumeData
    withdraw: eventVolumeData
