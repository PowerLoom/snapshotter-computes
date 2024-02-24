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


class RateDetailsData(BaseModel):
    varRateSlope1: float
    varRateSlope2: float
    stableRateSlope1: float
    stableRateSlope2: float
    baseStableRate: float
    baseVarRate: float
    optimalRate: float
    utilRate: float = 0


class UiDataProviderReserveData(BaseModel):
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


class AaveSupplyData(BaseModel):
    token_supply: int = 0
    usd_supply: float = 0

    def __add__(self, other: 'volumeData') -> 'volumeData':
        self.token_supply += other.token_supply
        self.usd_supply += other.usd_supply
        return self

    def __sub__(self, other: 'volumeData') -> 'volumeData':
        self.token_supply -= other.token_supply
        self.usd_supply -= other.usd_supply
        return self


class AaveDebtData(BaseModel):
    token_debt: int = 0
    usd_debt: float = 0


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
    rateDetails: RateDetailsData
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


class liquidationData(BaseModel):
    collateralAsset: str
    debtAsset: str
    debtToCover: AaveDebtData
    liquidatedCollateral: AaveSupplyData
    blockNumber: int


class eventLiquidationData(BaseModel):
    logs: List[Dict]
    totalLiquidatedCollateral: AaveSupplyData
    liquidations: List[liquidationData]


class epochEventVolumeData(BaseModel):
    borrow: eventVolumeData
    repay: eventVolumeData
    supply: eventVolumeData
    withdraw: eventVolumeData
    liquidation: eventLiquidationData
