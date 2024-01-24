from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from .message_models import AssetDetailsData


class ReserveConfiguration(BaseModel):
    data: int


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
