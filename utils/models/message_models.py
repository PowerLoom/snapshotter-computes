from typing import Dict
from typing import List

from pydantic import BaseModel
from snapshotter.utils.models.message_models import AggregateBase


class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: str
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class AaveSupplyData(BaseModel):
    token_supply: int
    usd_supply: float


class AaveDebtData(BaseModel):
    token_debt: int
    usd_debt: float


class AavePoolTotalAssetSnapshot(SnapshotBase):
    totalAToken: Dict[
        str,
        AaveSupplyData,
    ]  # block number to corresponding total supply
    liquidityRate: Dict[str, int]
    liquidityIndex: Dict[str, int]
    totalVariableDebt: Dict[str, AaveDebtData]
    totalStableDebt: Dict[str, AaveDebtData]
    variableBorrowRate: Dict[str, int]
    stableBorrowRate: Dict[str, int]
    variableBorrowIndex: Dict[str, int]
    lastUpdateTimestamp: Dict[str, int]


class AaveTopSupplyData(BaseModel):
    token_supply: float
    usd_supply: float


class AaveTopDebtData(BaseModel):
    token_debt: float
    usd_debt: float


class AaveTopAssetSnapshot(BaseModel):
    name: str
    symbol: str
    decimals: int
    address: str
    totalAToken: AaveTopSupplyData
    liquidityApy: float
    totalVariableDebt: AaveTopDebtData
    variableApy: float


class AaveTopAssetsSnapshot(AggregateBase):
    assets: List[AaveTopAssetSnapshot] = []
    complete: bool = True


class AaveMarketStatsSnapshot(AggregateBase):
    totalMarketSize: float
    totalAvailable: float
    totalBorrows: float
    marketChange24h: float
    availableChange24h: float
    borrowChange24h: float
    complete: bool = True
