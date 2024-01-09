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
    token_supply: float
    usd_supply: float


class AaveDebtData(BaseModel):
    token_debt: float
    usd_debt: float


class AavePoolTotalAssetSnapshot(SnapshotBase):
    totalAToken: Dict[
        str,
        AaveSupplyData,
    ]  # block number to corresponding total supply
    liquidityRate: Dict[str, float]
    liquidityIndex: Dict[str, float]
    totalVariableDebt: Dict[str, AaveDebtData]
    totalStableDebt: Dict[str, AaveDebtData]
    variableBorrowRate: Dict[str, float]
    stableBorrowRate: Dict[str, float]
    variableBorrowIndex: Dict[str, float]
    lastUpdateTimestamp: Dict[str, int]
