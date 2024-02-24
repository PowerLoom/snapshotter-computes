from typing import Dict
from typing import List

from pydantic import BaseModel
from snapshotter.utils.models.message_models import AggregateBase

from .data_models import AaveDebtData
from .data_models import AaveSupplyData
from .data_models import AssetDetailsData
from .data_models import liquidationData
from .data_models import RateDetailsData
from .data_models import volumeData


class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: str
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


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
    assetDetails: Dict[str, AssetDetailsData]
    rateDetails: Dict[str, RateDetailsData]
    availableLiquidity: Dict[str, AaveSupplyData]


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


class AaveAprAggregateSnapshot(AggregateBase):
    avgLiquidityRate: float = 0
    avgVariableRate: float = 0
    avgStableRate: float = 0
    avgUtilizationRate: float = 0
    timestamp: int = 0
    complete: bool = True


class AaveSupplyVolumeSnapshot(SnapshotBase):
    borrow: volumeData
    repay: volumeData
    supply: volumeData
    withdraw: volumeData
    liquidation: AaveSupplyData
    events: List[Dict]
    liquidationList: List[liquidationData]


class AaveVolumeAggregateSnapshot(AggregateBase):
    totalBorrow: volumeData = volumeData()
    totalRepay: volumeData = volumeData()
    totalSupply: volumeData = volumeData()
    totalWithdraw: volumeData = volumeData()
    totalLiquidatedCollateral: AaveSupplyData = AaveSupplyData()
    complete: bool = True


class AaveTopAssetVolumeSnapshot(BaseModel):
    name: str
    symbol: str
    address: str
    totalBorrow: volumeData
    totalRepay: volumeData
    totalSupply: volumeData
    totalWithdraw: volumeData
    totalLiquidatedCollateral: AaveSupplyData
    borrowChange24h: float
    repayChange24h: float
    supplyChange24h: float
    withdrawChange24h: float
    liquidationChange24h: float


class AaveTopAssetsVolumeSnapshot(AggregateBase):
    assets: List[AaveTopAssetVolumeSnapshot] = []
    complete: bool = True
