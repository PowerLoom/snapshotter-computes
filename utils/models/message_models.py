from typing import Dict
from typing import List

from pydantic import BaseModel

from computes.utils.models.data_models import AaveDebtData
from computes.utils.models.data_models import AaveSupplyData
from computes.utils.models.data_models import AssetDetailsData
from computes.utils.models.data_models import liquidationData
from computes.utils.models.data_models import RateDetailsData
from computes.utils.models.data_models import volumeData


class EpochBaseSnapshot(BaseModel):
    """
    Attributes:
        begin: Start of the epoch
        end: End of the epoch
    """
    begin: int
    end: int


class SnapshotBase(BaseModel):
    """
    Attributes:
        contract: Contract address
        chainHeightRange: Range of chain heights
        timestamp: Timestamp of the snapshot
    """
    contract: str
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class AavePoolTotalAssetSnapshot(SnapshotBase):
    """
    Attributes:
        totalAToken: Total aToken supply for each block
        liquidityRate: Liquidity rate for each block
        liquidityIndex: Liquidity index for each block
        totalVariableDebt: Total variable debt for each block
        variableBorrowRate: Variable borrow rate for each block
        variableBorrowIndex: Variable borrow index for each block
        lastUpdateTimestamp: Last update timestamp for each block
        isolationModeTotalDebt: Isolation mode total debt for each block
        assetDetails: Asset details for each block
        rateDetails: Rate details for each block
        availableLiquidity: Available liquidity for each block
    """
    totalAToken: Dict[
        str,
        AaveSupplyData,
    ]  # block number to corresponding total supply
    liquidityRate: Dict[str, int]
    liquidityIndex: Dict[str, int]
    totalVariableDebt: Dict[str, AaveDebtData]
    variableBorrowRate: Dict[str, int]
    variableBorrowIndex: Dict[str, int]
    lastUpdateTimestamp: Dict[str, int]
    isolationModeTotalDebt: Dict[str, int]
    assetDetails: Dict[str, AssetDetailsData]
    rateDetails: Dict[str, RateDetailsData]
    availableLiquidity: Dict[str, AaveSupplyData]


class AaveTopSupplyData(BaseModel):
    """
    Attributes:
        token_supply: Token supply amount
        usd_supply: USD value of supply
    """
    token_supply: float
    usd_supply: float


class AaveTopDebtData(BaseModel):
    """
    Attributes:
        token_debt: Token debt amount
        usd_debt: USD value of debt
    """
    token_debt: float
    usd_debt: float


class AaveTopAssetSnapshot(BaseModel):
    """
    Attributes:
        name: Asset name
        symbol: Asset symbol
        decimals: Number of decimals
        address: Asset address
        totalAToken: Total aToken supply
        liquidityApy: Liquidity APY
        totalVariableDebt: Total variable debt
        variableApy: Variable APY
        isIsolated: Whether the asset is isolated
    """
    name: str
    symbol: str
    decimals: int
    address: str
    totalAToken: AaveTopSupplyData
    liquidityApy: float
    totalVariableDebt: AaveTopDebtData
    variableApy: float
    isIsolated: bool


class AaveSupplyVolumeSnapshot(SnapshotBase):
    """
    Attributes:
        borrow: Borrow volume data
        repay: Repay volume data
        supply: Supply volume data
        withdraw: Withdraw volume data
        liquidation: Liquidation volume data
        events: List of events
        liquidationList: List of liquidation data
    """
    borrow: volumeData
    repay: volumeData
    supply: volumeData
    withdraw: volumeData
    liquidation: volumeData
    events: List[Dict]
    liquidationList: List[liquidationData]


class AaveTopAssetVolumeSnapshot(BaseModel):
    """
    Attributes:
        name: Asset name
        symbol: Asset symbol
        address: Asset address
        totalBorrow: Total borrow volume data
        totalRepay: Total repay volume data
        totalSupply: Total supply volume data
        totalWithdraw: Total withdraw volume data
        totalLiquidatedCollateral: Total liquidated collateral volume data
        borrowChange24h: Borrow change in the last 24 hours
        repayChange24h: Repay change in the last 24 hours
        supplyChange24h: Supply change in the last 24 hours
        withdrawChange24h: Withdraw change in the last 24 hours
        liquidationChange24h: Liquidation change in the last 24 hours
    """
    name: str
    symbol: str
    address: str
    totalBorrow: volumeData
    totalRepay: volumeData
    totalSupply: volumeData
    totalWithdraw: volumeData
    totalLiquidatedCollateral: volumeData
    borrowChange24h: float
    repayChange24h: float
    supplyChange24h: float
    withdrawChange24h: float
    liquidationChange24h: float
