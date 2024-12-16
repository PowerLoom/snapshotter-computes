from typing import Dict
from typing import List

from pydantic import BaseModel
from pydantic.dataclasses import dataclass


@dataclass
class DataProviderReserveData():
    """Data class representing reserve data from a data provider.

    Attributes:
        unbacked: Amount of unbacked assets.
        accruedToTreasuryScaled: Scaled amount accrued to treasury.
        totalAToken: Total amount of aTokens.
        totalVariableDebt: Total amount of variable debt.
        liquidityRate: Current liquidity rate.
        variableBorrowRate: Current variable borrow rate.
        liquidityIndex: Current liquidity index.
        variableBorrowIndex: Current variable borrow index.
        lastUpdateTimestamp: Timestamp of the last update.
        timestamp: Optional timestamp.
    """

    unbacked: int
    accruedToTreasuryScaled: int
    totalAToken: int
    totalVariableDebt: int
    liquidityRate: int
    variableBorrowRate: int
    liquidityIndex: int
    variableBorrowIndex: int
    lastUpdateTimestamp: int
    timestamp: int = None


class AssetDetailsData(BaseModel):
    """Model representing asset details data.

    Attributes:
        ltv: Loan to Value ratio.
        liqThreshold: Liquidation threshold.
        liqBonus: Liquidation bonus.
        resFactor: Reserve factor.
        borrowCap: Borrow cap.
        supplyCap: Supply cap.
    """

    ltv: float
    liqThreshold: float
    liqBonus: float
    resFactor: float
    borrowCap: int
    supplyCap: int


class RateDetailsData(BaseModel):
    """Model representing rate details data.

    Attributes:
        varRateSlope1: Variable rate slope 1.
        varRateSlope2: Variable rate slope 2.
        baseVarRate: Base variable rate.
        optimalRate: Optimal rate.
        utilRate: Utilization rate.
    """

    varRateSlope1: float
    varRateSlope2: float
    baseVarRate: float
    optimalRate: float
    utilRate: float = 0


class UiDataProviderReserveData(BaseModel):
    """Model representing UI data provider reserve data.

    Attributes:
        liquidityIndex: Current liquidity index.
        variableBorrowIndex: Current variable borrow index.
        liquidityRate: Current liquidity rate.
        variableBorrowRate: Current variable borrow rate.
        lastUpdateTimestamp: Timestamp of the last update.
        availableLiquidity: Available liquidity.
        totalScaledVariableDebt: Total scaled variable debt.
        priceInMarketReferenceCurrency: Price in market reference currency (usually USD).
        accruedToTreasury: Amount accrued to treasury.
        isolationModeTotalDebt: Total debt in isolation mode.
    """

    liquidityIndex: int
    variableBorrowIndex: int
    liquidityRate: int
    variableBorrowRate: int
    lastUpdateTimestamp: int
    availableLiquidity: int
    totalScaledVariableDebt: int
    priceInMarketReferenceCurrency: int
    accruedToTreasury: int
    isolationModeTotalDebt: int


class AaveSupplyData(BaseModel):
    """Model representing Aave supply data.

    Attributes:
        token_supply: Token supply amount.
        usd_supply: USD supply amount.
    """

    token_supply: int = 0
    usd_supply: float = 0


class AaveDebtData(BaseModel):
    """Model representing Aave debt data.

    Attributes:
        token_debt: Token debt amount.
        usd_debt: USD debt amount.
    """

    token_debt: int = 0
    usd_debt: float = 0


class AssetTotalData(BaseModel):
    """Model representing asset total data.

    Attributes:
        totalSupply: Total supply data.
        availableLiquidity: Available liquidity data.
        totalVariableDebt: Total variable debt data.
        liquidityRate: Current liquidity rate.
        liquidityIndex: Current liquidity index.
        variableBorrowRate: Current variable borrow rate.
        variableBorrowIndex: Current variable borrow index.
        lastUpdateTimestamp: Timestamp of the last update.
        isolationModeTotalDebt: Total debt in isolation mode.
        assetDetails: Asset details data.
        rateDetails: Rate details data.
        timestamp: Data retrieval timestamp.
    """

    totalSupply: AaveSupplyData
    availableLiquidity: AaveSupplyData
    totalVariableDebt: AaveDebtData
    liquidityRate: int
    liquidityIndex: int
    variableBorrowRate: int
    variableBorrowIndex: int
    lastUpdateTimestamp: int
    isolationModeTotalDebt: int
    assetDetails: AssetDetailsData
    rateDetails: RateDetailsData
    timestamp: int = None


class volumeData(BaseModel):
    """Model representing volume data.

    Attributes:
        totalUSD: Total volume in USD.
        totalToken: Total volume in tokens.
    """

    totalUSD: float = 0.0
    totalToken: int = 0

    def __add__(self, other: 'volumeData') -> 'volumeData':
        """Add two volumeData objects."""
        self.totalUSD += other.totalUSD
        self.totalToken += other.totalToken
        return self

    def __sub__(self, other: 'volumeData') -> 'volumeData':
        """Subtract two volumeData objects."""
        self.totalUSD -= other.totalUSD
        self.totalToken -= other.totalToken
        return self

    def __abs__(self) -> 'volumeData':
        """Get absolute values of volumeData."""
        self.totalUSD = abs(self.totalUSD)
        self.totalToken = abs(self.totalToken)
        return self


class eventVolumeData(BaseModel):
    """Model representing event volume data.

    Attributes:
        logs: List of event logs.
        totals: Total volume data.
    """

    logs: List[Dict]
    totals: volumeData


class liquidationData(BaseModel):
    """Model representing liquidation data.

    Attributes:
        collateralAsset: Collateral asset symbol.
        debtAsset: Debt asset symbol.
        debtToCover: Debt to cover volume data.
        liquidatedCollateral: Liquidated collateral volume data.
        blockNumber: Block number of the liquidation event.
    """

    collateralAsset: str
    debtAsset: str
    debtToCover: volumeData
    liquidatedCollateral: volumeData
    blockNumber: int


class eventLiquidationData(BaseModel):
    """Model representing event liquidation data.

    Attributes:
        logs: List of liquidation event logs.
        totalLiquidatedCollateral: Total liquidated collateral volume data.
        liquidations: List of individual liquidation data.
    """

    logs: List[Dict]
    totalLiquidatedCollateral: volumeData
    liquidations: List[liquidationData]


class epochEventVolumeData(BaseModel):
    """Model representing epoch event volume data.

    Attributes:
        borrow: Borrow event volume data.
        repay: Repay event volume data.
        supply: Supply event volume data.
        withdraw: Withdraw event volume data.
        liquidation: Liquidation event data.
    """

    borrow: eventVolumeData
    repay: eventVolumeData
    supply: eventVolumeData
    withdraw: eventVolumeData
    liquidation: eventLiquidationData
