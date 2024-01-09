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
