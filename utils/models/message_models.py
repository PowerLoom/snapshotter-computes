from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel

class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: Optional[str]
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class BlockDetailsSnapshot(SnapshotBase):
    blockTimestamps: Dict[str, int]
    blockTransactions: Dict[str, List[str]]


class EthUsdPriceSnapshot(SnapshotBase):
    blockEthUsdPrices: Dict[str, float]
