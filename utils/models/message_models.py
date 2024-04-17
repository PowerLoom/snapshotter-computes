from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel

from .data_models import BlockNftData


class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: Optional[str]
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class NftMintSnapshot(SnapshotBase):
    dataByBlock: Dict[int, BlockNftData]
    totalMinted: int
    totalUniqueMinters: int
    name: str
    symbol: str
    timestamp: int
