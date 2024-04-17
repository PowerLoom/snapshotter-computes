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


class NftTransfersSnapshot(SnapshotBase):
    dataByBlock: Dict[int, BlockNftData]
    totalMinted: int
    totalUniqueMinters: int
    timestamp: int


class ERC721TransfersSnapshot(NftTransfersSnapshot):
    name: str
    symbol: str
