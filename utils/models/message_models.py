from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel

from .data_models import BlockMintData

class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: Optional[str]
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class NftMintSnapshot(SnapshotBase):
    mintsByBlock: Dict[int, BlockMintData]
    totalMinted: int
    totalUniqueMinters: int
    timestamp: int