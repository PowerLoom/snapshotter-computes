from typing import Dict
from typing import List

from pydantic import BaseModel

class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: str
    chainHeightRange: EpochBaseSnapshot
    timestamp: int
