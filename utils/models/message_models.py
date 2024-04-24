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


class LidoStakingYieldSnapshot(SnapshotBase):
    stakingApr: Dict[str, float]
    reportTimestamp: Dict[str, int]
