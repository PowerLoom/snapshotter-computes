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


class ChainlinkOracleAnswersSnapshot(SnapshotBase):
    roundIds: Dict[str, int]
    oracleAnswers: Dict[str, float]
    roundStartTimes: Dict[str, int]
    roundUpdateTimes: Dict[str, int]
    answeredInRounds: Dict[str, int]
    blockTimestamps: Dict[str, int]
