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


class ChainlinkOracleAggregateSnapshot(AggregateBase):
    averageAnswer: float = 0.0
    sampleSize: int = 0
    timestamp: int = 0
    complete: bool = True


class ChainlinkOracleData(BaseModel):
    address: str
    description: str
    decimals: int
    answer: float = 0.0
    answer24hAvg: float = 0.0
    avgSampleSize: int = 0


class ChainlinkAllOraclesSnapshot(AggregateBase):
    tokens: List[ChainlinkOracleData] = []
    complete: bool = True
