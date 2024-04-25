from typing import Dict
from typing import List

from pydantic import BaseModel


class LatestRoundData(BaseModel):
    roundId: int
    answer: int
    startedAt: int
    updatedAt: int
    answeredInRound: int
