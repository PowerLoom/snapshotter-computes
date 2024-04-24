from typing import Dict
from typing import List

from pydantic import BaseModel


class LidoTokenRebaseData(BaseModel):
    reportTimestamp: int
    timeElapsed: int
    preTotalShares: int
    preTotalEther: int
    postTotalShares: int
    postTotalEther: int
    stakingApr: float = 0.0
