from typing import Dict
from typing import List

from pydantic import BaseModel


class EthSharesData(BaseModel):
    lastTimestamp: int
    preTotalShares: int
    preTotalEther: int
    postTotalShares: int
    postTotalEther: int