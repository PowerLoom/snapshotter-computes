from typing import Dict
from typing import List

from pydantic import BaseModel


class detailsData(BaseModel):
    number: int
    timestamp: int
    transactions: List[str]


class BlockDetails(BaseModel):
    details: Dict[int, detailsData]


class BlockPriceData(BaseModel):
    blockPrice: float
    timestamp: int


class EthPriceDict(BaseModel):
    blockPrices: Dict[int, BlockPriceData]


