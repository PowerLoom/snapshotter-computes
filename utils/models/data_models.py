from typing import Dict
from typing import List

from pydantic import BaseModel


class MintData(BaseModel):
    minter: str
    tokenId: int
    transactionHash: str


class BlockMintData(BaseModel):
    mints: List[MintData]
    timestamp: int


class EpochMintData(BaseModel):
    mintsByBlock: Dict[int, BlockMintData]
    totalMinted: int
    totalUniqueMinters: int
    timestamp: int
