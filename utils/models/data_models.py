from typing import Dict
from typing import List

from pydantic import BaseModel


class MintData(BaseModel):
    minterAddress: str
    tokenId: int
    transactionHash: str


class TransferData(BaseModel):
    fromAddress: str
    toAddress: str
    tokenId: int
    transactionHash: str


class BlockNftData(BaseModel):
    mints: List[MintData]
    transfers: List[TransferData]
    timestamp: int


class EpochNftData(BaseModel):
    dataByBlock: Dict[int, BlockNftData]
    totalMinted: int
    totalUniqueMinters: int
    name: str
    symbol: str
    timestamp: int
