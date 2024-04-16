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


class BlockMintData(BaseModel):
    mints: List[MintData]
    transfers: List[TransferData]
    timestamp: int


class EpochMintData(BaseModel):
    mintsByBlock: Dict[int, BlockMintData]
    totalMinted: int
    totalUniqueMinters: int
    timestamp: int
