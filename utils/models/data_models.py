from enum import Enum
from typing import Dict
from typing import List

from pydantic import BaseModel


class NftTransferTypes(Enum):
    TRANSFER = 'Transfer'
    TRANSFER_SINGLE = 'TransferSingle'
    TRANSFER_BATCH = 'TransferBatch'


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
    timestamp: int


class EpochERC721Data(EpochNftData):
    name: str
    symbol: str
