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


class WalletTrackerSnapShot(BaseModel):
    wallet_address: str
    contract_address: str
    transactionHash: str
    timestamp: str
    gasUsed: str

class BlockDetails(BaseModel):
    difficulty: str
    extraData: str
    gasLimit: str
    gasUsed: str
    hash: str
    logsBloom: str
    miner: str
    mixHash: str
    nonce: str
    number: str
    parentHash: str
    receiptsRoot: str
    sha3Uncles: str
    size: str
    stateRoot: str
    timestamp: str
    totalDifficulty: str
    transactions: List[str]
    transactionsRoot: str
    uncles: List[str]
