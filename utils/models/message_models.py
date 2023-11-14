from typing import Any
from typing import Dict
from typing import List
from typing import Union

from pydantic import BaseModel


class BalanceSnapshot(BaseModel):
    result: Union[str, Dict[int, str]]
    address: str
    txs: List[Dict[Any, Any]]


class BungeeBridgeSnapshot(BaseModel):
    receiver: str
    amount: int
    srcChainTxHash: str


class QuickswapSwapSnapshot(BaseModel):
    to: str
    value: float


class QuickswapLPDepositSnapshot(BaseModel):
    address: str
    sender: str
    shares: int
    amount0: int
    amount1: int


class QuickswapLPIncreaseLiquiditySnapshot(BaseModel):
    tokenId: int
    liquidity: int
    actualLiquidity: int
    amount0: int
    amount1: int
    pool: str


class SafeCreateSnapshot(BaseModel):
    proxy: str
    singleton: str
    initiator: str
    owners: List[str]
    threshold: int
    initializer: str


class SafeAddOwnerSnapshot(BaseModel):
    safe_address: str
    owner: str


class OwltoBridgeSnapshot(BaseModel):
    receiver: str
    amount: int


class ERC20BalanceSnapshot(BalanceSnapshot):
    token: str


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


class BlockDetailsSnapshot(BaseModel):
    result: BlockDetails
