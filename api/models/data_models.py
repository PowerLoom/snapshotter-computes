
from typing import Dict
from typing import List
from typing import Tuple
from snapshotter.utils.models.data_models import EpochBaseSnapshot
from pydantic import BaseModel


class UniswapTokenMetadata(BaseModel):
    """
    Metadata for a Uniswap token.
    """
    address: str  # Contract address of the token
    name: str  # Name of the token
    symbol: str  # Symbol of the token
    decimals: int  # Number of decimals for the token


class UniswapPoolMetadata(BaseModel):
    """
    Metadata for a Uniswap pair.
    """
    address: str  # Contract address of the pair
    token0: UniswapTokenMetadata  # Metadata for token0
    token1: UniswapTokenMetadata  # Metadata for token1
    fee: int  # Fee for the pair
    factory: str  # Factory address for the pair


class UniswapTokenPoolsSnapshot(BaseModel):
    """
    Snapshot of token pools for a Uniswap pair.
    """
    pools: Dict[str, UniswapPoolMetadata]  # Dictionary mapping token addresses to pool metadata


class UniswapEthPriceSnapshot(BaseModel):
    """
    Snapshot of ETH price for a Uniswap pair.
    """
    epoch: EpochBaseSnapshot  # Range of blocks for this snapshot
    ethPrice: Dict[int, float]  # Block number to corresponding ETH price
    previousSnapshots: List[Tuple[int, str]] = []  # Will be filled by snapshot worker

