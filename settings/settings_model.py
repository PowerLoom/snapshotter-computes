from typing import List
from typing import Dict
from typing import Any
from pydantic import BaseModel
from pydantic import Field


class UniswapContractAbis(BaseModel):
    factory: str
    pair_contract: str
    erc20: str
    trade_events: str


class ContractAddresses(BaseModel):
    uniswap_v3_factory: str
    DAI_WETH_PAIR: str
    USDC_WETH_PAIR: str
    USDT_WETH_PAIR: str
    WETH: str
    MAKER: str
    USDC: str
    USDT: str
    DAI: str


class Settings(BaseModel):
    uniswap_contract_abis: UniswapContractAbis
    contract_addresses: ContractAddresses
    uniswap_v2_whitelist: List[str]
    initial_pairs: List[str]
    metadata_cache: Dict[str, Any]
    static_pairs: bool
