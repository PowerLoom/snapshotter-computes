from typing import Optional

from pydantic import BaseModel


class LiteReservesCacheConfig(BaseModel):
    enabled: bool = True
    memory_max_entries_per_pool: int = 20
    file_enabled: bool = False
    file_path: str = './.reserves_cache'


class UniswapContractAbis(BaseModel):
    factory: str
    pair_contract: str
    erc20: str
    trade_events: str
    uniswap_v3_helper: str

class ContractAddresses(BaseModel):
    uniswap_v3_factory: str
    uniswap_v3_helper: str
    chainlink_eth_usd_oracle: str
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
    bds_api_url: str
    lite_reserves_cache: Optional[LiteReservesCacheConfig] = None