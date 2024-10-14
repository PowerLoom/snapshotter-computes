from pydantic import BaseModel
from pydantic import Field


class AaveContractAbis(BaseModel):
    pool_contract: str
    pool_data_provider_contract: str
    erc20: str
    a_token: str
    stable_token: str
    variable_token: str
    aave_oracle: str
    ui_pool_data_provider: str


# used for ETH price calculation in preloaders/eth_price/preloader.py
class UniswapContractAbis(BaseModel):
    pair_contract: str


class ContractAddresses(BaseModel):
    WETH: str
    MAKER: str
    aave_v3_pool: str
    pool_data_provider: str
    aave_oracle: str
    ui_pool_data_provider: str
    pool_address_provider: str


class Settings(BaseModel):
    aave_contract_abis: AaveContractAbis
    contract_addresses: ContractAddresses
    uniswap_contract_abis: UniswapContractAbis
