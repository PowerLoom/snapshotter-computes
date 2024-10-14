from pydantic import BaseModel
from pydantic import Field


class AaveContractAbis(BaseModel):
    pool_contract: str = Field(...)
    pool_data_provider_contract: str = Field(...)
    erc20: str = Field(...)
    a_token: str = Field(...)
    stable_token: str = Field(...)
    variable_token: str = Field(...)
    aave_oracle: str = Field(...)
    ui_pool_data_provider: str = Field(...)


class ContractAddresses(BaseModel):
    WETH: str = Field(...)
    MAKER: str = Field(...)
    aave_v3_pool: str = Field(...)
    pool_data_provider: str = Field(...)
    aave_oracle: str = Field(...)
    ui_pool_data_provider: str = Field(...)
    pool_address_provider: str = Field(...)


class Settings(BaseModel):
    aave_contract_abis: AaveContractAbis
    contract_addresses: ContractAddresses
