from typing import List

from pydantic import BaseModel
from pydantic import Field


class AaveContractAbis(BaseModel):
    pool_contract: str = Field(
        ..., example='snapshotter/modules/computes/static/abis/AaveV3Pool.json',
    )
    pool_data_provider_contract: str = Field(
        ..., example='snapshotter/modules/computes/static/abis/AaveProtocolDataProvider.json',
    )
    erc20: str = Field(
        ..., example='snapshotter/modules/computes/static/abis/IERC20.json',
    )
    a_token: str = Field(
        ..., example='snapshotter/modules/computes/static/abis/IERC20.json',
    )


class ContractAddresses(BaseModel):
    MAKER: str = Field(..., example='0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2')
    USDT: str = Field(..., example='0xc2132d05d31c914a87c6611c10748aeb04b58e8f')
    DAI: str = Field(..., example='0x8f3cf7ad23cd3cadbd9735aff958023239c6a063')
    USDC: str = Field(..., example='0x2791bca1f2de4661ed88a30c99a7a9449aa84174')
    WETH: str = Field(..., example='0x7ceb23fd6bc0add59e62ac25578270cff1b9f619')
    FRAX: str = Field(..., example='0x853d955aCEf822Db058eb8505911ED77F175b99e')
    aave_v3_pool: str = Field(..., example='0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2')
    pool_data_provider: str = Field(..., example='0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3')


class Settings(BaseModel):
    aave_contract_abis: AaveContractAbis
    contract_addresses: ContractAddresses
    aave_v3_whitelist: List[str]
