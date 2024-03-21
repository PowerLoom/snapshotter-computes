from typing import List

from pydantic import BaseModel
from pydantic import Field


class UniswapContractAbis(BaseModel):
    factory: str = Field(
        ..., example='pooler/modules/uniswapv2static/abis/IUniswapV2Factory.json',
    )
    pair_contract: str = Field(
        ..., example='pooler/modules/computes/static/abis/UniswapV2Pair.json',
    )
    erc20: str = Field(..., example='pooler/modules/computes/static/abis/IERC20.json')
    trade_events: str = Field(
        ..., example='pooler/modules/computes/static/abis/UniswapTradeEvents.json',
    )


class ContractAddresses(BaseModel):
    iuniswap_v2_factory: str = Field(
        ..., example='0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32',
    )
    MAKER: str = Field(
        ..., example='0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2',
    )
    USDbC: str = Field(..., example='0xc2132d05d31c914a87c6611c10748aeb04b58e8f')
    DAI: str = Field(..., example='0x8f3cf7ad23cd3cadbd9735aff958023239c6a063')
    USDC: str = Field(..., example='0x2791bca1f2de4661ed88a30c99a7a9449aa84174')
    WETH: str = Field(..., example='0x7ceb23fd6bc0add59e62ac25578270cff1b9f619')
    DAI_WETH_PAIR: str = Field(
        ..., example='0xa478c2975ab1ea89e8196811f51a7b7ade33eb11',
    )
    USDC_WETH_PAIR: str = Field(
        ..., example='0xb4e16d0168e52d35cacd2c6185b44281ec28c9',
    )
    USDbC_WETH_PAIR: str = Field(
        ..., example='0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852',
    )


class Settings(BaseModel):
    uniswap_contract_abis: UniswapContractAbis
    contract_addresses: ContractAddresses
    uniswap_v2_whitelist: List[str]
