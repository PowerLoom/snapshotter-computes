from typing import List

from pydantic import BaseModel
from pydantic import Field


class UniswapContractAbis(BaseModel):
    """
    Represents the file paths for various Uniswap contract ABIs.

    Attributes:
        factory (str): Path to the Uniswap V2 Factory ABI.
        router (str): Path to the Uniswap V2 Router ABI.
        pair_contract (str): Path to the Uniswap V2 Pair ABI.
        erc20 (str): Path to the ERC20 token ABI.
        trade_events (str): Path to the Uniswap trade events ABI.
    """
    factory: str = Field(
        ..., example='pooler/modules/uniswapv2static/abis/IUniswapV2Factory.json',
    )
    router: str = Field(..., example='pooler/modules/computes/static/abis/UniswapV2Router.json')
    pair_contract: str = Field(
        ..., example='pooler/modules/computes/static/abis/UniswapV2Pair.json',
    )
    erc20: str = Field(..., example='pooler/modules/computes/static/abis/IERC20.json')
    trade_events: str = Field(
        ..., example='pooler/modules/computes/static/abis/UniswapTradeEvents.json',
    )


class ContractAddresses(BaseModel):
    """
    Represents the contract addresses for various tokens and Uniswap pairs.

    Attributes:
        iuniswap_v2_factory (str): Address of the Uniswap V2 Factory contract.
        iuniswap_v2_router (str): Address of the Uniswap V2 Router contract.
        MAKER (str): Address of the Maker token contract.
        USDT (str): Address of the USDT token contract.
        DAI (str): Address of the DAI token contract.
        USDC (str): Address of the USDC token contract.
        WETH (str): Address of the Wrapped Ether (WETH) token contract.
        WETH_USDT (str): Address of the WETH-USDT pair contract.
        FRAX (str): Address of the FRAX token contract.
        SYN (str): Address of the SYN token contract.
        FEI (str): Address of the FEI token contract.
        agEUR (str): Address of the agEUR token contract.
        DAI_WETH_PAIR (str): Address of the DAI-WETH pair contract.
        USDC_WETH_PAIR (str): Address of the USDC-WETH pair contract.
        USDT_WETH_PAIR (str): Address of the USDT-WETH pair contract.
    """
    iuniswap_v2_factory: str = Field(
        ..., example='0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32',
    )
    iuniswap_v2_router: str = Field(
        ..., example='0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff',
    )
    MAKER: str = Field(
        ..., example='0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2',
    )
    USDT: str = Field(..., example='0xc2132d05d31c914a87c6611c10748aeb04b58e8f')
    DAI: str = Field(..., example='0x8f3cf7ad23cd3cadbd9735aff958023239c6a063')
    USDC: str = Field(..., example='0x2791bca1f2de4661ed88a30c99a7a9449aa84174')
    WETH: str = Field(..., example='0x7ceb23fd6bc0add59e62ac25578270cff1b9f619')
    WETH_USDT: str = Field(
        ..., example='0xf6422b997c7f54d1c6a6e103bcb1499eea0a7046',
    )
    FRAX: str = Field(..., example='0x853d955aCEf822Db058eb8505911ED77F175b99e')
    SYN: str = Field(..., example='0x0f2D719407FdBeFF09D87557AbB7232601FD9F29')
    FEI: str = Field(..., example='0x956F47F50A910163D8BF957Cf5846D573E7f87CA')
    agEUR: str = Field(
        ..., example='0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8',
    )
    DAI_WETH_PAIR: str = Field(
        ..., example='0xa478c2975ab1ea89e8196811f51a7b7ade33eb11',
    )
    USDC_WETH_PAIR: str = Field(
        ..., example='0xb4e16d0168e52d35cacd2c6185b44281ec28c9',
    )
    USDT_WETH_PAIR: str = Field(
        ..., example='0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852',
    )


class Settings(BaseModel):
    """
    Represents the overall settings for the Uniswap interaction.

    Attributes:
        uniswap_contract_abis (UniswapContractAbis): ABI file paths for Uniswap contracts.
        contract_addresses (ContractAddresses): Addresses of various contracts and tokens.
        uniswap_v2_whitelist (List[str]): List of whitelisted addresses for Uniswap V2.
    """
    uniswap_contract_abis: UniswapContractAbis
    contract_addresses: ContractAddresses
    uniswap_v2_whitelist: List[str]
